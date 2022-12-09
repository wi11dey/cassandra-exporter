import http.client
import json
import platform
import re
import signal
import ssl
import subprocess
import tarfile
import time
import typing as t
import urllib.request
import urllib.error
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from io import TextIOWrapper
from pathlib import Path
from typing import List, NamedTuple, Optional, Union, TextIO
from urllib.parse import urlparse

import appdirs
import click
import cloup
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import yaml
from tqdm import tqdm

import logging

from lib.ccm import TestCluster
from lib.click_helpers import fixup_kwargs

from lib.net import SocketAddress


class _TqdmIOStream(object):
    def __init__(self, stream, t):
        self._stream = stream
        self._t = t

    def read(self, size):
        buf = self._stream.read(size)
        self._t.update(len(buf))
        return buf

    def __enter__(self, *args, **kwargs):
        self._stream.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._stream.__exit__(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._stream, attr)


class LocalPrometheusArchive(NamedTuple):
    path: Path

    def extract(self, destination_directory: Path) -> Path:
        archive_roots = set()

        with tarfile.open(self.path, mode='r') as archive:
            for member in archive:
                archive_roots.add(Path(member.name).parts[0])

                archive.extract(member, destination_directory)

        return destination_directory / next(iter(archive_roots))


class RemotePrometheusArchive(NamedTuple):
    url: str

    logger = logging.getLogger(f'{__name__}.{__qualname__}')

    @classmethod
    def for_tag(cls, tag: str):
        def architecture_str():
            machine_aliases = {
                'x86_64': 'amd64'
            }

            machine = platform.machine()
            machine = machine_aliases.get(machine, machine)

            system = platform.system().lower()

            return f'{system}-{machine}'

        asset_pattern = re.compile(r'prometheus-.+\.' + architecture_str() + '\.tar\..+')

        with urllib.request.urlopen(f'https://api.github.com/repos/prometheus/prometheus/releases/{tag}') as response:
            release_info = json.load(response)

        for asset in release_info['assets']:
            if asset_pattern.fullmatch(asset['name']) is not None:
                return RemotePrometheusArchive(asset['browser_download_url'], )


    @staticmethod
    def default_download_cache_directory() -> Path:
        return Path(appdirs.user_cache_dir('cassandra-exporter-e2e')) / 'prometheus'

    def download(self, download_cache_directory: Path = None) -> LocalPrometheusArchive:
        if download_cache_directory is None:
            download_cache_directory = RemotePrometheusArchive.default_download_cache_directory()

        url_parts = urlparse(self.url)
        url_path = Path(url_parts.path)

        destination = download_cache_directory / url_path.name
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists():
            return LocalPrometheusArchive(destination)

        self.logger.info(f'Downloading {self.url} to {destination}...')

        try:
            with tqdm(unit='bytes', unit_scale=True, miniters=1) as t:
                def report(block_idx: int, block_size: int, file_size: int):
                    if t.total is None:
                        t.reset(file_size)

                    t.update(block_size)

                urllib.request.urlretrieve(self.url, destination, report)

        except:
            destination.unlink(missing_ok=True)  # don't leave half-download files around
            raise

        return LocalPrometheusArchive(destination)


def archive_from_path_or_url(purl: str) -> Union[LocalPrometheusArchive, RemotePrometheusArchive]:
    url_parts = urlparse(purl)

    if url_parts.netloc == '':
        return LocalPrometheusArchive(Path(purl))

    return RemotePrometheusArchive(purl)


class PrometheusApi:
    def __init__(self, address: SocketAddress, ssl_context: ssl.SSLContext):
        self.address = address
        self.ssl_context = ssl_context

    def _api_call(self, path):
        with urllib.request.urlopen(f'https://{self.address}{path}', context=self.ssl_context) as response:
            response_envelope = json.load(response)

            if response_envelope['status'] != 'success':
                raise Exception(response.url, response.status, response_envelope)

            return response_envelope['data']

    def get_targets(self):
        return self._api_call('/api/v1/targets')

    def query(self, q):
        return self._api_call(f'/api/v1/query?query={q}')


class PrometheusInstance:
    logger = logging.getLogger(f'{__name__}.{__qualname__}')

    listen_address: SocketAddress
    directory: Path = None

    process: subprocess.Popen = None
    log_file: TextIO

    tls_key_path: Path
    tls_cert_path: Path
    ssl_context: ssl.SSLContext

    api: PrometheusApi

    def __init__(self, archive: LocalPrometheusArchive, working_directory: Path,
                 listen_address: SocketAddress = SocketAddress('localhost', 9090)):
        self.directory = archive.extract(working_directory)
        self.listen_address = listen_address

        self.setup_tls()

        self.api = PrometheusApi(listen_address, self.ssl_context)

    def setup_tls(self):
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )

        self.tls_key_path = (self.directory / 'tls_key.pem')
        with self.tls_key_path.open('wb') as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ))

        subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, u"AU"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Australian Capital Territory"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, u"Canberra"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Instaclustr Pty Ltd"),
                x509.NameAttribute(NameOID.COMMON_NAME, u"Temporary Prometheus Server Certificate"),
            ])

        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.utcnow()
        ).not_valid_after(
            datetime.utcnow() + timedelta(days=1)
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName(self.listen_address.host)]),
            critical=False,
        ).sign(private_key, hashes.SHA256())  # Sign certificate with private key

        self.tls_cert_path = (self.directory / 'tls_cert.pem')
        with self.tls_cert_path.open('wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        self.ssl_context = ssl.SSLContext()
        self.ssl_context.load_verify_locations(self.tls_cert_path)
        self.ssl_context.verify_mode = ssl.VerifyMode.CERT_REQUIRED

    def start(self, wait=True):
        web_config_path = (self.directory / 'web-config.yaml')
        with web_config_path.open('w') as f:
            config = {
                'tls_server_config': {
                    'cert_file': str(self.tls_cert_path),
                    'key_file': str(self.tls_key_path)
                }
            }

            yaml.safe_dump(config, f)

        self.log_file = (self.directory / 'prometheus.log').open('w')

        self.logger.info('Starting Prometheus...')
        self.process = subprocess.Popen(
            args=[str(self.directory / 'prometheus'),
                  f'--web.config.file={web_config_path}',
                  f'--web.listen-address={self.listen_address}'],
            cwd=str(self.directory),
            stdout=self.log_file,
            stderr=subprocess.STDOUT
        )

        if wait:
            self.wait_ready()

        self.logger.info('Prometheus started successfully')

    def stop(self):
        self.logger.info('Stopping Prometheus...')

        if self.process is not None:
            self.process.terminate()

        self.logger.info('Prometheus stopped successfully')

    def wait_ready(self):
        self.logger.info('Waiting for Prometheus to become ready...')
        while not self.is_ready():
            rc = self.process.poll()
            if rc is not None:
                raise Exception(f'Prometheus process {self.process.pid} exited unexpectedly with rc {rc} while waiting for ready state!')

            time.sleep(1)

    @contextmanager
    def _modify_config(self):
        config_file_path = self.directory / 'prometheus.yml'

        with config_file_path.open('r+') as stream:
            config = yaml.safe_load(stream)

            yield config

            stream.seek(0)
            stream.truncate()

            yaml.safe_dump(config, stream)

        if self.process is not None:
            self.process.send_signal(signal.SIGHUP)
            self.wait_ready()

    def set_static_scrape_config(self, job_name: str, static_targets: List[Union[str, SocketAddress]]):
        with self._modify_config() as config:
            config['scrape_configs'] = [{
                'job_name': job_name,
                'scrape_interval': '10s',
                'static_configs': [{
                    'targets': [str(t) for t in static_targets]
                }]
            }]

    def is_ready(self):
        try:
            with urllib.request.urlopen(f'https://{self.listen_address}/-/ready', context=self.ssl_context) as response:
                return response.status == 200

        except urllib.error.HTTPError as e:
            self.logger.debug('HTTP error while checking for ready state: %s', e)
            return False

        except urllib.error.URLError as e:
            self.logger.debug('urllib error while checking for ready state: %s', e)
            if isinstance(e.reason, ConnectionRefusedError):
                return False

            if isinstance(e.reason, ssl.SSLError):
                self.logger.warning('SSL/TLS errors may mean that an instance of Prometheus (or some other server) is already listening on %s. Check the port.', self.listen_address)

            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

        if self.process is not None:
            self.process.__exit__(exc_type, exc_val, exc_tb)

        if self.log_file is not None:
            self.log_file.close()


def with_prometheus():
    def decorator(func: t.Callable) -> t.Callable:
        @cloup.option_group(
            "Prometheus Archive",
            cloup.option('--prometheus-version', metavar='TAG'),
            cloup.option('--prometheus-archive', metavar='PATH/URL'),
            constraint=cloup.constraints.mutually_exclusive
        )
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context,
                    prometheus_version: str,
                    prometheus_archive: str,
                    working_directory: Path,
                    ccm_cluster: t.Optional[TestCluster] = None,
                    **kwargs):

            if prometheus_version is None and prometheus_archive is None:
                prometheus_version = 'latest'

            if prometheus_version is not None:
                archive = RemotePrometheusArchive.for_tag(prometheus_version)

            else:
                archive = archive_from_path_or_url(prometheus_archive)

            if isinstance(archive, RemotePrometheusArchive):
                archive = archive.download()

            prometheus = ctx.with_resource(PrometheusInstance(
                archive=archive,
                working_directory=working_directory
            ))

            if ccm_cluster:
                prometheus.set_static_scrape_config('cassandra',
                                                    [str(n.exporter_address) for n in ccm_cluster.nodelist()]
                                                    )

            fixup_kwargs()

            func(prometheus=prometheus, **kwargs)

        return wrapper

    return decorator
