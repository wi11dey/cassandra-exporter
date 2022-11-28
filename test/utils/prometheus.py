import http.client
import json
import platform
import re
import subprocess
import tarfile
import time
import urllib.request
import urllib.error
from collections import namedtuple
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path
from typing import List, NamedTuple, Optional, Union
from urllib.parse import urlparse

import appdirs
import yaml
from tqdm import tqdm

import logging

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


    # @classmethod
    # def default_prometheus_archive_url(cls):
    #     return cls.archive_url_for_tag('latest')

    # @classmethod
    # def add_archive_argument(cls, name, parser):
    #     try:
    #         default_url = PrometheusArchive.default_prometheus_archive_url()
    #         default_help = '(default: %(default)s)'
    #
    #     except Exception as e:
    #         cls.logger.warning('failed to determine Prometheus archive URL', exc_info=True)
    #
    #         default_url = None
    #         default_help = f'(default: failed to determine archive URL)'
    #
    #     parser.add_argument(name, type=PrometheusArchive,
    #                         help="Prometheus binary release archive (tar, tar+gz, tar+bzip2) URL (schemes: http, https, file) " + default_help,
    #                         required=default_url is None,
    #                         default=str(default_url))

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


class PrometheusInstance:
    logger = logging.getLogger(f'{__name__}.{__qualname__}')

    prometheus_directory: Path = None
    prometheus_process: subprocess.Popen = None

    def __init__(self, archive: LocalPrometheusArchive, working_directory: Path, listen_address='localhost:9090'):
        self.prometheus_directory = archive.extract(working_directory)
        self.listen_address = listen_address

    def start(self, wait=True):
        logfile_path = self.prometheus_directory / 'prometheus.log'
        logfile = logfile_path.open('w')

        self.logger.info('Starting Prometheus...')
        self.prometheus_process = subprocess.Popen(
            args=[str(self.prometheus_directory / 'prometheus'),
                  f'--web.listen-address={self.listen_address}'],
            cwd=str(self.prometheus_directory),
            stdout=logfile,
            stderr=subprocess.STDOUT
        )

        if wait:
            self.logger.info('Waiting for Prometheus to become ready...')
            while not self.is_ready():
                time.sleep(1)

        self.logger.info('Prometheus started successfully')

    def stop(self):
        self.logger.info('Stopping Prometheus...')

        if self.prometheus_process is not None:
            self.prometheus_process.terminate()

        self.logger.info('Prometheus stopped successfully')

    @contextmanager
    def _modify_config(self):
        config_file_path = self.prometheus_directory / 'prometheus.yml'

        with config_file_path.open('r+') as stream:
            config = yaml.safe_load(stream)

            yield config

            stream.seek(0)
            yaml.safe_dump(config, stream)
            stream.truncate()

    def set_static_scrape_config(self, job_name: str, static_targets: List[str]):
        with self._modify_config() as config:
            config['scrape_configs'] = [{
                'job_name': job_name,
                'scrape_interval': '10s',
                'static_configs': [{
                    'targets': static_targets
                }]
            }]

    def is_ready(self):
        try:
            with urllib.request.urlopen(f'http://{self.listen_address}/-/ready') as response:
                return response.status == 200

        except urllib.error.HTTPError as e:
            return False

        except urllib.error.URLError as e:
            if isinstance(e.reason, ConnectionRefusedError):
                return False

            raise e

    def _api_call(self, path):
        with urllib.request.urlopen(f'http://{self.listen_address}{path}') as response:
            response_envelope = json.load(response)

            if response_envelope['status'] != 'success':
                raise Exception(response.url, response.status, response_envelope)

            return response_envelope['data']

    def get_targets(self):
        return self._api_call('/api/v1/targets')

    def query(self, q):
        return self._api_call(f'/api/v1/query?query={q}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

        if self.prometheus_process is not None:
            self.prometheus_process.__exit__(exc_type, exc_val, exc_tb)



