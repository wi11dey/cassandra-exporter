import logging
from pathlib import Path

import yaml
import urllib.request
import typing as t

from lib.ccm import TestCluster, with_ccm_cluster
from lib.click_helpers import with_working_directory

import click
import cloup

logger = logging.getLogger('dump')

@cloup.group('dump')
def dump():
    """Commands to capture, validate and diff metrics dumps"""


DUMP_MANIFEST_NAME = 'dump-manifest.yaml'


@dump.command('capture')
@with_working_directory()
@with_ccm_cluster()
@click.argument('destination')
def dump_capture(ccm_cluster: TestCluster, destination: Path, **kwargs):
    """Start a Cassandra cluster, capture metrics from each node's cassandra-exporter and save them to disk."""

    ccm_cluster.start()

    destination = Path(destination)
    destination.mkdir(exist_ok=True)

    logger.info('Capturing metrics dump to %s...', destination)

    with (destination / DUMP_MANIFEST_NAME).open('w') as f:
        manifest = {
            'version': '20221207',
            'cassandra': {
                'version': ccm_cluster.version(),
                'topology': {
                    'nodes': {n.name: {
                        'rack': n.rack,
                        'datacenter': n.data_center,
                        'ip': n.ip_addr
                    } for n in ccm_cluster.nodelist()}
                }
            },
            'exporter': {
                'version': 'unknown'
            }
        }

        yaml.safe_dump(manifest, f)

    for node in ccm_cluster.nodelist():
        for mimetype, ext in (('text/plain', 'txt'), ('application/json', 'json')):
            url = f'http://{node.exporter_address}/metrics?x-accept={mimetype}'
            download_path = destination / f'{node.name}-metrics.{ext}'

            urllib.request.urlretrieve(url, download_path)

            logger.info(f'Wrote {url} to {download_path}')


class DumpPathParamType(click.ParamType):
    name = 'dump'

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> t.Any:
        if isinstance(value, Path):
            return value

        p = Path(value)
        if p.is_file():
            p = p.parent

        manifest = p / DUMP_MANIFEST_NAME
        if not manifest.exists():
            self.fail(f'{p}: not a valid dump: {manifest} does not exist.', param, ctx)

        return p


@dump.command('validate')
@click.argument('dump', type=DumpPathParamType())
def dump_validate(dump: Path, **kwargs):
    """Validate a metrics dump using Prometheus's promtool"""
    pass


@dump.command('diff')
@click.argument('dump1', type=DumpPathParamType())
@click.argument('dump2', type=DumpPathParamType())
def dump_diff(dump1: Path, dump2: Path):
    """Compare two metrics dumps and output the difference"""
    pass



# capture dump (start C* with exporter, fetch and write metrics to file)
# this is every similar to the demo cmd
# validate dump (check for syntax errors, etc)
# compare/diff dump (list metrics added & removed)
