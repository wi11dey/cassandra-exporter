# spin up a CCM cluster of the specified C* version and Exporter build.
# Useful for testing and demos.

import argparse
import contextlib
import http.server
import logging
import random
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

import yaml
from frozendict import frozendict

from lib.ccm import TestCluster
from lib.jar_utils import ExporterJar
from lib.path_utils import nonexistent_or_empty_directory_arg
from lib.prometheus import PrometheusInstance, RemotePrometheusArchive
from lib.schema import CqlSchema

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")

    parser.add_argument('-C', '--working-directory', type=nonexistent_or_empty_directory_arg,
                        help="location to install Cassandra and Prometheus. Must be empty or not exist. (default is a temporary directory)")
    parser.add_argument('--keep-working-directory', help="don't delete the cluster directory on exit",
                        action='store_true')

    parser.add_argument('-d', '--datacenters', type=int, help="number of data centers (default: %(default)s)",
                        default=1)
    parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)",
                        default=3)
    parser.add_argument('-n', '--nodes', type=int, help="number of nodes per data center rack (default: %(default)s)",
                        default=3)

    ExporterJar.add_jar_argument('--exporter-jar', parser)
    CqlSchema.add_schema_argument('--schema', parser)
    RemotePrometheusArchive.add_archive_argument('--prometheus-archive', parser)

    args = parser.parse_args()

    if args.working_directory is None:
        args.working_directory = Path(tempfile.mkdtemp())


    def delete_working_dir():
        shutil.rmtree(args.working_directory)


    with contextlib.ExitStack() as defer:
        if not args.keep_working_directory:
            defer.callback(delete_working_dir)  # LIFO order -- this gets called last

        logger.info('Setting up Cassandra cluster.')
        ccm_cluster = defer.push(TestCluster(
            cluster_directory=args.working_directory / 'test-cluster',
            cassandra_version=args.cassandra_version,
            exporter_jar=args.exporter_jar,
            nodes=args.nodes, racks=args.racks, datacenters=args.datacenters,
            delete_cluster_on_stop=not args.keep_working_directory,
        ))



        print('Prometheus scrape config:')
        config = {'scrape_configs': [{
            'job_name': 'cassandra',
            'scrape_interval': '10s',
            'static_configs': [{
                'targets': [f'http://localhost:{node.exporter_port}' for node in ccm_cluster.nodelist()]
            }]
        }]}

        yaml.safe_dump(config, sys.stdout)

        ccm_cluster.start()
        logger.info("Cluster is now running.")

        input("Press any key to stop cluster...")