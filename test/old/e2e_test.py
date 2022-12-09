# this end-to-end test does the following:
# 1. download Prometheus (for the current platform)
# 2. setup a multi-node Cassandra cluster with the exporter installed
# 3. configure Prometheus to scrape from the Cassandra nodes
# 4. verifies that Prometheus successfully scrapes the metrics
# 5. cleans up everything

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
                        default=2)
    parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)",
                        default=3)
    parser.add_argument('-n', '--nodes', type=int, help="number of nodes per data center rack (default: %(default)s)",
                        default=3)

    ExporterJar.add_jar_argument('--exporter-jar', parser)
    CqlSchema.add_schema_argument('--'
                                  'schema', parser)
    RemotePrometheusArchive.add_archive_argument('--prometheus-archive', parser)

    args = parser.parse_args()

    if args.working_directory is None:
        args.working_directory = Path(tempfile.mkdtemp())

    def delete_working_dir():
        shutil.rmtree(args.working_directory)

    with contextlib.ExitStack() as defer:
        if not args.keep_working_directory:
            defer.callback(delete_working_dir)  # LIFO order -- this gets called last

        logger.info('Setting up Prometheus.')
        prometheus = defer.push(PrometheusInstance(
            archive=args.prometheus_archive,
            working_directory=args.working_directory
        ))

        logger.info('Setting up Cassandra cluster.')
        ccm_cluster = defer.push(TestCluster(
            cluster_directory=args.working_directory / 'test-cluster',
            cassandra_version=args.cassandra_version,
            exporter_jar=args.exporter_jar,
            nodes=args.nodes, racks=args.racks, datacenters=args.datacenters,
            delete_cluster_on_stop=not args.keep_working_directory,
        ))

        # httpd = http.server.HTTPServer(("", 9500), DummyPrometheusHTTPHandler)
        # threading.Thread(target=httpd.serve_forever, daemon=True).start()
        #
        # httpd = http.server.HTTPServer(("", 9501), DummyPrometheusHTTPHandler)
        # threading.Thread(target=httpd.serve_forever, daemon=True).start()

        prometheus.set_static_scrape_config('cassandra',
                                            list(map(lambda n: f'localhost:{n.exporter_port}', ccm_cluster.nodelist())))
        # prometheus.set_scrape_config('cassandra', ['localhost:9500', 'localhost:9501'])
        prometheus.start()

        logger.info('Starting Cassandra cluster.')
        ccm_cluster.start()

        logger.info('Applying CQL schema.')
        ccm_cluster.apply_schema(args.schema)

        target_histories = defaultdict(dict)

        while True:
            targets = prometheus.get_targets()

            if len(targets['activeTargets']) > 0:
                for target in targets['activeTargets']:
                    labels = frozendict(target['labels'])

                    # even if the target health is unknown, ensure the key exists so the length check below
                    # is aware of the target
                    history = target_histories[labels]

                    if target['health'] == 'unknown':
                        continue

                    history[target['lastScrape']] = (target['health'], target['lastError'])

                if all([len(v) >= 5 for v in target_histories.values()]):
                    break

            time.sleep(1)

        unhealthy_targets = dict((target, history) for target, history in target_histories.items()
                                 if any([health != 'up' for (health, error) in history.values()]))

        if len(unhealthy_targets):
            logger.error('One or more Prometheus scrape targets was unhealthy.')
            logger.error(unhealthy_targets)
            sys.exit(-1)
