import logging
import os
import sys
import time
import typing as t
from itertools import chain

import pkg_resources

import cloup

from lib.ccm import TestCluster, with_ccm_cluster
from lib.click_helpers import with_working_directory
from lib.prometheus import PrometheusInstance, with_prometheus
from tools.dump import dump

logger = logging.getLogger('test-tool')




@cloup.group()
def cli():
    pass


@cli.command('demo')
@with_working_directory()
@with_ccm_cluster()
def run_demo_cluster(ccm_cluster: TestCluster, **kwargs):
    """
    Start a Cassandra cluster with cassandra-exporter installed (agent or standalone).
    Optionally setup a schema.
    Wait for ctrl-c to shut everything down.
    """
    ccm_cluster.start()

    for node in ccm_cluster.nodelist():
        logger.info('Node %s cassandra-exporter running on http://%s', node.name, node.exporter_address)

    sys.stderr.flush()
    sys.stdout.flush()

    input("Press any key to stop cluster...")




@cli.command()
@with_working_directory()
@with_ccm_cluster()
@with_prometheus()
def e2e(ccm_cluster: TestCluster, prometheus: PrometheusInstance, **kwargs):
    """
    Run cassandra-exporter end-to-end tests.

    - Start C* with the exporter JAR (agent or standalone).
    - Setup a schema.
    - Configure and start prometheus.
    - Wait for all scrape targets to get healthy.
    - Run some tests.
    """

    ccm_cluster.start()

    prometheus.start()

    for node in ccm_cluster.nodelist():
        logger.info('Node %s cassandra-exporter running on http://%s', node.name, node.exporter_address)

    logger.info("Prometheus running on: https://%s", prometheus.listen_address)

    input("Press any key to stop cluster...")

    while True:
        targets = prometheus.api.get_targets()

        pass

        # if len(targets['activeTargets']) > 0:
        #     for target in targets['activeTargets']:
        #         labels = frozendict(target['labels'])
        #
        #         # even if the target health is unknown, ensure the key exists so the length check below
        #         # is aware of the target
        #         history = target_histories[labels]
        #
        #         if target['health'] == 'unknown':
        #             continue
        #
        #         history[target['lastScrape']] = (target['health'], target['lastError'])
        #
        #     if all([len(v) >= 5 for v in target_histories.values()]):
        #         break

        time.sleep(1)

    # unhealthy_targets = dict((target, history) for target, history in target_histories.items()
    #                          if any([health != 'up' for (health, error) in history.values()]))
    #
    # if len(unhealthy_targets):
    #     logger.error('One or more Prometheus scrape targets was unhealthy.')
    #     logger.error(unhealthy_targets)
    #     sys.exit(-1)



@cli.command('benchmark')
@with_working_directory()
@with_ccm_cluster()
def benchmark(ccm_cluster: TestCluster, **kwargs):
    """"""
    pass



cli.add_command(dump)


def main():
    os.environ['CCM_JAVA8_DEBUG'] = 'please'
    logging.basicConfig(level=logging.DEBUG)

    # load ccm extensions (useful for ccm-java8, for example)
    for entry_point in pkg_resources.iter_entry_points(group='ccm_extension'):
        entry_point.load()()

    cli()


if __name__ == '__main__':
    main()