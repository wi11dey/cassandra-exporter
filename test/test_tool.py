import argparse
import inspect
import logging
import os
import sys
import typing as t
from contextlib import contextmanager
import shutil
import tempfile
from functools import wraps, update_wrapper, WRAPPER_UPDATES
from itertools import chain
from pathlib import Path
import pkg_resources

import click

from utils.ccm import TestCluster
from utils.jar_utils import ExporterJar
from utils.prometheus import PrometheusInstance
from utils.schema import CqlSchema

logger = logging.getLogger('test-tool')


def ppstrlist(sl: t.List[t.Any], conj: str = 'or', quote: bool = False):
    joins = [', '] * len(sl)
    joins += [f' {conj} ', '']

    joins = joins[-len(sl):]

    if quote:
        sl = [f'"{s}"' for s in sl]

    return ''.join(chain.from_iterable(zip(sl, joins)))



def fixup_kwargs(*skip: str):
    """
    inspect the caller's frame, grab any arguments and shove them back into kwargs

    this is useful when the caller is a wrapper and wants to pass on the majority its arguments to the wrapped function
    """

    caller_frame = inspect.stack()[1].frame
    args, _, kwvar, values = inspect.getargvalues(caller_frame)

    args: t.List[str] = [a for a in args if a not in skip]

    kwargs: t.Dict[str, t.Any] = values[kwvar]

    for a in args:
        v = values[a]
        if isinstance(v, click.Context):
            continue

        kwargs[a] = v

    #kwargs.update(overrides)

    pass


def with_working_directory():
    def decorator(func: t.Callable) -> t.Callable:
        @click.option('-C', '--working-directory', type=click.Path(path_type=Path),
                      help="location to install Cassandra and/or Prometheus. Must be empty or not exist. Defaults to a temporary directory.")
        @click.option('--keep-working-directory', is_flag=True,
                      help="don't delete the working directory on exit.")
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context, working_directory: Path, keep_working_directory: bool, **kwargs):
            @contextmanager
            def working_dir_ctx():
                nonlocal working_directory, keep_working_directory

                if working_directory is None:
                    working_directory = Path(tempfile.mkdtemp())

                logger.info('Working directory is: %s', working_directory)

                try:
                    yield working_directory
                finally:
                    if not keep_working_directory:
                        logger.debug('Deleting working directory')
                        shutil.rmtree(working_directory)

            working_directory = ctx.with_resource(working_dir_ctx())

            fixup_kwargs()

            func(**kwargs)

        return wrapper

    return decorator


class ExporterJarParamType(click.ParamType):
    name = "path"

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> ExporterJar:
        if isinstance(value, ExporterJar):
            return value

        try:
            if isinstance(value, str):
                for t in ExporterJar.ExporterType:
                    if t.name.lower() == value.lower():
                        return ExporterJar.from_path(ExporterJar.default_jar_path(t))


            return ExporterJar.from_path(value)

        except Exception as e:
            self.fail(str(e), param, ctx)


class CqlSchemaParamType(click.ParamType):
    name = "path"

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> CqlSchema:
        if isinstance(value, CqlSchema):
            return value

        try:
            return CqlSchema.from_path(value)

        except Exception as e:
            self.fail(str(e), param, ctx)


def with_ccm_cluster():
    def decorator(func: t.Callable) -> t.Callable:

        jar_default_path = None

        # noinspection PyBroadException
        try:
            jar_default_path = ExporterJar.default_jar_path()

        except:
            logger.warning('Failed to determine default cassandra-exporter jar path', exc_info=True)

        jar_types = [type.name.lower() for type in ExporterJar.ExporterType]

        @click.argument('cassandra_version')
        @click.option('--cluster-name', 'cassandra_cluster_name', default='test-cluster', show_default=True)
        @click.option('--topology', 'cassandra_topology',
                      type=(int, int, int), default=(2, 3, 1), show_default=True,
                      metavar='DCS RACKS NODES', help="number of data centers, racks per data center, and nodes per rack.")
        @click.option('-j', '--exporter-jar', required=True, default=jar_default_path, show_default=True, type=ExporterJarParamType(),
                      help=f"path of the cassandra-exporter jar, either {ppstrlist(jar_types)} builds, or one of {ppstrlist(jar_types, quote=True)} for the default jar of that type.")
        @click.option('-s', '--schema', 'cql_schema', default=CqlSchema.default_schema_path(), show_default=True, type=CqlSchemaParamType(),
                       help='path of the CQL schema YAML file to apply on cluster start. The YAML file must contain a list of CQL statement strings.')
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context,
                    cassandra_version: str, cassandra_cluster_name: str, cassandra_topology: t.Tuple[int, int, int],
                    exporter_jar: ExporterJar,
                    cql_schema: t.Optional[CqlSchema],
                    working_directory: Path, **kwargs):

            datacenters, racks, nodes, = cassandra_topology

            logger.info('Creating Cassandra %s cluster, with:')
            logger.info('   Topology: %s data center(s), %s rack(s) per DC, %s node(s) per rack (%s node(s) total)', datacenters, racks, nodes, (nodes * racks * datacenters))
            logger.info('   cassandra-exporter: %s', exporter_jar)

            ccm_cluster = ctx.with_resource(TestCluster(
                cluster_directory=(working_directory / cassandra_cluster_name),
                cassandra_version=cassandra_version,
                nodes=nodes*racks*datacenters, racks=racks, datacenters=datacenters,
                exporter_jar=exporter_jar,
                initial_schema=cql_schema
            ))

            fixup_kwargs()

            func(ccm_cluster=ccm_cluster, **kwargs)


        return wrapper

    return decorator


def with_prometheus():
    def decorator(func: t.Callable) -> t.Callable:
        @click.option('--prometheus-version', 'prometheus_version', default='test-cluster', show_default=True)
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context,
                    working_directory: Path,
                    ccm_cluster: t.Optional[TestCluster] = None,
                    **kwargs):


            # prometheus = ctx.with_resource(PrometheusInstance(
            #     archive=args.prometheus_archive,
            #     working_directory=working_directory
            # ))

            if ccm_cluster:
                pass
                # prometheus.set_scrape_config('cassandra',
                #                              [f'localhost:{n.exporter_port}' for n in ccm_cluster.nodelist()]
                #                              )

            fixup_kwargs()

            func(prometheus=None, **kwargs)

        return wrapper


    return decorator


@click.group()
def cli():
    pass



@cli.command('demo')
@with_working_directory()
@with_ccm_cluster()
# @with_prometheus()
# @click.option('--hello')
def run_demo_cluster(ccm_cluster: TestCluster, **kwargs):
    """
    Start C* with the exporter jar (agent or standalone).
    Optionally setup a schema.
    Wait for ctrl-c to shut everything down.
    """
    ccm_cluster.start()

    config = {'scrape_configs': [{
        'job_name': 'cassandra',
        'scrape_interval': '10s',
        'static_configs': [{
            'targets': [f'http://localhost:{node.exporter_port}' for node in ccm_cluster.nodelist()]
        }]
    }]}

    sys.stderr.flush()
    sys.stdout.flush()

    input("Press any key to stop cluster...")


@cli.command()
def dump():
    pass

# capture dump (start C* with exporter, fetch and write metrics to file)
    # this is every similar to the demo cmd
# validate dump (check for syntax errors, etc)
# compare/diff dump (list metrics added & removed)


@cli.command()
@with_working_directory()
@with_ccm_cluster()
@with_prometheus()
def e2e(ccm_cluster: TestCluster, prometheus: PrometheusInstance):
    """
    Run end-to-end tests.

    - Start C* with the exporter JAR (agent or standalone).
    - Setup a schema.
    - Configure and start prometheus.
    - Wait for all scrape targets to get healthy.
    - Run some tests.
    """

    logger.info('Starting Prometheus.')
    prometheus.start()

    logger.info('Starting Cassandra cluster.')
    ccm_cluster.start()

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


def main():
    # load ccm extensions (useful for ccm-java8, for example)
    for entry_point in pkg_resources.iter_entry_points(group='ccm_extension'):
        entry_point.load()()

    cli()


if __name__ == '__main__':
    os.environ['CCM_JAVA8_DEBUG'] = 'please'
    logging.basicConfig(level=logging.DEBUG)
    #logger.info("Hello!")
    main()