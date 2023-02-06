import signal
import subprocess
import typing as t
from functools import wraps
from pathlib import Path
from typing import List, Optional

import click
import cloup
from ccmlib.cluster import Cluster
from ccmlib.common import check_socket_listening

from lib.click_helpers import fixup_kwargs, ppstrlist
from lib.jar_utils import ExporterJar, ExporterJarParamType
from lib.net import SocketAddress
from lib.schema import CqlSchema, CqlSchemaParamType

import cassandra.cluster
import cassandra.connection

import logging

logger = logging.getLogger('ccm')


class TestCluster(Cluster):
    logger = logging.getLogger(f'{__name__}.{__qualname__}')

    standalone_processes: List[subprocess.Popen] = []

    def __init__(self, cluster_directory: Path, cassandra_version: str,
                 nodes: int, racks: int, datacenters: int,
                 exporter_jar: ExporterJar,
                 initial_schema: Optional[CqlSchema]):

        if cluster_directory.exists():
            raise RuntimeError(f'Cluster directory {cluster_directory} must not exist.')  # CCM wants to create this

        super().__init__(
            path=cluster_directory.parent,
            name=cluster_directory.name,
            version=cassandra_version,
            create_directory=True  # if this is false, various config files wont be created...
        )

        self.exporter_jar = exporter_jar
        self.initial_schema = initial_schema

        self.populate(nodes, racks, datacenters)

    def populate(self, nodes: int, racks: int = 1, datacenters: int = 1,
                 debug=False, tokens=None, use_vnodes=False, ipprefix='127.0.0.', ipformat=None,
                 install_byteman=False):
        result = super().populate(nodes, debug, tokens, use_vnodes, ipprefix, ipformat, install_byteman)

        for i, node in enumerate(self.nodelist()):
            node.exporter_address = SocketAddress(node.ip_addr, 9500 + i)

            node.rack = f'rack-{(int(i / nodes) % racks) + 1}'
            node.data_center = f'dc-{(int(i / nodes * racks)) + 1}'

            if self.exporter_jar.type == ExporterJar.ExporterType.AGENT:
                node.set_environment_variable('JVM_OPTS', f'-javaagent:{self.exporter_jar.path}=-l{node.exporter_address}')

            # set dc/rack manually, since CCM doesn't support custom racks
            node.set_configuration_options({
                'endpoint_snitch': 'GossipingPropertyFileSnitch'
            })

            with (Path(node.get_conf_dir()) / 'cassandra-rackdc.properties').open('w') as f:
                print(f'dc={node.data_center}', file=f)
                print(f'rack={node.rack}', file=f)

        return result

    def start(self, verbose=False, wait_for_binary_proto=True, wait_other_notice=True, jvm_args=None,
              profile_options=None, quiet_start=False, allow_root=False, **kwargs):

        self.logger.info('Starting Cassandra cluster...')
        result = super().start(False, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options,
                               quiet_start, allow_root, **kwargs)
        self.logger.info('Cassandra cluster started successfully')

        # start the standalone exporters, if requested
        if self.exporter_jar.type == ExporterJar.ExporterType.STANDALONE:
            for node in self.nodelist():
                self.logger.info('Starting standalone cassandra-exporter for node %s...', node.ip_addr)

                process = self.exporter_jar.start_standalone(
                    logfile_path=Path(node.get_path()) / 'logs' / 'cassandra-exporter.log',
                    listen_address=node.exporter_address,
                    jmx_address=SocketAddress('localhost', node.jmx_port),
                    cql_address=SocketAddress(*node.network_interfaces["binary"])
                )

                self.standalone_processes.append(process)

            self.logger.info('Standalone cassandra-exporters started successfully')

        if self.initial_schema:
            self.logger.info('Applying initial CQL schema...')
            self.apply_schema(self.initial_schema)

        # wait for the exporters to accept connections
        for node in self.nodelist():
            check_socket_listening(node.exporter_address)

        return result

    def stop(self, wait=True, signal_event=signal.SIGTERM, **kwargs):
        if len(self.standalone_processes):
            # shutdown standalone exporters, if they're still running
            self.logger.info('Stopping standalone cassandra-exporters...')
            for p in self.standalone_processes:
                p.terminate()

                if wait:
                    p.wait()
            self.logger.info('Standalone cassandra-exporters stopped')

        self.logger.info('Stopping Cassandra cluster...')
        result = super().stop(wait, signal_event, **kwargs)
        self.logger.info('Cassandra cluster stopped')

        return result

    def apply_schema(self, schema: CqlSchema):
        contact_points = map(lambda n: cassandra.connection.DefaultEndPoint(*n.network_interfaces['binary']), self.nodelist())

        with cassandra.cluster.Cluster(list(contact_points)) as cql_cluster:
            with cql_cluster.connect() as cql_session:
                for stmt in schema.statements:
                    self.logger.debug('Executing CQL statement "{}".'.format(stmt.split('\n')[0]))
                    cql_session.execute(stmt)

        # # the collector defers registrations by a second or two.
        # # See com.zegelin.cassandra.exporter.Harvester.defer()
        # self.logger.info('Pausing to wait for deferred MBean registrations to complete.')
        # time.sleep(5)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def with_ccm_cluster():
    def decorator(func: t.Callable) -> t.Callable:
        jar_types = [type.name.lower() for type in ExporterJar.ExporterType]

        @cloup.option_group(
            "Cassandra",
            cloup.option('--cluster-name', 'cassandra_cluster_name', default='test-cluster', show_default=True,
                         help='name of the Cassandra cluster'),
            cloup.option('--cassandra-version', default='4.1.0', show_default=True,
                         help='Cassandra version to run'),
            cloup.option('--topology', 'cassandra_topology',
                         type=(int, int, int), default=(2, 3, 1), show_default=True,
                         metavar='DCS RACKS NODES', help='number of data centers, racks per data center, and nodes per rack.'),
            cloup.option('-j', '--exporter-jar', default='agent', show_default=True, type=ExporterJarParamType(),
                         help=f'path of the cassandra-exporter jar to use, either {ppstrlist(jar_types)} builds, '
                              f'or one of {ppstrlist(jar_types, quote=True)} for the currently built jar of that type in the project directory '
                              f'(assumes that the sources for this test tool are in the standard location within the project, and that the jar(s) have been built).'),
            cloup.option('-s', '--schema', 'cql_schema', default=CqlSchema.default_schema_path(), show_default=True, type=CqlSchemaParamType(),
                           help='path of the CQL schema YAML file to apply on cluster start. The YAML file must contain a list of CQL statement strings, which are applied in order.')
        )
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context,
                    cassandra_version: str, cassandra_cluster_name: str, cassandra_topology: t.Tuple[int, int, int],
                    exporter_jar: ExporterJar,
                    cql_schema: t.Optional[CqlSchema],
                    working_directory: Path, **kwargs):

            datacenters, racks, nodes, = cassandra_topology

            logger.info('Creating Cassandra %s cluster, with:', cassandra_version)
            logger.info('   CCM working directory %s:', working_directory)
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
