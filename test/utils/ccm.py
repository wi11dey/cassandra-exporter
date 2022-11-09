import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import List, Optional

from ccmlib.cluster import Cluster

from utils.jar_utils import ExporterJar
from utils.schema import CqlSchema

import cassandra.cluster
import cassandra.connection

import logging


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
            node.exporter_port = 9500 + i

            if self.exporter_jar.type == ExporterJar.ExporterType.AGENT:
                node.set_environment_variable('JVM_OPTS', f'-javaagent:{self.exporter_jar.path}=-l:{node.exporter_port}')

            # set dc/rack manually, since CCM doesn't support custom racks
            node.set_configuration_options({
                'endpoint_snitch': 'GossipingPropertyFileSnitch'
            })

            rackdc_path = Path(node.get_conf_dir()) / 'cassandra-rackdc.properties'

            node.rack_idx = (int(i / nodes) % racks) + 1
            node.dc_idx = (int(i / nodes * racks)) + 1

            with open(rackdc_path, 'w') as f:
                f.write(f'dc=dc{node.dc_idx}\nrack=rack{node.rack_idx}\n')

        return result

    def start(self, verbose=False, wait_for_binary_proto=True, wait_other_notice=True, jvm_args=None,
              profile_options=None, quiet_start=False, allow_root=False, **kwargs):

        self.logger.info('Starting Cassandra cluster...')
        result = super().start(False, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options,
                               quiet_start, allow_root, **kwargs)
        self.logger.info('Cassandra cluster started successfully')

        if self.initial_schema:
            self.logger.info('Applying initial CQL schema...')
            self.apply_schema(self.initial_schema)

        # start the standalone exporters, if requested
        if self.exporter_jar.type == ExporterJar.ExporterType.STANDALONE:
            for node in self.nodelist():
                self.logger.info('Starting standalone cassandra-exporter for node %s...', node.ip_addr)

                process = self.exporter_jar.start_standalone(
                    logfile_path=Path(node.get_path()) / 'logs' / 'cassandra-exporter.log',
                    listen_address=('localhost', node.exporter_port),
                    jmx_address=('localhost', node.jmx_port),
                    cql_address=node.network_interfaces["binary"]
                )

                self.standalone_processes.append(process)

            self.logger.info('Standalone cassandra-exporters started successfully')

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

