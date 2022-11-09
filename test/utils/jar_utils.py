import argparse
from dataclasses import dataclass
import logging
import re
import subprocess
import typing as t
import zipfile
from enum import Enum
from os import PathLike
from pathlib import Path
from xml.etree import ElementTree

from utils.path_utils import existing_file_arg

logger = logging.getLogger(__name__)


@dataclass
class ExporterJar:
    class ExporterType(Enum):
        AGENT = ('Premain-Class', 'com.zegelin.cassandra.exporter.Agent')
        STANDALONE = ('Main-Class', 'com.zegelin.cassandra.exporter.Application')

        def path(self, version: str):
            lname = self.name.lower()
            return f'{lname}/target/cassandra-exporter-{lname}-{version}.jar'

    path: Path
    type: ExporterType

    @classmethod
    def from_path(cls, path: PathLike) -> 'ExporterJar':
        path = existing_file_arg(path)

        # determine the JAR type (agent or standalone) via the Main/Premain class name listed in the manifest
        try:
            with zipfile.ZipFile(path) as zf:
                manifest = zf.open('META-INF/MANIFEST.MF').readlines()

                def parse_line(line):
                    m = re.match('(.+): (.+)', line.decode("utf-8").strip())
                    return None if m is None else m.groups()

                manifest = dict(filter(None, map(parse_line, manifest)))

                type = next(iter([t for t in ExporterJar.ExporterType if t.value in manifest.items()]), None)
                if type is None:
                    raise ValueError(f'no manifest attribute found that matches known values')

                return cls(path, type)

        except Exception as e:
            raise ValueError(f'{path} is not a valid cassandra-exporter jar: {e}')

    @staticmethod
    def default_jar_path(type: ExporterType = ExporterType.AGENT) -> Path:
        project_dir = Path(__file__).parents[2]

        root_pom = ElementTree.parse(project_dir / 'pom.xml').getroot()
        project_version = root_pom.find('{http://maven.apache.org/POM/4.0.0}version').text

        return project_dir / type.path(project_version)

    def __str__(self) -> str:
        return f'{self.path} ({self.type.name})'

    def start_standalone(self, listen_address: (str, int),
                         jmx_address: (str, int),
                         cql_address: (str, int),
                         logfile_path: Path):

        logfile = logfile_path.open('w')

        def addr_str(address: (str, int)):
            return ':'.join(map(str, address))

        command = ['java',
                   '-jar', self.path,
                   '--listen', addr_str(listen_address),
                   '--jmx-service-url', f'service:jmx:rmi:///jndi/rmi://{addr_str(jmx_address)}/jmxrmi',
                   '--cql-address', addr_str(cql_address)
                   ]

        logger.debug('Standalone exec(%s)', command)

        return subprocess.Popen(command, stdout=logfile, stderr=subprocess.STDOUT)