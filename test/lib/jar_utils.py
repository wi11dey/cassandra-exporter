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

import click

from lib.net import SocketAddress
from lib.path_utils import existing_file_arg

@dataclass
class ExporterJar:
    logger = logging.getLogger(f'{__name__}.{__qualname__}')

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

    def start_standalone(self, listen_address: SocketAddress,
                         jmx_address: SocketAddress,
                         cql_address: SocketAddress,
                         logfile_path: Path):

        self.logger.info('Standalone log file: %s', logfile_path)

        logfile = logfile_path.open('w')  # TODO: cleanup

        command = ['java',
                   '-jar', self.path,
                   '--listen', listen_address,
                   '--jmx-service-url', f'service:jmx:rmi:///jndi/rmi://{jmx_address}/jmxrmi',
                   '--cql-address', cql_address
                   ]
        command = [str(v) for v in command]

        self.logger.debug('Standalone exec(%s)', ' '.join(command))

        return subprocess.Popen(command, stdout=logfile, stderr=subprocess.STDOUT)


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
