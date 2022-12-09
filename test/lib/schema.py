import argparse
from dataclasses import dataclass
from os import PathLike
import typing as t

import click
import yaml
from pathlib import Path
from collections import namedtuple

from lib.path_utils import existing_file_arg

@dataclass
class CqlSchema:
    path: Path
    statements: t.List[str]

    @classmethod
    def from_path(cls, path: PathLike) -> 'CqlSchema':
        path = existing_file_arg(path)

        with open(path, 'r') as f:
            schema = yaml.load(f, Loader=yaml.SafeLoader)

            if not isinstance(schema, list):
                raise ValueError(f'root of the schema YAML must be a list. Got a {type(schema).__name__}.')

            for i, o in enumerate(schema):
                if not isinstance(o, str):
                    raise ValueError(f'schema YAML must be a list of statement strings. Item {i} is a {type(o).__name__}.')

            return cls(path, schema)

    @staticmethod
    def default_schema_path() -> Path:
        test_dir = Path(__file__).parents[1]
        return test_dir / "schema.yaml"


class CqlSchemaParamType(click.ParamType):
    name = "path"

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> CqlSchema:
        if isinstance(value, CqlSchema):
            return value

        try:
            return CqlSchema.from_path(value)

        except Exception as e:
            self.fail(str(e), param, ctx)
