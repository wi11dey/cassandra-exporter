import itertools
from pathlib import Path
from typing import NamedTuple, Any, Union, Iterable, List

import io

from frozendict import frozendict
from prometheus_client import Metric
from prometheus_client.parser import text_fd_to_metric_families
import prometheus_client.samples


class ValidationResult(NamedTuple):
    untyped_families: Any
    duplicate_families: Any
    duplicate_samples: Any

 #       = namedtuple('ValidationResult', ['duplicate_families', 'duplicate_samples'])
#DiffResult = namedtuple('DiffResult', ['added_families', 'removed_families', 'added_samples', 'removed_samples'])


class MetricsDump(NamedTuple):
    path: Union[str, Path]
    metric_families: List[Metric]

    @classmethod
    def from_file(cls, path: Path) -> 'MetricsDump':
        with open(path, 'rt', encoding='utf-8') as fd:
            return MetricsDump.from_lines(fd)

    @classmethod
    def from_str(cls, s: str) -> 'MetricsDump':
        with io.StringIO(s) as fd:
            return MetricsDump.from_lines(fd)

    @classmethod
    def from_lines(cls, lines: Iterable[str]) -> 'MetricsDump':
        def parse_lines():
            for family in text_fd_to_metric_families(lines):
                # freeze the labels dict so its hashable and the keys can be used as a set
                #family.samples = [sample._replace(labels=frozendict(sample.labels)) for sample in family.samples]

                yield family

        metric_families = list(parse_lines())

        path = '<memory>'
        if isinstance(lines, io.BufferedReader):
            path = lines.name

        return MetricsDump(path, metric_families)

    def validate(self) -> ValidationResult:
        def find_duplicate_families():
            def family_name_key_fn(f):
                return f.name

            families = sorted(self.metric_families, key=family_name_key_fn)  # sort by name
            family_groups = itertools.groupby(families, key=family_name_key_fn)  # group by name
            family_groups = [(k, list(group)) for k, group in family_groups]  # convert groups to lists

            return {name: group for name, group in family_groups if len(group) > 1}

        def find_duplicate_samples():
            samples = itertools.chain(family.samples for family in self.metric_families)
            #sample_groups =

            return


        return ValidationResult(
            duplicate_families=find_duplicate_families(),
            duplicate_samples=find_duplicate_samples()
        )

    def diff(self, other: 'MetricsDump'):
        pass