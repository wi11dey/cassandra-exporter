import inspect
import shutil
import tempfile
import typing as t
from enum import Enum
from functools import wraps
from itertools import chain
from pathlib import Path

import click
import cloup

from lib.path_utils import nonexistent_or_empty_directory_arg


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


def ppstrlist(sl: t.List[t.Any], conj: str = 'or', quote: bool = False):
    joins = [', '] * len(sl)
    joins += [f' {conj} ', '']

    joins = joins[-len(sl):]

    if quote:
        sl = [f'"{s}"' for s in sl]

    return ''.join(chain.from_iterable(zip(sl, joins)))



class DictChoice(click.Choice):
    """like Choice except takes a Dict[str, Any].

    The choices are the string keys of the dict.
    convert() returns the value for the chosen key."""

    dict_choices: t.Dict[str, t.Any]

    def __init__(self, choices: t.Dict[str, t.Any], case_sensitive: bool = True) -> None:
        self.dict_choices = choices
        super().__init__(list(choices.keys()), case_sensitive)

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> t.Any:
        return self.dict_choices[super().convert(value, param, ctx)]


class WorkingDirectory:
    class CleanupMode(Enum):
        KEEP_ON_ERROR = (True, False)
        KEEP_ALWAYS = (False, False)
        DELETE_ALWAYS = (True, True)

        def __init__(self, delete_normally: bool, delete_on_exception: bool):
            self.delete_normally = delete_normally
            self.delete_on_exception = delete_on_exception

        def should_delete(self, has_exception: bool) -> bool:
            return self.delete_on_exception if has_exception else self.delete_normally

    def __init__(self, cleanup_mode: CleanupMode, directory: t.Optional[Path] = None):
        self.cleanup_mode = cleanup_mode
        self.directory = directory

    def __enter__(self) -> Path:
        if self.directory is None:
            self.directory = Path(tempfile.mkdtemp())

        self.directory.mkdir(exist_ok=True)

        return self.directory

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        has_e = exc_type is not None

        if self.cleanup_mode.should_delete(has_e):
            shutil.rmtree(self.directory)

        return False


class DirectoryPathType(click.Path):
    def __init__(self, empty: bool = False):
        super().__init__(path_type=Path)
        self.empty = empty

    def convert(self, value: t.Any, param: t.Optional[click.Parameter], ctx: t.Optional[click.Context]) -> t.Any:
        path: Path = super().convert(value, param, ctx)

        if path.exists():
            if not path.is_dir():
                self.fail(f'{path}: must be a directory', param, ctx)

            if self.empty and next(path.iterdir(), None) is not None:
                self.fail(f'{path}: must be an empty directory', param, ctx)

        return path


def with_working_directory():
    keep_option_choices = {
        'on-error': WorkingDirectory.CleanupMode.KEEP_ON_ERROR,
        'always': WorkingDirectory.CleanupMode.KEEP_ALWAYS,
        'never': WorkingDirectory.CleanupMode.DELETE_ALWAYS
    }

    def decorator(func: t.Callable) -> t.Callable:
        @cloup.option_group(
            "Working Directory",
            cloup.option('-C', '--working-directory', type=DirectoryPathType(empty=True),
                         help='location to install Cassandra and/or Prometheus. Must be empty or not exist. Defaults to a temporary directory.'),
            cloup.option('--cleanup-working-directory', type=DictChoice(keep_option_choices, case_sensitive=False),
                         default='on-error', show_default=True,
                         help='how to delete the working directory on exit: '
                              '"on-error": delete working directory on exit unless an error occurs, '
                              '"always": always delete working directory on exit, '
                              '"never": never delete working directory.')
        )
        @click.pass_context
        @wraps(func)
        def wrapper(ctx: click.Context, working_directory: Path,
                    cleanup_working_directory: WorkingDirectory.CleanupMode, **kwargs):
            working_directory = ctx.with_resource(WorkingDirectory(cleanup_working_directory, working_directory))

            fixup_kwargs()

            func(**kwargs)

        return wrapper

    return decorator
