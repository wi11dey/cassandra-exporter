from os import PathLike
from pathlib import Path


def existing_file_arg(path: PathLike):
    path = Path(path)
    if not path.exists():
        raise ValueError(f'{path}: file does not exist.')

    if not path.is_file():
        raise ValueError(f'{path}: not a regular file.')

    return path


def nonexistent_or_empty_directory_arg(path):
    path = Path(path)

    if path.exists():
        if not path.is_dir():
            raise ValueError(f'"{path}" must be a directory.')

        if next(path.iterdir(), None) is not None:
            raise ValueError(f'"{path}" must be an empty directory.')

    return path