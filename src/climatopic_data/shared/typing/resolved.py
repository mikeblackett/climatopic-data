from pathlib import Path
from typing import Annotated, TypeAlias

import dagster as dg
from upath import UPath

__all__ = ['ResolvedPath', 'ResolvedUPath']


def _resolve_path(
    context: dg.ResolutionContext,
    str_path: str,
) -> Path:
    return Path(str_path)


ResolvedPath: TypeAlias = Annotated[
    Path, dg.Resolver(_resolve_path, model_field_type=str)
]


def _resolve_upath(
    context: dg.ResolutionContext,
    str_path: str,
) -> UPath:
    return UPath(str_path)


ResolvedUPath: TypeAlias = Annotated[
    UPath, dg.Resolver(_resolve_upath, model_field_type=str)
]
