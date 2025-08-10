from enum import StrEnum

import xarray as xr
import dagster as dg
from upath import UPath

from climatopic_data.shared.typing import Chunks


class ZarrWriteMode(StrEnum):
    """Enum for Zarr write modes."""

    CREATE = 'w'
    """Create (overwrite if exists)"""
    CREATE_SAFE = 'w-'
    """Create (fail if exists)"""
    APPEND = 'a'
    """Override all existing variables (create if not exists)"""
    APPEND_DIM = 'a-'
    """Append only those variables that have `append_dim`"""
    MODIFY = 'r+'
    """Modify existing array values only (fail if any metadata or shapes would change)"""


class XarrayIOManager(dg.UPathIOManager):
    """An IOManager for reading and writing xarray datasets from Zarr stores."""

    def dump_to_path(
        self,
        context: dg.OutputContext,
        obj: xr.Dataset | xr.DataArray,
        path: UPath,
    ) -> None:
        context.log.info(
            (
                f'{type(self).__name__}: Saving asset '
                f'{context.get_asset_identifier()!r} to Zarr store {path!r}.'
            )
        )

        metadata = context.metadata or {}
        mode = ZarrWriteMode(metadata.get('zarr/mode', 'w')).value
        consolidated = metadata.get('zarr/consolidated', True)

        chunks: Chunks = metadata.get('xarray/chunks')
        if chunks is not None:
            obj = obj.chunk(chunks)

        obj.to_zarr(store=path, mode=mode, consolidated=consolidated)

    def load_from_path(
        self,
        context: dg.InputContext,
        path: UPath,
    ) -> xr.Dataset:
        context.log.info(
            (
                f'{type(self).__name__}: Loading asset '
                f'{context.get_asset_identifier()!r} from Zarr store {path!r}.'
            )
        )
        metadata = context.metadata or {}
        chunks: Chunks = metadata.get('xarray/chunks', {})
        return xr.open_dataset(
            path,
            chunks=chunks,
            # Avoid decoding data variables with `units: days` as `timedelta64`
            # See: https://github.com/pydata/xarray/issues/1621
            decode_timedelta=False,
            engine='zarr',
        )
