from collections.abc import Iterable
from os import PathLike
from typing import Any, Literal

import xarray as xr
import dask.array as da

from climatopic_data.shared.typing import Chunks

CHUNKS_SPACE = {
    'time': -1,
    'projection_x_coordinate': 'auto',
    'projection_y_coordinate': 'auto',
}

CHUNKS_TIME = {
    'time': 'auto',
    'projection_x_coordinate': -1,
    'projection_y_coordinate': -1,
}


def balance_chunks[T: xr.DataArray | xr.Dataset](
    obj: T, block_size_limit=1e8
) -> T:
    if isinstance(obj, xr.Dataset):
        return obj.map(balance_chunks, block_size_limit=block_size_limit)
    chunked = da.rechunk(
        obj.data,
        chunks='auto',
        balance=True,
        block_size_limit=block_size_limit,
    )
    return obj.copy(data=chunked)


def open_distributed_dataset(
    paths: Iterable[PathLike],
    *,
    concat_dim: str = 'time',
    chunks: Literal['auto'] | Chunks = None,
    parallel: bool = True,
    backend_kwargs: dict[str, Any] | None = None,
) -> xr.Dataset:
    """Open multiple files as a single dataset.

    This function is a wrapper around `xarray.open_mfdataset` optimized for
    multi-file datasets separated by a common dimension.

    Variables that contain the `concat_dim` dimension are concatenated along
    the common dimension. Variables that do not contain the `concat_dim`
    dimension are taken from the first dataset.

    Parameters
    ----------
    path : Path | str
        Path to the directory containing the netCDF files.
    concat_dim : str, optional
        Dimension to concatenate the variables along, by default "time".
    chunks : Chunks, optional
        Per-file chunk size, by default `None`.
        The default is to load entire input files into memory at once.
    parallel : bool, optional
        Whether to parallelize the opening of the files, by default True.
    backend_kwargs
        Options to pass to the backend engine.
    """
    return xr.open_mfdataset(
        paths=paths,  # type: ignore
        chunks=chunks,
        combine='nested',
        combine_attrs='override',
        compat='override',
        concat_dim=concat_dim,
        coords='minimal',
        data_vars='minimal',
        engine='h5netcdf',
        parallel=parallel,
        backend_kwargs=backend_kwargs,
    )
