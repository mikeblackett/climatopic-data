from dataclasses import dataclass

import dagster as dg
import xarray as xr

from climatopic_data.lib.constants import CHUNKS_SPACE
from climatopic_data.lib.io_managers.xarray_io_manager import (
    XarrayIOManager,
)
from climatopic_data.lib.resolved import ResolvedUPath
from climatopic_data.lib.resources.ceda_resource import CEDAHTTPResource
from climatopic_data.lib.resources.dask_resource import DaskResource
from climatopic_data.lib.utils import open_distributed_dataset


class HadUKGridVariable(dg.Model):
    name: str
    description: str
    frequencies: list[str]


CEDA_PATH_TEMPLATE = (
    '{version}/{resolution}/{name}/{frequency}/{release}/catalog'
)


@dataclass
class HadUKGridComponent(dg.Component, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """

    base_path: ResolvedUPath
    base_url: str
    release: str
    resolution: str
    variables: list[HadUKGridVariable]
    version: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _assets = []
        for spec in self.variables:

            @dg.asset(
                name=spec.name,
                description=spec.description,
                group_name='hadukgrid',
                partitions_def=dg.StaticPartitionsDefinition(spec.frequencies),
                io_manager_key='xarray_io_manager',
                metadata={'xarray/chunks': CHUNKS_SPACE},
            )
            def _asset(
                context: dg.AssetExecutionContext,
                ceda: CEDAHTTPResource,
                dask_client: DaskResource,
            ) -> dg.Output[xr.Dataset]:
                name = context.asset_key.path[-1]
                frequency = context.partition_key
                path = CEDA_PATH_TEMPLATE.format(
                    version=self.version,
                    resolution=self.resolution,
                    name=name,
                    frequency=frequency,
                    release=self.release,
                )

                context.log.info(
                    f'{type(self).__name__}: Fetching file paths from URL {path!r}.'
                )
                paths = ceda.get_file_paths(path)

                dataset = open_distributed_dataset(
                    paths,
                    concat_dim='time',
                    backend_kwargs={
                        'storage_options': {
                            'headers': ceda.headers,
                        }
                    },
                )

                return dg.Output(value=dataset)

            _assets.append(_asset)

        # TODO: (mike) I am not sure why this is needed. I thought I should be
        #  able to declare `required_resource_keys` attribute on `XarrayIOManager`
        #  class, but it doesn't seem to work!
        @dg.io_manager(required_resource_keys={'dask_client'})
        def xarray_io_manager() -> XarrayIOManager:
            return XarrayIOManager(base_path=self.base_path)

        _resources = {
            'ceda': CEDAHTTPResource(
                base_url=self.base_url,
                access_token=dg.EnvVar('CEDA_ACCESS_TOKEN'),
            ),
            'dask_client': DaskResource(
                processes=False, n_workers=1, threads_per_worker=100
            ),
            'xarray_io_manager': xarray_io_manager,
        }

        return dg.Definitions(assets=_assets, resources=_resources)
