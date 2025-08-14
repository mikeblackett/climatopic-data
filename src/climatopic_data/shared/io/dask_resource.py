"""Resource for sharing a Dask LocalCluster across components."""

import dagster as dg
from distributed.client import Client


class DaskResource(dg.ConfigurableResource):
    """Resource for sharing a Dask LocalCluster across components.

    Args:
        processes (bool, optional): Whether to use processes (True) or threads (False). Defaults to True.
        n_workers (int | None, optional): The number of workers to start.
        threads_per_worker (int | None, optional): The number of threads to use per worker.
    """

    processes: bool = True
    n_workers: int | None = None
    threads_per_worker: int | None = None
    # memory_limit: str | float | Literal['auto'] = 'auto'

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._client = Client(
            # memory_limit=self.memory_limit,
            n_workers=self.n_workers,
            processes=self.processes,
            threads_per_worker=self.threads_per_worker,
        )
        context.log.info(
            f'Dask LocalCluster {self._client}. '
            f'Dashboard: {self._client.dashboard_link}'
        )

    def teardown_after_execution(self, context):
        self._client.close()

    @property
    def client(self) -> Client:
        """Dask LocalCluster."""
        return self._client
