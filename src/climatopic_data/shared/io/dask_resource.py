import dagster as dg
from distributed.client import Client


class DaskResource(dg.ConfigurableResource):
    processes: bool = True
    """Whether to use processes (True) or threads (False). Defaults to True."""

    n_workers: int | None = None
    """The number of workers to start."""

    # memory_limit: str | float | Literal['auto'] = 'auto'
    # """The amount of memory to use *per worker*."""

    threads_per_worker: int | None = None
    """The number of threads to use per worker."""

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._client = Client(
            # memory_limit=self.memory_limit,
            n_workers=self.n_workers,
            processes=self.processes,
            threads_per_worker=self.threads_per_worker,
        )
        context.log.info(f'Dask dashboard: {self._client.dashboard_link}')
        context.log.info(self._client)

    def teardown_after_execution(self, context):
        self._client.close()

    @property
    def client(self) -> Client:
        return self._client
