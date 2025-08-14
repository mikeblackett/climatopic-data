"""Resource for THREDDS web server data access."""

import math
import random
import re
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import StrEnum
from fnmatch import fnmatch
from functools import cached_property
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import urljoin

import dagster as dg
import fsspec
from fsspec.implementations.http import HTTPFileSystem

# TODO (mike): Support copying files to a flattened directory structure.
#    Currently the source's directory structure is preserved, but we could add
#    an option to save files to a flat directory.

type TDSWriteModes = Literal[
    'w',
    'w-',
]


class TDSWriteMode(StrEnum):
    """Enum for file write modes."""

    CREATE = 'w'
    """Create (overwrite if exists)"""
    CREATE_SAFE = 'w-'
    """Create (skip if exists)"""


class TDSDownloadStatus(StrEnum):
    """Enum for file download status."""

    SUCCESS = 'success'
    SKIPPED = 'skipped'
    FAILED = 'failed'


MAX_RETRIES: Final[int] = 0
"""The maximum number of retries allowed when retrieving files."""
BACKOFF_BASE: Final[float] = 1.0
"""The base backoff time when retrieving files (seconds)."""
BACKOFF_JITTER: Final[float] = 0.3
"""The jitter factor when retrieving files."""
MAX_WORKERS: Final[int] = 4
"""The maximum number of workers allowed when retrieving files."""
BATCH_SIZE: Final[int] = 50
"""The batch size when retrieving files."""
BATCH_DELAY: Final[float] = 2.0
"""The batch delay when retrieving files."""


def _backoff(retry: int, base: float = 1.0, jitter: float = 0.3) -> float:
    """Return the backoff time for a given retry."""
    return base * (2**retry) * (1 + random.uniform(-jitter, jitter))


def _parse_glob_path(
    path: str,
) -> tuple[str, str]:
    """Split a glob path into a base path and a glob pattern."""
    if '*' not in path:
        return path, ''
    base, pattern = re.split(pattern=r'(?=\*)', string=path, maxsplit=1)
    return base, pattern


@dataclass(frozen=True)
class TDSFileHandle:
    """File handle for THREDDS data server downloads."""

    path: str | None = None
    status: TDSDownloadStatus = TDSDownloadStatus.FAILED
    error: Exception | None = None


class TDSResource(dg.ConfigurableResource):
    """Resource for THREDDS web server data access.

    Attributes:
        server_url (str): URL of THREDDS data server.
        base_path (str | None, optional): Base path for THREDDS data (relative to ``server_url``). Defaults to None.
        access_token (str | None, optional): Access token for THREDDS data server authentication. Defaults to None.
    """

    server_url: str
    base_path: str | None = None
    access_token: str | None = None

    def resolve_path(self, path: str) -> str:
        """Resolve a path to an absolute URL on the THREDDS server."""
        if self.base_path:
            path = urljoin(self.base_path, path)
        return urljoin(self.server_url, path)

    def iter_files(
        self,
        path: str,
        max_depth: int | None = None,
    ) -> Generator[str, None, None]:
        """Iterate over files below a given path.

        The path will be resolved to an absolute URL of the form: ``{server_url}/{base_path}/{path}``

        Args:
            path (str): Path to search from. Can contain shell-style wildcards.
            max_depth (int | None): Maximum directory depth to search. Only applies if the path contains ``**``

        Yields:
            str: Fully qualified remote file path.
        """
        url = self.resolve_path(path)
        base, pattern = _parse_glob_path(url)
        stack = [(base, 0)]

        if '**' not in pattern:
            max_depth = 1

        while stack:
            current_path, depth = stack.pop()

            if max_depth and depth > max_depth:
                continue

            for item in self.fs.ls(current_path, detail=True):
                path = item['name']
                if item.get('type') == 'file':
                    # Remove root (``http://``) for matching with ``fnmatch``.
                    relative_path = path[len(self.server_url) :].lstrip('/')
                    if fnmatch(relative_path, pattern):
                        yield path
                elif item.get('type') == 'directory':
                    stack.append((path, depth + 1))

    def list_files(self, path: str) -> list[str]:
        """List all file below a given path.

        The path will be resolved to an absolute URL of the form: ``{server_url}/{base_path}/{path}``

        Args:
             path (str): Path to search from. Can contain shell-style wildcards.

        Returns:
            list[str]: Fully qualified remote file path.
        """
        return list(self.iter_files(path=path))

    def copy_file(
        self,
        source: str,
        target: str,
        *,
        mode: TDSWriteModes = TDSWriteMode.CREATE_SAFE.value,
    ) -> TDSFileHandle:
        """Copy a single file to a local path.

        The source directory structure will be preserved.

        Args:
            source (str): Path to the file on the THREDDS server.
            target (str): Local path where the file will be stored.
            mode (TDSWriteModes, optional): Write mode for file creation. Defaults to ``TDSWriteMode.CREATE_SAFE``.

        Returns:
            TDSFileHandle: File handle containing download status, path (if successful), and error (if failed).
        """
        source = self.resolve_path(source)
        # Keep files organized like the source structure
        _target = Path(target) / Path(source[len(self.server_url) :])

        if _target.exists() and TDSWriteMode(mode) == TDSWriteMode.CREATE_SAFE:
            return TDSFileHandle(
                path=str(_target), status=TDSDownloadStatus.SKIPPED
            )

        last_error: Exception | None = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                _target.parent.mkdir(parents=True, exist_ok=True)
                self.fs.get_file(source, str(_target))
                return TDSFileHandle(
                    path=str(_target), status=TDSDownloadStatus.SUCCESS
                )
            except Exception as error:
                last_error = error
                if attempt < MAX_RETRIES:
                    time.sleep(_backoff(attempt, BACKOFF_BASE, BACKOFF_JITTER))
        # All attempts failed...
        return TDSFileHandle(status=TDSDownloadStatus.FAILED, error=last_error)

    def copy_files(
        self,
        source: str | list[str],
        target: str,
        mode: TDSWriteModes = TDSWriteMode.CREATE_SAFE.value,
        max_workers: int = MAX_WORKERS,
        batch_size: int = BATCH_SIZE,
    ) -> list[TDSFileHandle]:
        """Copy multiple files to a local path.

        The source directory structure will be preserved.

        Args:
            source (str | list[str]): Path or list of paths to the files on the THREDDS server. If a path is provided, it can contain shell-style wildcards.
            target (str): Local directory path where the files will be stored.
            mode (TDSWriteModes, optional): Write mode for file creation. Defaults to WriteMode.CREATE_SAFE.value.
            max_workers (int, optional): Maximum number of concurrent download workers. Defaults to MAX_WORKERS.
            batch_size (int, optional): Number of files to download in each batch. Defaults to BATCH_SIZE.

        Returns:
            list[TDSFileHandle]: List of file handles containing download status, paths (if successful),
                and errors (if failed) for each downloaded file.
        """
        if isinstance(source, str):
            source = self.list_files(source)
        total = len(source)
        batches = math.ceil(total / batch_size)
        results: list[TDSFileHandle] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for b in range(batches):
                batch = source[b * batch_size : (b + 1) * batch_size]

                future_to_path = {
                    executor.submit(
                        self.copy_file,
                        source=path,
                        target=target,
                        mode=mode,
                    ): path
                    for path in batch
                }

                for future in as_completed(future_to_path):
                    try:
                        file_handle = future.result()
                        results.append(file_handle)
                    except Exception as error:
                        results.append(
                            TDSFileHandle(
                                status=TDSDownloadStatus.FAILED, error=error
                            )
                        )

                if b < batches - 1:
                    time.sleep(BATCH_DELAY)

        return results

    @cached_property
    def fs(self) -> HTTPFileSystem:
        """FSSpec filesystem."""
        return fsspec.filesystem(
            'http',
            headers=self.headers,
        )

    @property
    def headers(self) -> dict[str, Any]:
        """Headers for requests."""
        if self.access_token:
            return {'Authorization': f'Bearer {self.access_token}'}
        return {}
