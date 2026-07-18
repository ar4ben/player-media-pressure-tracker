from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Self

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


@dataclass(frozen=True)
class HttpSettings:
    connect_timeout: float = 3.5
    read_timeout: float = 10
    # retry policy: 0s, 2s, 4s, 8s for consecutive retry attempts with total=4 and backoff_factor=1
    retry_total: int = 4
    retry_backoff_factor: float = 1
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


class HttpClient:
    """Own a requests session configured with the shared retry policy."""

    def __init__(self, settings: HttpSettings | None = None) -> None:
        self.http_settings = settings or HttpSettings()
        self.session = self._create_session()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.session.close()

    @property
    def timeout(self) -> tuple[float, float]:
        return (
            self.http_settings.connect_timeout,
            self.http_settings.read_timeout,
        )

    def _create_session(self) -> requests.Session:
        retry_policy = Retry(
            total=self.http_settings.retry_total,
            backoff_factor=self.http_settings.retry_backoff_factor,
            status_forcelist=self.http_settings.retry_statuses,
            raise_on_status=False,
        )

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
