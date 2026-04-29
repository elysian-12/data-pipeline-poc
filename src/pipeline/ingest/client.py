"""Shared HTTP plumbing: async client, retry policy, per-provider rate limiting."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipeline.observability.logging import get_logger

log = get_logger(__name__)


class RateLimitError(RuntimeError):
    """HTTP 429 — tenacity treats this as retryable."""


class TransientHTTPError(RuntimeError):
    """5xx or connection error — tenacity treats this as retryable."""


@dataclass
class ProviderClient:
    """httpx.AsyncClient wrapped with a semaphore and a retry policy.

    One instance per provider (Massive, CoinGecko) so rate limits stay isolated.
    """

    base_url: str
    concurrency: int
    max_retries: int
    timeout_seconds: float
    default_headers: dict[str, str] = field(default_factory=dict)

    _client: httpx.AsyncClient | None = None
    _sem: asyncio.Semaphore | None = None

    async def __aenter__(self) -> ProviderClient:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            headers=self.default_headers,
        )
        self._sem = asyncio.Semaphore(self.concurrency)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        assert self._client is not None and self._sem is not None, "use as async ctx manager"

        async def _call() -> dict[str, Any]:
            async with self._sem:  # type: ignore[union-attr]
                try:
                    resp = await self._client.get(url, params=params, headers=headers)  # type: ignore[union-attr]
                except (httpx.ConnectError, httpx.ReadTimeout) as exc:
                    raise TransientHTTPError(str(exc)) from exc

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait_s = float(retry_after) if retry_after else 0.0
                    if wait_s > 0:
                        log.warning("http.429.retry_after", url=url, wait_s=wait_s)
                        await asyncio.sleep(wait_s)
                    raise RateLimitError(f"429 from {url}")
                if 500 <= resp.status_code < 600:
                    raise TransientHTTPError(f"{resp.status_code} from {url}")
                resp.raise_for_status()
                return resp.json()  # type: ignore[no-any-return]

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.max_retries + 1),
                wait=wait_exponential_jitter(initial=1, max=30),
                retry=retry_if_exception_type((RateLimitError, TransientHTTPError)),
                reraise=True,
            ):
                with attempt:
                    return await _call()
        except RetryError as exc:  # pragma: no cover — reraise=True means inner raises
            raise (exc.last_attempt.exception() or exc) from exc
        raise RuntimeError("unreachable")  # pragma: no cover


@asynccontextmanager
async def provider(
    base_url: str,
    *,
    concurrency: int,
    max_retries: int,
    timeout_seconds: float,
    headers: dict[str, str] | None = None,
) -> AsyncIterator[ProviderClient]:
    async with ProviderClient(
        base_url=base_url,
        concurrency=concurrency,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        default_headers=dict(headers or {}),
    ) as client:
        yield client
