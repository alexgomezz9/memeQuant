from __future__ import annotations

import asyncio
import itertools
import random
from collections.abc import Callable
from typing import Any

import httpx


class RpcError(RuntimeError):
    def __init__(self, method: str, error: Any):
        super().__init__(f"RPC {method} failed: {error}")
        self.method = method
        self.error = error


class SolanaHttpRpc:
    def __init__(
        self,
        url: str,
        concurrency: int = 8,
        timeout: float = 30.0,
        max_tx_version: int = 0,
        rps_limit: float = 6.0,
        max_retries: int = 5,
        metric_sink: Callable[[str, int], None] | None = None,
    ):
        self.url = url
        self.client = httpx.AsyncClient(timeout=timeout)
        self._ids = itertools.count(1)
        self._sem = asyncio.Semaphore(concurrency)
        self._rate_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._min_interval = 1.0 / float(rps_limit)
        self.max_tx_version = max_tx_version
        self.max_retries = max_retries
        self._metric_sink = metric_sink

    def _count(self, key: str, amount: int = 1) -> None:
        if self._metric_sink is not None:
            self._metric_sink(key, amount)

    def count_get_transaction_retry(self) -> None:
        """Record a retry requested by a caller after a null transaction result."""
        self._count("getTransaction_retries")

    async def _pace(self) -> None:
        """Globally pace requests so concurrent workers cannot burst above the tier limit."""
        async with self._rate_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if now < self._next_request_at:
                await asyncio.sleep(self._next_request_at - now)
                now = loop.time()
            self._next_request_at = max(now, self._next_request_at) + self._min_interval

    async def call(self, method: str, params: list[Any]) -> Any:
        last_response: httpx.Response | None = None
        for attempt in range(self.max_retries + 1):
            await self._pace()
            async with self._sem:
                response = await self.client.post(
                    self.url,
                    json={
                        "jsonrpc": "2.0",
                        "id": next(self._ids),
                        "method": method,
                        "params": params,
                    },
                )
            last_response = response

            if response.status_code == 429:
                self._count("http_429_count")

            if response.status_code not in {429, 503}:
                response.raise_for_status()
                body = response.json()
                if "error" in body:
                    raise RpcError(method, body["error"])
                return body.get("result")

            if attempt >= self.max_retries:
                break

            if method == "getTransaction":
                self._count("getTransaction_retries")

            retry_after = response.headers.get("retry-after")
            if retry_after is not None:
                try:
                    delay = max(0.0, float(retry_after))
                except ValueError:
                    delay = 0.0
            else:
                delay = min(1.0 * (2**attempt), 30.0)
            # Small jitter prevents reconnect/reconcile loops from re-hitting the same window.
            delay *= random.uniform(0.85, 1.15)
            await asyncio.sleep(delay)

        assert last_response is not None
        last_response.raise_for_status()
        raise RuntimeError("unreachable")

    async def get_transaction(self, signature: str, commitment: str = "confirmed"):
        self._count("getTransaction_started")
        result = await self.call(
            "getTransaction",
            [
                signature,
                {
                    "commitment": commitment,
                    "encoding": "json",
                    "maxSupportedTransactionVersion": self.max_tx_version,
                },
            ],
        )
        self._count("getTransaction_completed")
        return result

    async def get_signatures_for_address(
        self,
        address: str,
        *,
        commitment: str = "confirmed",
        until: str | None = None,
        before: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        config: dict[str, Any] = {"commitment": commitment, "limit": min(limit, 1000)}
        if until:
            config["until"] = until
        if before:
            config["before"] = before
        return await self.call("getSignaturesForAddress", [address, config])

    async def close(self) -> None:
        await self.client.aclose()
