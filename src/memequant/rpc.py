from __future__ import annotations

import asyncio
import itertools
from typing import Any

import httpx


class RpcError(RuntimeError):
    def __init__(self, method: str, error: Any):
        super().__init__(f"RPC {method} failed: {error}")
        self.method = method
        self.error = error


class SolanaHttpRpc:
    def __init__(
        self, url: str, concurrency: int = 8, timeout: float = 30.0, max_tx_version: int = 0
    ):
        self.url = url
        self.client = httpx.AsyncClient(timeout=timeout)
        self._ids = itertools.count(1)
        self._sem = asyncio.Semaphore(concurrency)
        self.max_tx_version = max_tx_version

    async def call(self, method: str, params: list[Any]) -> Any:
        async with self._sem:
            response = await self.client.post(
                self.url,
                json={"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params},
            )
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise RpcError(method, body["error"])
            return body.get("result")

    async def get_transaction(self, signature: str, commitment: str = "confirmed"):
        return await self.call(
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
