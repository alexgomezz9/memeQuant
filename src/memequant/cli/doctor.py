from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

import websockets

from memequant.config import Settings
from memequant.constants import PROGRAMS
from memequant.protocols.decoder import default_decoders
from memequant.rpc import SolanaHttpRpc
from memequant.security import redact_known_secrets


async def _network_checks(settings: Settings) -> dict:
    settings.require_live_urls()
    out: dict = {"http_rpc": {}, "websocket": {}}
    rpc = SolanaHttpRpc(
        settings.solana_http_rpc_url,
        concurrency=1,
        max_tx_version=settings.max_supported_transaction_version,
    )
    try:
        version = await rpc.call("getVersion", [])
        out["http_rpc"]["ok"] = True
        out["http_rpc"]["solana_core"] = (version or {}).get("solana-core")
        first_program = PROGRAMS[settings.enabled_protocols[0]]
        latest = await rpc.get_signatures_for_address(
            first_program, commitment=settings.commitment, limit=1
        )
        out["http_rpc"]["latest_program_signature_visible"] = bool(latest)
    finally:
        await rpc.close()

    async with websockets.connect(
        settings.solana_ws_rpc_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=2 * 1024 * 1024,
    ) as ws:
        program = PROGRAMS[settings.enabled_protocols[0]]
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "blockSubscribe",
            "params": [
                {"mentionsAccountOrProgram": program},
                {
                    "commitment": settings.commitment,
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": settings.max_supported_transaction_version,
                    "showRewards": False,
                },
            ],
        }
        await ws.send(json.dumps(request))
        async with asyncio.timeout(10):
            reply = json.loads(await ws.recv())
        if "error" in reply:
            out["websocket"]["block_subscribe_ok"] = False
            out["websocket"]["block_subscribe_error"] = reply["error"]
        else:
            out["websocket"]["block_subscribe_ok"] = True
            out["websocket"]["subscription_id"] = reply.get("result")
    return out


def _local_checks(settings: Settings) -> dict:
    decoders = list(default_decoders())
    idls = {}
    for decoder in decoders:
        path: Path = decoder.idl_decoder.idl_path
        body = path.read_bytes()
        idls[decoder.protocol] = {
            "program_id": decoder.program_id,
            "path": str(path),
            "sha256": hashlib.sha256(body).hexdigest(),
            "events": sorted(decoder.idl_decoder.events_by_disc.values()),
        }
    probe = settings.data_dir / ".doctor-write-test"
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    probe.write_text("ok")
    probe.unlink()
    return {
        "local_ok": True,
        "data_dir": str(settings.data_dir),
        "data_dir_writable": True,
        "enabled_protocols": list(settings.enabled_protocols),
        "commitment": settings.commitment,
        "subscription_mode": settings.subscription_mode,
        "max_supported_transaction_version": settings.max_supported_transaction_version,
        "idls": idls,
    }


async def _main(network: bool) -> None:
    settings = Settings()
    output = _local_checks(settings)
    if network:
        try:
            output["network"] = await _network_checks(settings)
        except Exception as exc:
            output["network"] = {
                "ok": False,
                "error": redact_known_secrets(
                    f"{type(exc).__name__}: {exc}",
                    settings.solana_http_rpc_url,
                    settings.solana_ws_rpc_url,
                ),
            }
            print(json.dumps(output, indent=2, ensure_ascii=False))
            raise SystemExit(2) from exc
    print(json.dumps(output, indent=2, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate MemeQuant local setup and optional RPC access")
    parser.add_argument(
        "--network",
        action="store_true",
        help="also test HTTP RPC and whether the provider accepts blockSubscribe",
    )
    args = parser.parse_args()
    asyncio.run(_main(args.network))


if __name__ == "__main__":
    main()
