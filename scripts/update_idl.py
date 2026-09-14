#!/usr/bin/env python3
"""Download current official Pump/PumpSwap IDLs.

This intentionally does not overwrite the bundled snapshot automatically. Review diffs and
run the test suite before promoting a new IDL into production.
"""
from pathlib import Path
from urllib.request import urlopen

URLS = {
    "pump": "https://raw.githubusercontent.com/pump-fun/pump-public-docs/refs/heads/main/idl/pump.json",
    "pump_amm": "https://raw.githubusercontent.com/pump-fun/pump-public-docs/refs/heads/main/idl/pump_amm.json",
}


def main() -> None:
    target = Path("idl/upstream")
    target.mkdir(parents=True, exist_ok=True)
    for name, url in URLS.items():
        with urlopen(url, timeout=30) as response:
            body = response.read()
        path = target / f"{name}.json"
        path.write_bytes(body)
        print(f"wrote {path} ({len(body):,} bytes)")


if __name__ == "__main__":
    main()
