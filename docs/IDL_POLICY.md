# IDL policy

Pump programs evolve. Silent schema drift is unacceptable for research.

Bundled files under `src/memequant/idl/` are event-only snapshots reviewed against the official repository on 2026-09-10. They are deliberately version-controlled.

Before deployment:

```bash
python scripts/update_idl.py
```

This downloads full upstream IDLs into `idl/upstream/` without changing the bundled production schemas. Review event discriminators/field order/types. If a relevant event changed, update the bundled snapshot and fixtures together, then run `pytest`.

The runtime decoder quarantines recognized events with trailing bytes and short/invalid payloads rather than assuming that an old schema is still valid.
