import asyncio
import logging

from memequant.app import build_engine
from memequant.config import Settings
from memequant.ingestion.live import LiveCollector
from memequant.ingestion.reconcile import ReconciliationError
from memequant.logging import configure_logging
from memequant.security import redact_text


async def _run() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    engine = build_engine(settings)
    collector = LiveCollector(settings, engine)
    logging.getLogger(__name__).info(
        "collector starting", extra={"mode": settings.subscription_mode}
    )
    try:
        await collector.run()
    finally:
        await collector.close()
        engine.close()
        engine.state.close()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
    except ReconciliationError as exc:
        logging.getLogger(__name__).critical(
            "collector stopped: %s", redact_text(str(exc))
        )
        raise SystemExit(2) from None
    except Exception as exc:
        # Settings/constructor failures happen before the JSON logger is configured and
        # may include an RPC URL. Never let the interpreter print an unredacted traceback.
        logging.getLogger(__name__).critical(
            "collector failed: %s", redact_text(f"{type(exc).__name__}: {exc}")
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
