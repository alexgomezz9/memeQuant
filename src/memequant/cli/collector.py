import asyncio
import logging

from memequant.app import build_engine
from memequant.config import Settings
from memequant.ingestion.live import LiveCollector
from memequant.logging import configure_logging


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


if __name__ == "__main__":
    main()
