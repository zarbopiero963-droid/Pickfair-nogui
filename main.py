from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def run_gui() -> int:
    from mini_gui import main as gui_main

    gui_main()
    return 0


def run_headless() -> int:
    from headless_main import main as headless_main

    return int(headless_main() or 0)


def main() -> int:
    args = [str(x).strip().lower() for x in sys.argv[1:]]

    if "--headless" in args or "headless" in args:
        logger.info("Avvio Pickfair in modalità HEADLESS")
        return run_headless()

    logger.info("Avvio Pickfair in modalità MINI GUI")
    return run_gui()


if __name__ == "__main__":
    raise SystemExit(main())