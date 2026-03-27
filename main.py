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

    result = headless_main()
    return int(result or 0)


def main() -> int:
    try:
        args = [str(x).strip().lower() for x in sys.argv[1:]]

        if "--headless" in args or "headless" in args:
            logger.info("Avvio Pickfair in modalità HEADLESS")
            return run_headless()

        logger.info("Avvio Pickfair in modalità MINI GUI")
        return run_gui()

    except KeyboardInterrupt:
        logger.info("Arresto richiesto dall'utente")
        return 130
    except Exception as exc:
        logger.exception("Errore fatale in main: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())