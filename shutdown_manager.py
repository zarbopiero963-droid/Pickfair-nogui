import logging

logger = logging.getLogger("SHUTDOWN")


class ShutdownManager:
    def __init__(self):
        self.handlers = []

    def register(self, name, fn, priority=10):
        self.handlers.append((priority, name, fn))
        self.handlers.sort(key=lambda x: x[0])

    def shutdown(self):
        for _, name, fn in self.handlers:
            try:
                logger.info("[SHUTDOWN] %s", name)
                fn()
            except Exception as e:
                logger.exception("Shutdown error: %s", e)

