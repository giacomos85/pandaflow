import logging

logger = logging.getLogger("pandaflow")
logger.setLevel(logging.INFO)

# Optional: configure only if not already set (e.g. during testing)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
