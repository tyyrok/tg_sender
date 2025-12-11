import logging
import sys


logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="[%(levelname)s][%(name)s]: %(message)s",
)
logging.getLogger("TG_SENDER").setLevel(logging.INFO)
logger = logging.getLogger("TG_SENDER")
