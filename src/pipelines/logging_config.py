import logging
import sys

PIPELINE_LOGGER_NAME = "pipelines"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        stream=sys.stdout,
    )
    logging.getLogger(PIPELINE_LOGGER_NAME).setLevel(logging.INFO)
