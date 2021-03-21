import logging

import colorlog
from tqdm.autonotebook import tqdm


class TqdmHandler(logging.StreamHandler):
    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)


def init_logger(level=logging.INFO):
    handler = TqdmHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%d-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(level)
