import logging

from rich import traceback
from rich.console import Console
from rich.logging import RichHandler


console = Console()
traceback.install(console=console)


def init_logger():
    FORMAT = "%(message)s"
    logging.basicConfig(
        level="INFO",
        format=FORMAT,
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=console,
                markup=True,
                omit_repeated_times=False,
                rich_tracebacks=True,
            )
        ],
    )
