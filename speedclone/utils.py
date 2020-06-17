# import functools
import os
import time

import aiostream
from colorama import Fore, init
from tqdm.autonotebook import tqdm

init()


def norm_path(*path):
    norm = [os.path.normpath(p).replace("\\", "/").strip("/") for p in path if p]
    return "/".join(norm)


def get_now_time():
    return "[" + time.strftime("%H:%M:%S", time.localtime()) + "]"


def iter_path(path):
    if os.path.isfile(path):
        yield path
    else:
        for root, _, files in os.walk(path):
            for filename in files:
                yield os.path.join(root, filename)


def console_write(mode, message):
    if mode == "sleep":
        tqdm.write(
            "{color}[{message}]{reset}".format(
                color=Fore.BLUE, message=message, reset=Fore.RESET,
            )
        )

    elif mode == "error":
        tqdm.write(
            "{color}[{message}]{reset}".format(
                color=Fore.RED, message=message, reset=Fore.RESET
            )
        )

    elif mode == "exists":
        tqdm.write(
            "{color}[{message}]{reset}".format(
                color=Fore.YELLOW, message=message, reset=Fore.RESET
            )
        )

    elif mode == "fail":
        tqdm.write(
            "{color}[{message}]{reset}".format(
                color=Fore.RED, message=message, reset=Fore.RESET
            )
        )


async def data_iter(file_piece, step_size, bar):
    while file_piece:
        step = file_piece[:step_size]
        yield step
        bar.update(len(step))
        file_piece = file_piece[step_size:]


async def aenumerate(asequence, start=0):
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1


async def aiter_bytes(asequence, chunk_size=1024):
    with aiostream.stream.chunks(asequence, n=chunk_size).stream() as streamer:
        async for data in streamer:
            yield b"".join(data)
