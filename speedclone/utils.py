
import os
from functools import reduce

import aiostream


def norm_path(*path):
    norm = [os.path.normpath(p).replace("\\", "/").strip("/") for p in path if p]
    return "/".join(norm)


def iter_path(path):
    if os.path.isfile(path):
        yield path
    else:
        for root, _, files in os.walk(path):
            for filename in files:
                yield os.path.join(root, filename)


def compose_path(path):
    return reduce(lambda x, y: x + [norm_path(x[-1], y)], path.split("/"), [""])[1:]


# def console_write(mode, message):
#     if mode == "sleep":
#         tqdm.write(
#             "{color}[{message}]{reset}".format(
#                 color=Fore.BLUE, message=message, reset=Fore.RESET,
#             )
#         )

#     elif mode == "error":
#         tqdm.write(
#             "{color}[{message}]{reset}".format(
#                 color=Fore.RED, message=message, reset=Fore.RESET
#             )
#         )

#     elif mode == "exists":
#         tqdm.write(
#             "{color}[{message}]{reset}".format(
#                 color=Fore.YELLOW, message=message, reset=Fore.RESET
#             )
#         )

#     elif mode == "fail":
#         tqdm.write(
#             "{color}[{message}]{reset}".format(
#                 color=Fore.RED, message=message, reset=Fore.RESET
#             )
#         )


async def aenumerate(asequence, start=0):
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1


async def aiter_data(file_piece, step_size, bar):
    while file_piece:
        step = file_piece[:step_size]
        yield step
        bar.update(len(step))
        file_piece = file_piece[step_size:]


async def aiter_bytes(asequence, chunk_size=1024):
    with aiostream.stream.chunks(asequence, n=chunk_size).stream() as streamer:
        async for data in streamer:
            yield b"".join(data)


async def aiter_with_end(iterator):
    async def _():
        yield None

    last = None
    async with aiostream.stream.chain(iterator, _()).stream() as streamer:
        async for item in streamer:
            if not last:
                continue
            elif item:
                last = item
            yield last, False if item else True
