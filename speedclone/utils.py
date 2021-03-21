import asyncio
import copy
import datetime

# import logging
import os
from functools import reduce

import aiostream
from httpx import StreamConsumed

from .ahttpx import create_client


def format_path(*path):
    formated_path = [
        os.path.normpath(p).replace("\\", "/").strip("/") for p in path if p
    ]
    return "/".join(formated_path)


def iter_path(path):
    if os.path.isfile(path):
        yield path
    else:
        for root, _, files in os.walk(path):
            for filename in files:
                yield os.path.join(root, filename)


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


def deep_merge(*dicts):
    def deep_search(dict1, dict2):
        dict1 = copy.deepcopy(dict1)
        for key in dict2.keys():
            if key not in dict1.keys():
                dict1[key] = dict2[key]
            else:
                dict1[key] = deep_search(dict1[key], dict2[key])
        return dict1

    return reduce(deep_search, dicts, {})


GMT_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_gmtdatetime():
    return datetime.datetime.utcnow().strftime(GMT_FORMAT)


def utc_to_datetime(utc_string):
    return datetime.datetime.strptime(utc_string, UTC_FORMAT)


def parse_headers(headers, join=":"):
    return "\n".join([k + join + v for k, v in headers.items()])


def parse_params(params):
    parsed_params = "&".join([f"{k}{'=' if v else ''}{v}" for k, v in params.items()])
    if parsed_params:
        parsed_params = "?" + parsed_params
    return parsed_params


def parse_cookies(cookies):
    return "; ".join([f"{k}={v}" for k, v in cookies.items()])


def parse_ranges(ranges):
    return "bytes=" + ", ".join([f"{start}-{end}" for start, end in ranges])


def raise_for_status(response):
    error_code = response.status_code // 100
    if error_code == 4 or error_code == 5:
        from .error import HttpStatusError

        raise HttpStatusError(response=response)


class HttpxBytesReader(object):
    def __init__(self, response):
        self._bytes_iter = response.aiter_bytes()
        self._total = b""

    async def read(self, n):
        try:
            async for block in self._bytes_iter:
                self._total += block
                if len(self._total) >= n:
                    result = self._total[:n]
                    self._total = self._total[n:]
                    return result
        except StreamConsumed:
            pass

        remains = self._total
        self._total = b""
        return remains


class OrderedIterator:
    def __init__(self, start=0):
        self._loop = asyncio.get_event_loop()

        self._queue = {}
        self._start = start
        self._index = self._start
        self._finished = False

    def _get_future(self, index):
        if index not in self._queue:
            self._queue[index] = self._loop.create_future()
            if self._finished:
                self._queue[index].set_result(None)
        return self._queue[index]

    def _set_future(self, index, item):
        f = self._get_future(index)
        f.set_result(item)

    def _set_finished(self):
        self._finished = True
        for f in self._queue.values():
            if not f.done():
                f.set_result(None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._set_finished()

    async def __aiter__(self):
        while True:
            future = self._get_future(self._index)

            result = await future

            if result is None:
                break
            else:
                yield result

            self._queue.pop(self._index)
            self._index += 1

    def push(self, index, item):
        self._set_future(index, item)
        self._counted += 1

    def reset(self):
        self._queue.clear()
        self._counted = 0


CLIENT_ARGS_KEYS = ("cert", "verify", "timeout", "trust_env", "proxies")


class MultiWorkersRequest:
    def __init__(self, http_kwargs, max_workers=5, allow_multiple_ranges=False):
        self.http_kwargs = http_kwargs
        self.max_workers = max_workers
        self.allow_multiple_ranges = allow_multiple_ranges

        self.sem = asyncio.Semaphore(self.max_workers)
        self.loop = asyncio.get_event_loop()
        self.oi = OrderedIterator()

    def _get_ranges(self, start, end, chunk_size, ranges_num):
        ranges = [
            (i, (r, min(r + chunk_size, end)))
            for i, r in enumerate(range(start, end, chunk_size))
        ]

        return list(filter(None, [ranges[w::ranges_num] for w in range(ranges_num)]))

    async def _single_request(self, client, http_range, chunk_size):
        async with self.sem:
            async with client.stream(
                "GET",
                **deep_merge(
                    self.http_kwargs,
                    {
                        "headers": {
                            "Range": parse_ranges([r[1] for r in http_range]),
                        },
                    },
                ),
            ) as response:
                raise_for_status(response)

                bytes_reader = HttpxBytesReader(response)
                for i, (start, end) in http_range:
                    chunk = await bytes_reader.read(end - start)
                    yield i, chunk

    def _get_streamers(self, client, http_ranges, chunk_size):
        return [
            self._single_request(
                client=client,
                http_range=http_range,
                chunk_size=chunk_size,
            )
            for http_range in http_ranges
        ]

    async def _push_streamers(self, chunk_streamers):
        with self.oi:
            async with aiostream.stream.merge(*chunk_streamers).stream() as streamer:
                async for i, chunk in streamer:
                    self.oi.push(i, chunk)

    async def aiter_chunk(self, start, end, chunk_size):
        self.oi.reset()

        ranges_num = (
            int((end - start) / chunk_size) + 1
            if self.allow_multiple_ranges is False
            else self.max_workers
        )

        http_ranges = self._get_ranges(
            start=start, end=end, chunk_size=chunk_size, ranges_num=ranges_num
        )

        client_args = {
            k: v for k, v in self.http_kwargs.items() if k in CLIENT_ARGS_KEYS
        }

        self.http_kwargs = {
            k: v for k, v in self.http_kwargs.items() if k not in CLIENT_ARGS_KEYS
        }

        async with create_client(**client_args) as client:
            streamers = self._get_streamers(
                client=client, http_ranges=http_ranges, chunk_size=chunk_size
            )

            asyncio.run_coroutine_threadsafe(
                self._push_streamers(chunk_streamers=streamers), self.loop
            ).add_done_callback(lambda f: f.result())

            async for chunk in self.oi:
                yield chunk

    async def aread(self, start, end, chunk_size):
        data = b""
        async for chunk in self.aiter_chunk(
            start=start, end=end, chunk_size=chunk_size
        ):
            data += chunk
        return data


# def compose_path(path):
#     return reduce(lambda x, y: x + [format_path(x[-1], y)], path.split("/"), [""])[1:]
