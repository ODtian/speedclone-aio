import asyncio

import aiofiles
import aiostream
from httpx import AsyncClient

from .args import args_dict
from .utils import raise_for_status

PROXY = args_dict["PROXY"]


class SequentialQueue:
    def __init__(self, start=0):
        self._queue = asyncio.Queue()
        self._buffer = {}
        self._index = start

        self.items = self._items()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close(exc)
        return True

    async def _items(self):
        while True:
            result = await self._queue.get()

            if result is None:
                break

            if result[0] == "exc":
                raise result[1]

            index, data = result

            if index == self._index:
                self._index += 1
                yield data
            else:
                self._buffer[index] = data

            for index, data in sorted(self._buffer.items(), key=lambda item: item[0]):
                if index == self._index:
                    yield data
                    self._buffer.pop(index)
                    self._index += 1

    async def put(self, index, data):
        await self._queue.put((index, data))

    async def close(self, exc=None):
        if exc is not None:
            await self._queue.put(("exc", exc))
        else:
            await self._queue.put(None)


class LocalFileReader:
    def __init__(self, file_path, data_range=(0, None)):
        self.file_path = file_path
        self.start, self.end = data_range

        self.now_seek = self.start or 0
        self._file = None
        self._reader = None

    async def __aenter__(self):
        self._file = aiofiles.open(self.file_path, "rb")
        self._reader = await self._file.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._file.__aexit__(exc_type, exc, tb)

    async def read(self, n):
        await self._reader.seek(self.now_seek)

        data_len = n if self.end is None else min(self.end - self.now_seek, n)
        self.now_seek += data_len

        data = await self._reader.read(data_len)
        return data


class HttpFileReader:
    def __init__(
        self,
        url,
        data_range,
        max_workers,
        **request_args,
    ):
        self.url = url
        self.request_args = request_args
        self.start, self.end, self.range_size = data_range
        self.max_workers = max_workers

        self._sequential_queue = SequentialQueue(start=0)
        self._streamer_future = None
        self._buffer = bytearray(b"")

        self._closed = False

    async def __aenter__(self):
        loop = asyncio.get_event_loop()
        self._streamer_future = asyncio.run_coroutine_threadsafe(self._streamer(), loop)
        return self

    async def __aexit__(self, *args, **kwargs):
        self._streamer_future.cancel()
        del self._sequential_queue
        self._closed = True

    def _genarate_range(self):
        range_for_clients = [
            (index, left, min(left + self.range_size, self.end))
            for index, left in enumerate(range(self.start, self.end, self.range_size))
        ]

        for i in range(self.max_workers):
            range_for_worker = range_for_clients[i :: self.max_workers]
            if range_for_worker:
                yield range_for_worker

    async def _client_request(self, request_range):
        async with AsyncClient(**self.request_args, proxies=PROXY) as client:
            for index, start, end in request_range:
                r = await client.get(
                    self.url, headers={"Range": f"bytes={start}-{end - 1}"}
                )
                raise_for_status(r)
                yield index, r.content

    async def _streamer(self):
        try:
            async with self._sequential_queue:
                client_requests = [
                    self._client_request(r) for r in self._genarate_range()
                ]
                async with aiostream.stream.merge(
                    *client_requests
                ).stream() as streamer:
                    async for i, data in streamer:
                        await self._sequential_queue.put(i, data)
        except asyncio.CancelledError:
            pass

    async def read(self, n):
        if self._closed:
            raise ValueError("read of closed file")

        async for data in self._sequential_queue.items:
            self._buffer += data
            if len(self._buffer) >= n:
                break

        result = self._buffer[:n]
        del self._buffer[:n]
        return bytes(result)
