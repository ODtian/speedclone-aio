import asyncio

import aiofiles
from httpx import AsyncClient

from .args import Args
from .utils import raise_for_status


class StreamTransport:
    def __init__(self, loop=asyncio.get_event_loop(), limit=(0, 0)):
        self._loop = loop

        self._read_waiter = self._loop.create_future()
        self._write_waiter = self._loop.create_future()
        self._write_waiter.set_result(None)

        self._finished = False
        self._buffer = bytearray()
        self._low_level, self._high_level = limit
        if self._low_level > self._high_level:
            raise ValueError("low level must be lower than high level")

        self._exception = None

    def _pause_read(self):
        if self._read_waiter.done():
            self._read_waiter = self._loop.create_future()

    def _resume_read(self):
        if not self._read_waiter.done():
            self._read_waiter.set_result(None)

    def _maybe_resume_write(self):
        if len(self._buffer) <= self._low_level and not self._write_waiter.done():
            self._write_waiter.set_result(None)

    def _maybe_pause_write(self):
        if len(self._buffer) > self._high_level and self._write_waiter.done():
            self._write_waiter = self._loop.create_future()

    def close(self, exc=None):
        self._finished = True
        self._resume_read()
        if exc is not None:
            self._exception = exc

    async def write(self, data):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ValueError(
                f"data must be bytes, bytearray or memoryview and not {type(data)}"
            )

        await self._write_waiter
        self._buffer.extend(data)
        self._maybe_pause_write()
        self._resume_read()

    async def read(self, n):
        if n < 0:
            raise ValueError("size can not be less than zero")

        if n == 0:
            return b""

        while len(self._buffer) < n and self._write_waiter.done():
            if self._finished:
                break

            self._pause_read()
            await self._read_waiter

        if self._exception is not None:
            raise self._exception

        if len(self._buffer) == n:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]

        self._maybe_resume_write()
        return data

    async def readall(self):
        return await self.read(len(self))

    def __len__(self):
        return len(self._buffer)


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

        self._loop = asyncio.get_event_loop()

        self._transport = StreamTransport(
            loop=self._loop,
            limit=(int(Args.BUFFER_SIZE / 2), Args.BUFFER_SIZE),
        )

        self._streamer_future = None
        self._chunk_finished = {}
        self._closed = False

    async def __aenter__(self):
        self._streamer_future = asyncio.create_task(self._streamer())
        return self

    async def __aexit__(self, *args, **kwargs):
        self._streamer_future.cancel()
        self._closed = True

    def _genarate_range(self):
        range_for_clients = []

        for index, left in enumerate(range(self.start, self.end, self.range_size)):
            range_for_clients.append(
                (index, left, min(left + self.range_size, self.end))
            )
            self._chunk_finished[index] = self._loop.create_future()

        self._chunk_finished[-1] = self._loop.create_future()
        self._chunk_finished[-1].set_result(None)

        for i in range(self.max_workers):
            range_for_worker = range_for_clients[i :: self.max_workers]
            if range_for_worker:
                yield range_for_worker

    async def _client_request(self, request_range):
        async with AsyncClient(
            **self.request_args, proxies=Args.PROXY, timeout=60
        ) as client:
            for index, start, end in request_range:
                async with client.stream(
                    "GET", self.url, headers={"Range": f"bytes={start}-{end - 1}"}
                ) as r:
                    raise_for_status(r)

                    chunk_finished = self._chunk_finished[index - 1]
                    buffer = bytearray()

                    async for data in r.aiter_bytes():
                        buffer.extend(data)
                        if chunk_finished.done():
                            await self._transport.write(buffer)
                            buffer.clear()

                    if len(buffer) != 0:
                        await chunk_finished
                        await self._transport.write(buffer)
                        buffer.clear()

                    self._chunk_finished[index].set_result(None)

    async def _streamer(self):
        try:
            await asyncio.gather(
                *[self._client_request(r) for r in self._genarate_range()]
            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._transport.close(exc=e)
        else:
            self._transport.close()

    async def read(self, n):
        if self._closed:
            raise ValueError("Cannot read a closed file")
        return await self._transport.read(n)
