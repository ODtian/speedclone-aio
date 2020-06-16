import os
from urllib.parse import unquote

import aiofiles
import requests

from .. import ahttpx


class HttpTransferDownloadTask:
    http = {}

    def __init__(self, url, relative_path):
        self.url = url
        self.relative_path = relative_path
        self._r = None

    async def iter_data(self, chunk_size=(10 * 1024 ** 2)):
        self.r.raise_for_status()
        async for chunk in self.r.aiter_bytes(chunk_size=chunk_size):
            yield chunk

    def get_relative_path(self):
        return self.relative_path

    def get_total(self):
        try:
            if self.r.status_code == requests.codes.ok:
                return int(self.r.headers.get("Content-Length", 0))
        except Exception:
            return 0

    @property
    async def r(self):
        if not self._r:
            self._r = await ahttpx.get(self.url, stream=True, **self.http)
        return self._r


class HttpTransferManager:
    def __init__(self, path):
        self.path = path

    async def _iter_urls(self):
        if self.path.startswith("http://") or self.path.startswith("https://"):
            yield self.path, unquote(os.path.basename(self.path))
        elif os.path.exists(self.path) and os.path.isfile(self.path):
            async with aiofiles.open(self.path, "r") as f:
                while True:
                    url = await f.readline().rstrip("\n")
                    if url:
                        resource_name = unquote(os.path.basename(url))
                        yield url, resource_name
                    else:
                        return
        else:
            raise Exception("Source not illegal")

    @classmethod
    def get_transfer(cls, conf, path, args):
        HttpTransferDownloadTask.chunk_size = args.chunk_size
        HttpTransferDownloadTask.http = conf.get("http", {})
        return cls(path=path)

    async def iter_tasks(self):
        async for url, name in self._iter_urls():
            yield HttpTransferDownloadTask(url, name)

    async def get_worker(self, task):
        pass
