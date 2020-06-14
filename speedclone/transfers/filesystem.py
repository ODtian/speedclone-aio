from ..error import TaskExistError, TaskFailError
from ..utils import norm_path, iter_path
import os
import aiofiles


class FileSystemTransferDownloadTask:
    def __init__(self, file_path, relative_path):
        self.file_path = file_path
        self.relative_path = relative_path

    async def iter_data(self, chunk_size=(10 * 1024 ** 2)):
        async with aiofiles.open(self.file_path, "rb") as f:
            while True:
                chunk = await f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    return

    async def get_total(self):
        return os.path.getsize(self.file_path)

    def get_relative_path(self):
        return self.relative_path


class FileSystemTransferUploadTask:
    chunk_size = 10 * 1024 ** 2
    step_size = 1024 ** 2

    def __init__(self, task, bar):
        self.task = task
        self.bar = bar

    async def run(self, total_path):
        if os.path.exists(total_path):
            raise TaskExistError(task=self.task)
        try:
            base_dir = os.path.dirname(total_path)
            if not os.path.exists(base_dir):
                os.makedirs(base_dir)

            self.bar.init_bar(self.task.get_total(), self.task.get_relative_path())

            async with open(total_path, "wb") as f:
                async for data in self.task.iter_data(chunk_size=self.chunk_size):
                    while data:
                        step = data[: self.step_size]
                        await f.write(step)
                        self.bar.update(len(data))
                        data = data[self.step_size :]

        except Exception as e:
            raise TaskFailError(task=self.task, msg=str(e), code=type(e))
        finally:
            self.bar.close()


class FileSystemTransferManager:
    def __init__(self, path):
        self.path = path

    def _iter_localpaths(self):
        base_path, _ = os.path.split(self.path)
        for p in iter_path(self.path):
            relative_path = p[len(base_path) :]
            yield p, relative_path

    @classmethod
    def get_transfer(cls, conf, path, args):
        FileSystemTransferUploadTask.chunk_size = args.chunk_size
        FileSystemTransferUploadTask.step_size = args.step_size
        return cls(path=path)

    async def iter_tasks(self):
        for l, r in self._iter_localpaths():
            yield FileSystemTransferDownloadTask(l, r)

    async def get_worker(self, task):
        total_path = norm_path(self.path, task.get_relative_path())

        async def worker(bar):
            w = FileSystemTransferUploadTask(task, bar)
            await w.run(total_path=total_path)

        return worker
