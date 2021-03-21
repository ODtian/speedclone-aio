import os

import aiofiles

from ..args import args_dict
from ..error import TaskError, TaskExistError, TaskFailError, TaskNotDoneError
from ..utils import aiter_data, format_path, iter_path

CHUNK_SIZE = args_dict["CHUNK_SIZE"]
STEP_SIZE = args_dict["STEP_SIZE"]


class LocalFile:
    def __init__(self, abs_path, relative_path):
        self.abs_path = abs_path
        self.relative_path = relative_path
        self.size = os.path.getsize(self.abs_path)

    def get_relative_path(self):
        return self.relative_path

    def get_size(self):
        return self.size

    async def iter_chunk(self, chunk_size, offset=0):
        async with aiofiles.open(self.abs_path, "rb") as f:
            f.seek(offset * chunk_size)
            while True:
                chunk = await f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    return

    async def read(self, length, offset=0):
        async with aiofiles.open(self.abs_path, "rb") as f:
            f.seek(offset)
            return await f.read(length)


class LocalFileTask:
    def __init__(self, total_path, file):
        self.total_path = total_path
        self.file = file
        self.bar = None

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    def make_dir(self):
        base_dir = os.path.dirname(self.total_path)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    async def run(self):
        try:
            if os.path.exists(self.total_path):
                raise TaskExistError(path=self.total_path, task=self)

            self.make_dir()

            async with aiofiles.open(self.total_path, "wb") as f:
                async for chunk in self.file.iter_chunk(chunk_size=CHUNK_SIZE):
                    async for step in aiter_data(chunk, STEP_SIZE, self.bar):
                        await f.write(step)

        except TaskError as e:
            raise e

        except Exception as e:
            raise TaskFailError(
                path=self.total_path, task=self, error_msg=type(e).__name__
            )

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path, task=self)


class LocalFiles:
    def __init__(self, source_path):
        self.source_path = source_path

    @classmethod
    def get_trans(cls, path, config):
        return cls(source_path=path)

    async def iter_file(self):
        base_path, _ = os.path.split(self.source_path)
        for local_path in iter_path(self.source_path):
            relative_path = local_path[len(base_path) :]
            yield LocalFile(local_path, relative_path)


class LocalFileTasks:
    def __init__(self, target_path):
        self.target_path = target_path

    @classmethod
    def get_trans(cls, path, config):
        return cls(target_path=path)

    async def get_task(self, file):
        total_path = format_path(self.target_path, file.get_relative_path())
        return LocalFileTask(total_path, file)
