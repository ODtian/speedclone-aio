import os

import aiofiles

from ..args import args_dict
from ..error import TaskError, TaskExistError, TaskFailError, TaskNotDoneError
from ..utils import format_path, iter_path
from ..filereader import LocalFileReader

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

    async def get_reader(self, start=0, end=None):
        return LocalFileReader(self.abs_path, (start, end))


class LocalFileTask:
    def __init__(self, total_path, file):
        self.total_path = total_path
        self.file = file
        self.bar = None

        self._seek = 0

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    def _make_dir(self):
        base_dir = os.path.dirname(self.total_path)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    async def run(self):
        try:
            if os.path.exists(self.total_path):
                size = os.path.getsize(self.total_path)
                if size < self.file.get_size():
                    self._seek = size
                else:
                    raise TaskExistError(path=self.total_path)

            self._make_dir()

            async with aiofiles.open(self.total_path, "ab") as f:
                async with (await self.file.get_reader(start=self._seek)) as reader:
                    while True:
                        data = await reader.read(STEP_SIZE)
                        if not data:
                            break
                        await f.write(data)
                        self.bar.update(len(data))

        except TaskError as e:
            raise e

        except Exception as e:
            raise TaskFailError(path=self.total_path, error_msg=type(e).__name__)

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path)


class LocalFiles:
    def __init__(self, path):
        self._path = path

    @classmethod
    def transport_factory(cls, path):
        return cls(path=path)

    async def iter_file(self):
        base_path, _ = os.path.split(self._path)
        for local_path in iter_path(self._path):
            relative_path = local_path[len(base_path) :]
            yield LocalFile(local_path, relative_path)


class LocalFileTasks:
    def __init__(self, path):
        self._path = path

    @classmethod
    def transport_factory(cls, path, config):
        return cls(path=path)

    async def get_task(self, file):
        total_path = format_path(self._path, file.get_relative_path())
        return LocalFileTask(total_path, file)
