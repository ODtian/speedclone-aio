import asyncio
import os

import aioaria2

from ..args import Args
from ..error import TaskError, TaskFailError, TaskNotDoneError
from ..manager import on_close_callbacks
from ..utils import format_path, parse_headers


class Aria2Task:
    def __init__(self, total_path, file, client):
        self.total_path = total_path
        self.file = file
        self.bar = None
        self.client = client

    def set_bar(self, bar):
        self.bar = bar
        self.bar.set_info(total=self.file.get_size(), content=self.total_path)

    async def run(self):
        if not hasattr(self.file, "get_download_info"):
            raise TaskFailError(
                path=self.total_path,
                error_msg="File can't download by aria2",
                task_exit=True,
                traceback=False,
            )

        downloaded_length = 0
        try:
            url, headers = await self.file.get_download_info()
            base_path, _ = os.path.split(self.total_path)
            gid = await self.client.addUri(
                [url],
                options={
                    "dir": base_path,
                    "header": parse_headers(headers).split("\n"),
                },
            )

            while True:
                status = await self.client.tellStatus(
                    gid, keys=["status", "completedLength", "errorCode", "errorMessage"]
                )

                if status["status"] == "active":
                    completed_length = int(status["completedLength"])
                    self.bar.update(completed_length - downloaded_length)
                    downloaded_length = completed_length

                elif status["status"] == "error":
                    raise TaskFailError(
                        path=self.total_path,
                        error_msg=f"aria2 error code={status['errorCode']} {status['errorMessage']}",
                    )

                elif status["status"] == "removed":
                    raise TaskFailError(
                        path=self.total_path,
                        error_msg="aria2 download removed",
                        task_exit=True,
                        traceback=False,
                    )

                elif status["status"] == "complete":
                    self.bar.update(self.bar.bytes_total - self.bar.bytes_counted)
                    break

                await asyncio.sleep(Args.ARIA2_POLLING_INTERVAL)

        except TaskError as e:
            raise e

        except Exception as e:
            raise TaskFailError(path=self.total_path, error_msg=type(e).__name__)

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path)


class Aria2Tasks:
    def __init__(self, path, url, token):
        self._path = path
        self._client = aioaria2.Aria2HttpClient(url, token=token)
        on_close_callbacks.append(self._client.close())

    @classmethod
    def transport_factory(cls, path, url="http://127.0.0.1:6800/jsonrpc", token=""):
        return cls(path=path, url=url, token=token)

    async def get_task(self, file):
        total_path = format_path(self._path, file.get_relative_path())
        return Aria2Task(total_path, file, self._client)
