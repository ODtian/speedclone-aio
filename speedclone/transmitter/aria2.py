import asyncio
import logging
import os

import aioaria2

from ..args import args_dict
from ..error import TaskError, TaskFailError, TaskNotDoneError
from ..manager import on_close_callbacks
from ..utils import format_path, parse_headers

ARIA2_POLLING_INTERVAL = args_dict["ARIA2_POLLING_INTERVAL"]


class Aria2Task:
    def __init__(self, total_path, file, client):
        self.total_path = total_path
        self.file = file
        self.bar = None
        self.client = client

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    async def run(self):
        if not hasattr(self.file, "get_download_info"):
            raise TaskFailError(
                path=self.total_path,
                task=self,
                error_msg="File can't download by aria2",
                task_exit=True,
                traceback=False,
            )

        # pprint(await client.getVersion())
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
                        task=self,
                        error_msg=f"aria2 error code={status['errorCode']} {status['errorMessage']}",
                    )

                elif status["status"] == "removed":
                    raise TaskFailError(
                        path=self.total_path,
                        task=self,
                        error_msg="aria2 download removed",
                        task_exit=True,
                        traceback=False,
                    )

                elif status["status"] == "complete":
                    self.bar.update(self.bar.bytes_total - self.bar.bytes_counted)
                    break

                await asyncio.sleep(ARIA2_POLLING_INTERVAL)

        except TaskError as e:
            raise e

        except Exception as e:
            raise TaskFailError(
                path=self.total_path, task=self, error_msg=type(e).__name__
            )

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path, task=self)


class Aria2Tasks:
    def __init__(self, target_path, url, token):
        self.target_path = target_path
        self.client = aioaria2.Aria2HttpClient(url, token=token)
        on_close_callbacks.append(self.client.close())

    @classmethod
    def get_trans(cls, path, config):
        return cls(target_path=path, url=config["url"], token=config["token"])

    async def get_task(self, file):
        total_path = format_path(self.target_path, file.get_relative_path())
        return Aria2Task(total_path, file, self.client)
