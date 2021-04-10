import asyncio
import logging
import time

from .args import Args
from .error import FileListError, TaskError, TaskExistError
from .log import console

on_close_callbacks = []


def init_uvloop():
    try:
        import uvloop
    except ImportError:
        logging.warning(
            "Uvloop is not installed, which can bring performance improvement. Install by 'pip install uvloop'."
        )
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class TransferManager:
    def __init__(self, files, tasks, bar_manager):
        self._files = files
        self._tasks = tasks
        self._bar_manager = bar_manager

        self._result = asyncio.get_event_loop().create_future()

        self._task_queue = None
        self._interval = None
        self._max_results = None

        self.failed = []

    async def _feed_tasks(self):
        try:
            async for f in self._files.list_file():
                task = await self._tasks.get_task(f)
                await self._task_queue.put(task)

        except FileListError as e:
            logging.log(e.level, e.msg, exc_info=e.traceback)
            if e.extra_msg:
                console.log(e.extra_msg)

        finally:
            await self._task_queue.put(None)

    async def _apply_tasks(self):
        while True:
            task = await self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                await self._task_queue.join()

                self._result.set_result(None)
                break
            else:
                asyncio.create_task(self._worker(task))

    async def _worker(self, task):
        try:
            bar = self._bar_manager.get_bar()
            task.set_bar(bar)

            for i in range(self._max_retries):
                try:
                    await task.run()

                except TaskError as e:
                    logging.log(e.level, e.msg, exc_info=e.traceback)
                    if e.extra_msg:
                        console.log(e.extra_msg)

                    if isinstance(e, TaskExistError):
                        task.bar.complete()
                        break
                    else:
                        task.bar.reset()
                else:
                    break

                if i == (self._max_retries - 1):
                    self.failed.append(task.get_relative_path())

                await asyncio.sleep(self._interval)

        except Exception as e:
            self._result.set_exception(e)

        finally:
            self._task_queue.task_done()

    async def run(
        self,
        interval=0.05,
        max_workers=5,
        max_retries=3,
        chunk_size=20 * (1024 ** 2),
        step_size=1024 ** 2,
        buffer_size=20 * (1024 ** 2),
        download_chunk_size=1024 ** 2,
        max_download_workers=3,
        proxy=None,
        client_sleep_time=10,
        max_clients=10,
        max_page_size=100,
        aria2_polling_interval=1,
        failed_task_save_path="failed-{ts}.txt",
    ):
        for k, v in locals().items():
            setattr(Args, k.upper(), v)

        self._task_queue = asyncio.Queue(maxsize=max_workers)
        self._interval = interval
        self._max_retries = max_retries

        try:
            await asyncio.gather(self._feed_tasks(), self._apply_tasks(), self._result)

        finally:
            for callback in on_close_callbacks:
                await callback()

            if failed_task_save_path is not None and len(self.failed) > 0:
                with open(failed_task_save_path.format(ts=int(time.time())), "w") as f:
                    f.writelines(self.failed)
