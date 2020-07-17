import asyncio
import time
import logging

# from queue import Empty, Queue
from threading import Thread

from .error import TaskExistError, TaskFailError
from .utils import aiter_with_end

try:
    import uvloop
except ImportError:
    logging.warning(
        "Uvloop is not installed, which can bring performance improvement. Install by 'pip install uvloop'."
    )
    # console_write(
    #     "sleep",
    #     "[info] Module uvloop is not installed, use default loop instead. Using uvloop can bring significant performance improvement, install it by 'pip install uvloop'.",
    # )
else:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class TransferManager:
    def __init__(
        self, download_manager, upload_manager, bar_manager, sleep_time, max_workers
    ):
        self.download_manager = download_manager
        self.upload_manager = upload_manager
        self.bar_manager = bar_manager

        self.sleep_time = sleep_time
        self.max_workers = max_workers

        self.sem = asyncio.Semaphore(self.max_workers)
        self.loop = asyncio.get_event_loop()

        self.pusher_finished = False
        self.task_queue = asyncio.Queue()
        self.now_task = 0

        # TransferManager.me = self

    # async def handle_sleep(self, e):
    #     await self.put_task(e.task)
    #     time.sleep(e.sleep_time)
    #     self.bar_manager.sleep(e)

    async def handle_error(self, e, task=None):
        await self.put_task(task or e.task)
        self.bar_manager.error(e)

    def handle_exists(self, e):
        self.bar_manager.exists(e)

    async def put_task(self, task):
        self.now_task += 1
        await self.task_queue.put(task)

    def task_done(self):
        self.now_task -= 1

    def finished(self):
        return self.now_task == 0 and self.pusher_finished

    async def task_pusher(self):
        async for task, is_end in aiter_with_end(self.download_manager.iter_tasks()):
            if is_end:
                task.end = True
            await self.put_task(task)
        self.pusher_finished = True

    async def get_worker(self):
        try:
            task = self.task_queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        else:
            _worker = await self.upload_manager.get_worker(task)
            bar = self.bar_manager.get_bar(task)

            async def worker():
                return await _worker(bar)

            worker.task = task
            return worker

    async def excutor(self, worker):
        async with self.sem:
            try:
                await worker()
            # except TaskSleepError as e:
            #     await self.handle_sleep(e)
            except TaskExistError as e:
                self.handle_exists(e)
            except TaskFailError as e:
                await self.handle_fail(e)
            except Exception as e:
                await self.handle_error(e, worker.task)
            finally:
                self.task_done()

    def start_loop(self):
        def loop_runner():
            self.loop.run_forever()

        loop_thread = Thread(target=loop_runner)
        loop_thread.start()

    def add_to_loop(self, excutor):
        return asyncio.run_coroutine_threadsafe(excutor, self.loop)

    def run_loop(self):
        while True:
            time.sleep(self.sleep_time)
            if self.finished():
                break
            else:
                worker = self.add_to_loop(self.get_worker()).result()
                if not worker:
                    continue
                self.add_to_loop(self.excutor(worker))

    def stop_loop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)

    def run(self):
        try:
            self.start_loop()
            self.add_to_loop(self.task_pusher())
            self.run_loop()
        finally:
            self.stop_loop()
