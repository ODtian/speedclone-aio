# from concurrent.futures import CancelledError, ThreadPoolExecutor
# from queue import Empty, Queue
# from threading import Thread
import asyncio
import time

import aiotimeout

from .error import TaskExistError, TaskFailError, TaskSleepError

# from .utils import console_write


class TransferManager:
    def __init__(self, download_manager, upload_manager, bar_manager, sleep_time):
        self.download_manager = download_manager
        self.upload_manager = upload_manager
        self.bar_manager = bar_manager

        self.sleep_time = sleep_time
        self.pusher_finished = False

        self.task_queue = asyncio.Queue()
        self.now_task = 0

    async def put_task(self, task):
        self.now_task += 1
        _worker = await self.upload_manager.get_worker(task)
        bar = self.bar_manager.get_bar(task)

        async def worker():
            return await _worker(bar)

        await self.task_queue.put(worker)

    def task_done(self):
        self.now_task -= 1

    async def handle_sleep(self, e):
        await self.put_task(e.task)
        time.sleep(e.sleep_time)
        self.bar_manager.sleep(e)

    async def handle_error(self, e):
        await self.put_task(e.task)
        self.bar_manager.error(e)

    async def handle_exists(self, e):
        await self.bar_manager.exists(e)

    async def handle_fail(self, e):
        await self.put_task(e.task)
        self.bar_manager.fail(e)

    async def task_pusher(self):
        async for task in self.download_manager.iter_tasks():
            await self.put_task(task)
        self.pusher_finished = True

    def finished(self):
        return self.now_task == 0 and self.pusher_finished

    async def get_task(self):
        try:
            with aiotimeout.timeout(self.sleep_time):
                task = await self.task_queue.get()
        except Exception:
            return
        else:
            return task

    async def excutor(self):
        while True:
            if self.finished():
                break
            else:
                worker = await self.get_task()
                if not worker:
                    continue

                try:
                    await worker()
                except TaskSleepError as e:
                    await self.handle_sleep(e)
                except TaskExistError as e:
                    await self.handle_exists(e)
                except TaskFailError as e:
                    await self.handle_fail(e)
                except Exception as e:
                    await self.handle_error(e)
                finally:
                    self.task_done()
                    await asyncio.sleep(self.sleep_time)

    def run(self, max_workers=10):
        event = asyncio.gather(
            self.task_pusher(), *[self.excutor() for _ in range(max_workers)]
        )
        loop = asyncio.get_event_loop()
        loop.run_until_complete(event)
