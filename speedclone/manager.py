import asyncio
import logging
import time
from threading import Thread

from .error import TaskError, TaskExistError

on_close_callbacks = []


def init_uv():
    try:
        import uvloop
    except ImportError:
        logging.warning(
            "Uvloop is not installed, which can bring performance improvement. Install by 'pip install uvloop'."
        )
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class TransferManager:
    def __init__(self, trans, bar_manager, interval, max_workers):
        self.source_trans, self.target_trans = trans
        self.bar_manager = bar_manager

        self.interval = interval
        self.max_workers = max_workers

        self.sem = asyncio.Semaphore(self.max_workers)
        self.loop = asyncio.get_event_loop()

        self.task_pusher_finished = False
        self.task_queue = asyncio.Queue()
        self.now_task = 0
        self._finished = False

    async def handle_task_fail(self, e):
        if isinstance(e, TaskExistError):
            e.task.bar.update(e.task.bar.bytes_total)
        else:
            e.task.bar.update(-e.task.bar.bytes_counted)

        logging.log(e.level, e.msg, exc_info=e.traceback)

        if not e.task_exit:
            await self.put_task(e.task)

    async def put_task(self, task):
        self.now_task += 1
        await self.task_queue.put(task)

    def task_done(self):
        self.now_task -= 1

    def all_tasks_finished(self):
        return (self.now_task == 0 and self.task_pusher_finished) or self._finished

    async def task_pusher(self):
        try:
            async for file in self.source_trans.iter_file():
                task = await self.target_trans.get_task(file)
                await self.put_task(task)
        except Exception as e:
            self._finished = True
            raise e
        finally:
            self.task_pusher_finished = True

    async def get_task(self):
        try:
            task = self.task_queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        else:
            bar = self.bar_manager.get_bar()
            task.set_bar(bar)
            return task

    async def worker(self, task):
        async with self.sem:
            try:
                await task.run()
            except TaskError as e:
                await self.handle_task_fail(e)
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
            time.sleep(self.interval)
            if self.all_tasks_finished():
                break
            else:
                task = self.add_to_loop(self.get_task()).result()
                if not task:
                    continue

                worker = self.add_to_loop(self.worker(task))
                worker.add_done_callback(lambda w: w.result())

    def stop_loop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)

    def run(self):
        try:
            self.start_loop()
            pusher = self.add_to_loop(self.task_pusher())
            pusher.add_done_callback(lambda p: p.result())
            self.run_loop()
        finally:
            for callback in on_close_callbacks:
                self.add_to_loop(callback)
            self.stop_loop()
