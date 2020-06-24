import asyncio
import time
from queue import Empty, Queue
from threading import Thread

from .error import TaskExistError, TaskFailError, TaskSleepError
from .utils import console_write

try:
    import uvloop
except ImportError:
    console_write(
        "sleep",
        "[info] Module uvloop is not installed, use default loop instead. Using uvloop can bring significant performance improvement, install it by 'pip install uvloop'.",
    )
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

        self.loop_thread = None
        self.sem = asyncio.Semaphore(self.max_workers)

        self.pusher_finished = False
        self.task_queue = Queue()
        self.now_task = 0

    def handle_sleep(self, e):
        self.put_task(e.task)
        time.sleep(e.sleep_time)
        self.bar_manager.sleep(e)

    def handle_error(self, e, task):
        self.put_task(task)
        self.bar_manager.error(e)

    def handle_exists(self, e):
        self.bar_manager.exists(e)

    def handle_fail(self, e):
        self.put_task(e.task)
        self.bar_manager.fail(e)

    def put_task(self, task):
        self.now_task += 1
        self.task_queue.put(task)

    def task_done(self):
        self.now_task -= 1

    def finished(self):
        return self.now_task == 0 and self.pusher_finished

    async def task_pusher(self):
        async for task in self.download_manager.iter_tasks():
            self.put_task(task)
        self.pusher_finished = True

    async def get_task(self):
        try:
            task = self.task_queue.get(timeout=self.sleep_time)
        except Empty:
            return
        else:
            _worker = await self.upload_manager.get_worker(task)
            bar = self.bar_manager.get_bar(task)

            async def worker():
                return await _worker(bar)

            return worker

    async def excutor(self, task):
        async with self.sem:
            try:
                await task()
            except TaskSleepError as e:
                self.handle_sleep(e)
            except TaskExistError as e:
                self.handle_exists(e)
            except TaskFailError as e:
                self.handle_fail(e)
            except Exception as e:
                self.handle_error(e, task)
            finally:
                self.task_done()

    def start_loop(self, loop):
        def loop_runner():
            loop.run_forever()

        self.loop_thread = Thread(target=loop_runner)
        self.loop_thread.start()

    def add_to_loop(self, excutor, loop):
        asyncio.run_coroutine_threadsafe(excutor, loop)

    async def run_loop(self, loop):
        while True:
            if self.finished():
                break
            else:
                task = await self.get_task()
                if not task:
                    continue
                self.add_to_loop(self.excutor(task), loop)
            time.sleep(self.sleep_time)

    def run(self):
        loop = asyncio.get_event_loop()
        try:
            self.start_loop(loop)
            self.add_to_loop(self.task_pusher(), loop)
            asyncio.new_event_loop().run_until_complete(self.run_loop(loop))
        except KeyboardInterrupt:
            console_write("exists", "Stopping loop.")
        finally:
            loop.call_soon_threadsafe(loop.stop)
