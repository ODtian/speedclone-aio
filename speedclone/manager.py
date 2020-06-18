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
    def __init__(self, download_manager, upload_manager, bar_manager, sleep_time):
        self.download_manager = download_manager
        self.upload_manager = upload_manager
        self.bar_manager = bar_manager

        self.sleep_time = sleep_time
        self.pusher_finished = False

        # self.task_queue = asyncio.Queue()
        self.task_queue = Queue()
        self.now_task = 0
        self.loop_thread = None

    async def put_task(self, task):
        self.now_task += 1
        self.task_queue.put(task)

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
        self.bar_manager.exists(e)

    async def handle_fail(self, e):
        await self.put_task(e.task)
        self.bar_manager.fail(e)

    async def task_pusher(self):
        async for task in self.download_manager.iter_tasks():
            await self.put_task(task)
        self.pusher_finished = True

    def get_task(self):
        try:
            task = self.task_queue.get(timeout=self.sleep_time)
        except Empty:
            return
        else:
            return task

    def finished(self):
        return self.now_task == 0 and self.pusher_finished

    async def excutor(self, task):
        worker = await self.upload_manager.get_worker(task)
        bar = self.bar_manager.get_bar(task)

        try:
            await worker(bar)
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

    def start_loop(self, loop):
        def loop_runner():
            loop.run_forever()

        self.loop_thread = Thread(target=loop_runner)
        self.loop_thread.start()

    def add_to_loop(excutor, loop):
        asyncio.run_coroutine_threadsafe(excutor, loop)

    def run_loop(self, loop):
        while True:
            if self.finished():
                break
            else:
                task = self.get_task()
                if not task:
                    continue
                self.add_to_loop(self.excutor(task), loop)
            time.sleep(self.sleep_time)

    def run(self, max_workers=10):
        loop = asyncio.get_event_loop()
        try:
            self.start_loop(loop)
            self.add_to_loop(self.task_pusher(), loop)
            self.run_loop(loop)
        except KeyboardInterrupt:
            console_write("exists", "Stopping loop.")
        finally:
            loop.stop()
