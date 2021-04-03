import asyncio
import logging

from .args import args_dict
from .error import TaskError, TaskExistError

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
    def __init__(self, source, target, bar_manager):
        self._source = source
        self._target = target
        self._bar_manager = bar_manager

        self._loop = asyncio.get_event_loop()

        self._task_queue = asyncio.Queue()
        self._result = self._loop.create_future()

        self._lock = None
        self._interval = None
        self._max_results = None

    async def _feed_tasks(self):
        async for f in self._source.iter_file():
            task = await self._target.get_task(f)
            await self._task_queue.put(task)
        await self._task_queue.put(None)

    async def _apply_tasks(self):
        while True:
            task = await self._task_queue.get()
            if task is None:
                self._task_queue.join()
                break
            else:
                self._add_to_loop(self._worker(task))

            await asyncio.sleep(self._interval)

    async def _worker(self, task):
        async with self._lock:
            bar = self._bar_manager.get_bar()
            task.set_bar(bar)

            for i in range(self._max_retries):
                try:
                    await task.run()
                except TaskExistError as e:
                    task.bar.update(task.bar.bytes_total)
                    logging.log(e.level, e.msg, exc_info=e.traceback)
                except TaskError as e:
                    task.bar.update(-task.bar.bytes_counted)
                    logging.log(e.level, e.msg, exc_info=e.traceback)
                except Exception as e:
                    self._result.set_exception(e)
                    break
                else:
                    self._task_queue.task_done()
                    break

                if i == (self._max_retries - 1):
                    pass

    def _add_to_loop(self, excutor):
        return asyncio.run_coroutine_threadsafe(excutor, self._loop)

    async def run(
        self,
        interval=0.05,
        max_workers=5,
        max_retries=3,
        chunk_size=20 * (1024 ** 2),
        step_size=1024 ** 2,
        download_chunk_size=1024 ** 2,
        proxy=None,
        client_sleep_time=10,
        max_clients=10,
        max_page_size=100,
        max_download_workers=3,
        aria2_polling_interval=1,
    ):
        for k, v in locals().items():
            args_dict[k] = v

        self._lock = asyncio.Semaphore(max_workers)
        self._interval = interval
        self._max_retries = max_retries

        try:
            await asyncio.gather(self._feed_tasks(), self._apply_tasks(), self._result)
        finally:
            for callback in on_close_callbacks:
                await callback


# class TransferManager:
#     def __init__(self, source, target, bar_manager):
#         self.source = source
#         self.target = target
#         self.bar_manager = bar_manager

#         self.sem = asyncio.Semaphore(MAX_WORKERS)
#         self.loop = asyncio.get_event_loop()
#         self.task_queue = asyncio.Queue()

#         self._now_task = 0
#         self._task_pusher_finished = False
#         self._finished = False
#         self._retries_count = {}

#     async def put_task(self, task):
#         self._now_task += 1
#         await self.task_queue.put(task)

#     def task_done(self):
#         self._now_task -= 1

#     def all_tasks_finished(self):
#         return (self._now_task == 0 and self._task_pusher_finished) or self._finished

#     async def task_pusher(self):
#         try:
#             async for f in self.source.iter_file():
#                 self._retries_count[f.get_relative_path()] = MAX_RETRIES
#                 task = await self.target.get_task(f)
#                 await self.put_task(task)
#         except Exception:
#             self._finished = True
#             raise
#         finally:
#             self.task_pusher_finished = True

#     async def get_task(self):
#         try:
#             task = self.task_queue.get_nowait()
#         except asyncio.QueueEmpty:
#             return None
#         else:
#             bar = self.bar_manager.get_bar()
#             task.set_bar(bar)
#             return task

#     async def worker(self, task):
#         async with self.sem:
#             try:
#                 await task.run()
#             except TaskExistError:
#                 task.bar.update(task.bar.bytes_total)
#             except TaskError as e:
#                 task.bar.update(-task.bar.bytes_counted)

#                 logging.log(e.level, e.msg, exc_info=e.traceback)

#                 if not e.task_exit:
#                     path = task.file.get_relative_path()

#                     if self._retries_count[path] > 0:
#                         self._retries_count[path] -= 1
#                         await self.put_task(task)
#             finally:
#                 self.task_done()

#     def start_loop(self):
#         def loop_runner():
#             self.loop.run_forever()

#         loop_thread = Thread(target=loop_runner)
#         loop_thread.start()

#     def add_to_loop(self, excutor):
#         return asyncio.run_coroutine_threadsafe(excutor, self.loop)

#     def run_loop(self):
#         while True:
#             time.sleep(INTERVAL)
#             if self.all_tasks_finished():
#                 break
#             else:
#                 task = self.add_to_loop(self.get_task()).result()
#                 if not task:
#                     continue

#                 worker = self.add_to_loop(self.worker(task))
#                 worker.add_done_callback(lambda w: w.result())

#     def stop_loop(self):
#         self.loop.call_soon_threadsafe(self.loop.stop)

#     def run(self):
#         try:
#             self.start_loop()
#             pusher = self.add_to_loop(self.task_pusher())
#             pusher.add_done_callback(lambda p: p.result())
#             self.run_loop()
#         finally:
#             for callback in on_close_callbacks:
#                 self.add_to_loop(callback)
#             self.stop_loop()
