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

        # self.pusher_thread = None
        self.pusher_finished = False

        self.task_queue = asyncio.Queue()
        # self.taskdone_queue = Queue()
        self.now_task = 0
        # self.sleep_queue = asyncio.Queue()

        # self.futures = []

    async def put_task(self, task):
        self.now_task += 1
        await self.task_queue.put(task)

    def task_done(self):
        self.now_task -= 1

    async def handle_sleep(self, e):
        await self.put_task(e.task)
        time.sleep(e.sleep_time)
        # if not await self.sleep_queue.empty():
        #     await self.sleep_queue.put(e.sleep_time)
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

    # def run_task_pusher(self):
    #     def pusher():
    #         for task in self.download_manager.iter_tasks():
    #             if self.pusher_finished:
    #                 return
    #             else:
    #                 self.put_task(task)
    #         self.pusher_finished = True

    #     self.pusher_thread = Thread(target=pusher)
    #     self.pusher_thread.start()

    def finished(self):
        return self.now_task == 0 and self.pusher_finished

    # def if_sleep(self):
    #     return not self.sleep_queue.empty()

    # def sleep(self):
    #     if not await self.sleep_queue.empty():
    #         sleep_time = await self.sleep_queue.get()
    #         time.sleep(sleep_time)
    # def done_callback(self, task):
    #     try:
    #         task.result()
    #     except CancelledError:
    #         pass
    #     except TaskExistError as e:
    #         self.handle_exists(e)
    #     except TaskSleepError as e:
    #         self.handle_sleep(e)
    #     except TaskFailError as e:
    #         self.handle_fail(e)
    #     except Exception as e:
    #         self.handle_error(e)
    #     finally:
    #         self.task_done()

    # def clear_all_futueres(self):
    #     [f.cancel() for f in self.futures]

    async def get_task(self):
        try:
            async with aiotimeout.timeout(self.sleep_time):
                task = await self.task_queue.get()
        except Exception:
            return
        else:
            return task

    # def get_worker(self, task):
    #     _worker = self.upload_manager.get_worker(task)
    #     bar = self.bar_manager.get_bar(task)

    #     def worker():
    #         self.sleep_queue.join()
    #         return _worker(bar)

    #     return worker

    async def worker(self,):
        while True:
            if self.finished():
                break
            else:
                task = await self.get_task()
                if not task:
                    continue
                worker = await self.upload_manager.get_worker(task)
                bar = await self.bar_manager.get_bar(task)

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
                    await asyncio.sleep(self.sleep_time)

    # def add_to_excutor(self, executor):
    #     while True:
    #         if self.finished():
    #             break
    #         elif self.if_sleep():
    #             self.sleep()
    #         else:
    #             task = self.get_task()
    #             if not task:
    #                 continue
    #             worker = self.get_worker(task)
    #             future = executor.submit(worker)
    #             future.add_done_callback(self.done_callback)
    #             self.futures.append(future)
    #         time.sleep(self.sleep_time)

    def run(self, max_workers=10):
        event = asyncio.gather(
            self.task_pusher(), *[self.worker() for _ in range(max_workers)]
        )
        loop = asyncio.get_event_loop()
        loop.run_until_complete(event)
        # self.run_task_pusher()
        # executor = ThreadPoolExecutor(max_workers=max_workers)
        # try:
        #     self.add_to_excutor(executor)
        # except KeyboardInterrupt:
        #     if not self.pusher_finished:
        #         console_write("error", "Stopping pusher thread.")
        #         self.pusher_finished = True
        #         console_write("error", "Waitting pusher thread.")
        #         self.pusher_thread.join()

        #     console_write("error", "Waitting worker threads.")
        #     self.clear_all_futueres()
        #     executor.shutdown()

        #     console_write("error", "Clearing queues.")
        #     self.sleep_queue.queue.clear()
        #     self.task_queue.queue.clear()
        #     self.taskdone_queue.queue.clear()

        #     console_write("error", "Closing bars.")
        #     self.bar_manager.exit()
