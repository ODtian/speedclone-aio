# import logging


class BaseBarManager:
    @classmethod
    def get_bar_manager(cls):
        return cls()

    # def task_fail(self, e):

    #     e.task.bar.update(-e.task.bar.bytes_counted)
    #     logging.log(e.level, e.msg, exc_info=e.traceback)

    # def file_exist(self, e):
    #     e.task.bar.update(e.task.bar.bytes_total)
    #     logging.info(e.level, e.msg)
