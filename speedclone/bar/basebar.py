import logging


class BaseBarManager:
    @classmethod
    def get_bar_manager(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def error(self, e):
        logging.error(getattr(e, "mes", str(e)), exc_info=True)

    def exists(self, e):
        logging.info(e.msg)
