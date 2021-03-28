import logging

from .utils import parse_headers


class HttpStatusError(Exception):
    def __init__(self, response, status_code=None):
        self.response = response
        self.status_code = status_code or self.response.status_code

    def __str__(self):
        return f"\n(Failed response from {self.response.request.url})\n\n{self._build_raw_response()}"

    def _build_raw_response(self):
        return "\n\n".join(
            (
                f"{self.response.request.method} {self.response.status_code}\n"
                + parse_headers(self.response.headers, join=": "),
                self.response.text,
            )
        )


class TaskError(Exception):
    level = logging.DEBUG
    task_exit = False
    traceback = False


class TaskNotDoneError(TaskError):
    level = logging.ERROR

    def __init__(self, path, task):
        self.msg = f"Task at '{path}' not uploaded completely, will try again."
        self.task = task


class TaskFailError(TaskError):
    level = logging.ERROR

    def __init__(self, path, task, error_msg, task_exit=False, traceback=True):
        self.msg = f"Task at '{path}' failed: {error_msg}"
        self.task = task
        self.task_exit = task_exit
        self.traceback = traceback


class TaskExistError(TaskError):
    level = logging.INFO
    task_exit = True

    def __init__(self, path, task):
        self.msg = f"Task at '{path}' already exists."
        self.task = task
