import logging

from rich.panel import Panel

from .utils import parse_headers


class HttpStatusError(Exception):
    def __init__(self, response, status_code=None):
        self.response = response
        self.status_code = status_code or self.response.status_code

    def build_raw_response(self):
        return Panel.fit(
            f"[bold]{self.response.request.method}[/] {self.response.status_code}\n"
            f"[underline]{self.response.request.url}[/]\n"
            f"{parse_headers(self.response.headers, join=': ')}\n\n"
            f"{self.response.text if self.response.is_stream_consumed else '<Stream content>'}\n",
            title="Bad response",
        )


class CloneError(Exception):
    level = logging.ERROR
    msg = ""
    extra_msg = None

    traceback = False


class FileListError(CloneError):
    t_msg = "'{path}' listing failed: {error_msg}"

    def __init__(self, path, error_msg, extra_msg=None, traceback=True):
        self.msg = self.t_msg.format(path=path, error_msg=error_msg)
        self.extra_msg = extra_msg

        self.traceback = traceback


class TaskError(CloneError):
    task_exit = False


class TaskFailError(TaskError):
    t_msg = "'{path}' failed: {error_msg}"

    def __init__(
        self, path, error_msg, extra_msg=None, traceback=True, task_exit=False
    ):
        self.msg = self.t_msg.format(path=path, error_msg=error_msg)
        self.extra_msg = extra_msg

        self.traceback = traceback
        self.task_exit = task_exit


class TaskNotDoneError(TaskError):
    t_msg = "'{path}' not uploaded completely."

    def __init__(self, path):
        self.msg = self.t_msg.format(path=path)


class TaskExistError(TaskError):
    level = logging.INFO
    t_msg = "'{path}' already exists."

    task_exit = True

    def __init__(self, path):
        self.msg = self.t_msg.format(path=path)
