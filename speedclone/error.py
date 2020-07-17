class TaskException(Exception):
    def __init__(self, task, msg):
        self.task = task
        self.msg = msg


class TaskFailError(TaskException):
    def __init__(
        self, exce=None, **kwargs,
    ):
        super().__init__(**kwargs)
        self.exce = exce
        self.msg += "" if self.msg else str(type(self.exce))


class TaskExistError(TaskException):
    def __init__(
        self, **kwargs,
    ):
        if "msg" not in kwargs.keys():
            msg = "File already exists {}".format(
                kwargs.get("task").get_relative_path()
            )
            kwargs["msg"] = msg

        super().__init__(**kwargs)
