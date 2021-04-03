from ..utils import format_path


class CountTask:
    def __init__(self, total_path, file):
        self.total_path = total_path
        self.file = file
        self.bar = None

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    async def run(self):
        self.bar.update(self.file.get_size())


class CountTasks:
    def __init__(self, path):
        self._path = path

    @classmethod
    def transport_factory(cls, path):
        return cls(path=path)

    async def get_task(self, file):
        total_path = format_path(self._path, file.get_relative_path())
        return CountTask(total_path, file)
