from tqdm.autonotebook import tqdm
from .basebar import BaseBarManager


class VirtualBar:
    def __init__(self, main_bar):
        self.main_bar = main_bar
        self.bytes_counted = 0
        self.bytes_total = 0

    def set_info(self, file_size, total_path):
        self.bytes_total = file_size
        self.main_bar.add_total_bytes(self.bytes_total)
        self.main_bar.add_total_tasks(1)

    def update(self, n):
        self.bytes_counted += n
        self.main_bar.add_counted_bytes(n)
        if self.is_finished():
            self.main_bar.add_counted_tasks(1)

    def is_finished(self):
        return self.bytes_counted == self.bytes_total


class OverallBar:
    def __init__(self):
        self.bar = self.create_bar()
        self.tasks_total = 0
        self.tasks_counted = 0

    def create_bar(self):
        bar_format = "| {desc} | {percentage: >6.2f}% |{bar:20}| {n_fmt:>6} / {total_fmt:<6} [{rate_fmt:<8} {elapsed}>{remaining}]"
        bar = tqdm(
            total=0,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            bar_format=bar_format,
        )
        return bar

    def get_tasks_count(self):
        return "tasks: {} / {}".format(self.tasks_counted, self.tasks_total)

    def refresh_tasks_count(self):
        self.bar.desc = self.get_tasks_count()
        self.bar.refresh()

    def add_counted_bytes(self, n):
        self.bar.update(n)

    def add_total_bytes(self, n):
        self.bar.total += n
        self.bar.refresh()

    def add_counted_tasks(self, n):
        self.tasks_counted += n
        self.refresh_tasks_count()

    def add_total_tasks(self, n):
        self.tasks_total += n
        self.refresh_tasks_count()


class OverallBarManager(BaseBarManager):
    def __init__(self):
        self.bar = OverallBar()

    def get_bar(self):
        return VirtualBar(self.bar)
