from tqdm.autonotebook import tqdm
from .basebar import BaseBarManager


class VirtualBar:
    def __init__(self, bar, total):
        self.bar = bar
        self.counted = 0
        self.total = 0

    def init_bar(self, total, desc):
        self.total = total

    def update(self, n):
        self.counted += n
        self.bar.byte_bar.update(n)

    def close(self):
        self.bar.byte_bar.update(self.total - self.counted)
        self.bar.count_bar.update(1)


class SlimBar:
    def __init__(self):
        self.byte_bar = tqdm(
            total=0, position=1, unit="B", unit_scale=True, unit_divisor=1024
        )
        self.count_bar = tqdm(total=0, position=2, unit="tasks")

    def add_byte(self, n):
        self.byte_bar.total += n
        self.byte_bar.refresh()

    def add_count(self, n):
        self.byte_bar.total += n
        self.byte_bar.refresh()


class SlimBarManager(BaseBarManager):
    def __init__(self):
        self.bar = SlimBar()

    def get_bar(self, task):
        self.bar.add_byte(task.get_total())
        self.bar.add_count(1)
        return VirtualBar(self.bar)

    def exists(self, e):
        super().exists(e)
        self.bar.byte_bar.update(e.task.get_total())
        self.bar.count_bar.update(1)
