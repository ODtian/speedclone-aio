from rich.tree import Tree
from rich.live import Live
from ..log import console
from ..manager import on_close_callbacks

from .commonbar import create_bar, CommonBar


class VirtualBar:
    def __init__(self, tree, overall_bar):
        self._bytes_counted = 0
        self._bytes_total = 0

        self._tree = tree
        self._overall_bar = overall_bar

    def get_sub_bar(self):
        return CommonBar(self._tree)

    def reset(self):
        self._overall_bar.update_counted_bytes(-self._bytes_counted)

    def complete(self):
        self._overall_bar.update_counted_bytes(self._bytes_total - self._bytes_counted)

    def set_info(self, total=0, content=""):
        self._bytes_total = total
        self._overall_bar.update_total_bytes(self._bytes_total)
        self._overall_bar.update_total_tasks(1)

    def update(self, n):
        self._bytes_counted += n
        self._overall_bar.update_counted_bytes(n)

        if self.is_finished():
            self._overall_bar.update_counted_tasks(1)

    def disappear(self):
        pass

    def is_finished(self):
        return self._bytes_counted == self._bytes_total


class OverallBar:
    def __init__(self, bar):
        self._bar = bar

        self._tasks_total = 0
        self._tasks_counted = 0
        self._task = None

        self._create_task()

    def _create_task(self):
        task_id = self._bar.add_task(self._get_tasks_count(), total=0)
        self._task = self._bar.tasks[task_id]

    def _get_tasks_count(self):
        return f"Tasks: {self._tasks_counted} / {self._tasks_total}"

    def update_counted_bytes(self, n):
        self._bar.advance(self._task.id, n)

    def update_total_bytes(self, n):
        total = self._task.total + n
        self._bar.update(self._task.id, total=total)

    def update_counted_tasks(self, n):
        self._tasks_counted += n
        self._bar.update(self._task.id, description=self._get_tasks_count())

    def update_total_tasks(self, n):
        self._tasks_total += n
        self._bar.update(self._task.id, description=self._get_tasks_count())


class OverallBarManager:
    def __init__(self):
        self._bar = create_bar()
        self._tree = Tree(self._bar)
        self._live = Live(self._tree, console=console)

        async def close():
            self._live.stop()

        on_close_callbacks.append(close)

        self._live.start()

        self._overall_bar = OverallBar(self._bar)

    def get_bar(self):
        return VirtualBar(self._tree, self._overall_bar)
