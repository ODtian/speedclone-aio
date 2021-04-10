from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.tree import Tree
from rich.live import Live
from ..log import console
from ..manager import on_close_callbacks


def create_bar():
    return Progress(
        "[progress.percentage]{task.percentage:>3.0f}%",
        BarColumn(),
        DownloadColumn(),
        TextColumn("[red]["),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        TextColumn("[cyan]]"),
        TextColumn("[yellow]["),
        TextColumn("[yellow]{task.description}", justify="center"),
        TextColumn("[yellow]]"),
    )


class CommonBar:
    def __init__(self, tree, bar=None):
        self._bar = bar or create_bar()
        self._parent_tree, self._tree = (
            (None, tree) if bar else (tree, tree.add(self._bar))
        )

        self._task = None

    def get_sub_bar(self):
        return CommonBar(self._tree)

    def reset(self):
        self._bar.reset(self._task.id)

    def complete(self):
        self._bar.update(self._task.id, completed=self._task.total)

    def set_info(self, total=0, content=""):
        task_id = self._bar.add_task(content, total=total, start=False)
        self._task = self._bar.tasks[task_id]

    def update(self, n):
        if not self._task.started:
            self._bar.start_task(self._task.id)

        self._bar.advance(self._task.id, n)

    def disappear(self):
        if self._parent_tree:
            self._parent_tree.children.remove(self._tree)

    def is_finished(self):
        return self._task.finished


class CommonBarManager:
    def __init__(self):
        self._table = Table.grid()
        self._live = Live(self._table, console=console)

        async def close():
            self._live.stop()

        on_close_callbacks.append(close)

        self._live.start()

    def get_bar(self):
        bar = create_bar()
        tree = Tree(bar)
        self._table.add_row(tree)
        return CommonBar(tree, bar)
