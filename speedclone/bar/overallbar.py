from tqdm.autonotebook import tqdm


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
        self.tasks_total = 0
        self.tasks_counted = 0

        self._bar = None
        self._create_bar()

    def _create_bar(self):
        bar_format = "| {desc} | {percentage: >6.2f}% |{bar:20}| {n_fmt:>6} / {total_fmt:<6} [{rate_fmt:<8} {elapsed}>{remaining}]"
        self._bar = tqdm(
            total=0,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            bar_format=bar_format,
        )

    def _get_tasks_count(self):
        return f"tasks: {self.tasks_counted} / {self.tasks_total}"

    def _refresh_tasks_count(self):
        self._bar.desc = self._get_tasks_count()
        self._bar.refresh()

    def add_counted_bytes(self, n):
        self._bar.update(n)

    def add_total_bytes(self, n):
        self._bar.total += n
        self._bar.refresh()

    def add_counted_tasks(self, n):
        self.tasks_counted += n
        self._refresh_tasks_count()

    def add_total_tasks(self, n):
        self.tasks_total += n
        self._refresh_tasks_count()


class OverallBarManager:
    def __init__(self):
        self.bar = OverallBar()

    def get_bar(self):
        return VirtualBar(self.bar)
