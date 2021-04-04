from tqdm import tqdm


class CommonBar:
    def __init__(self, level=0):
        self.bytes_counted = 0
        self.bytes_total = 0

        self._bar = None
        self._content = ""
        self._level = level
        self._sub_bars = {}

    def _create_bar(self):
        bar_format = (
            " " * self._level * 4
            + ("·~>  " if self._level != 0 else "")
            + "{percentage: <6.2f}% |{bar:20}| {n_fmt:>6} / {total_fmt:<6} [{rate_fmt:>8} {elapsed}>{remaining}] [{desc}]"
        )
        self._bar = tqdm(
            total=self.bytes_total,
            bar_format=bar_format,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
            leave=self._level == 0,
        )
        self._bar.set_description_str(self._content)

    def get_sub_bar(self, name):
        sub_bar = self._sub_bars[name] = CommonBar(level=self._level + 1)
        return sub_bar

    def set_info(self, total=0, content=""):
        self.bytes_total = total
        self._content = content

        self._create_bar()

    def update(self, n):
        self.bytes_counted += n
        self._bar.update(n)

        if self.is_finished():
            self._bar.close()

    def is_finished(self):
        return self.bytes_counted == self.bytes_total


class CommonBarManager:
    def get_bar(self):
        bar = CommonBar()
        return bar
