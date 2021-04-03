from tqdm.autonotebook import tqdm


class CommonBar:
    def __init__(self):
        self.bytes_counted = 0
        self.bytes_total = 0

        self._bar = None
        self._content = ""

    def _create_bar(self):
        bar_format = "{percentage: >6.2f}% |{bar:20}| {n_fmt:>6} / {total_fmt:<6} [{rate_fmt:>8} {elapsed}>{remaining}] [{desc}]"
        self._bar = tqdm(
            total=self.bytes_total,
            bar_format=bar_format,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        )
        self._bar.set_description_str(self._content)

    def set_info(self, file_size, total_path):
        self.bytes_total = file_size
        self._content = total_path

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
