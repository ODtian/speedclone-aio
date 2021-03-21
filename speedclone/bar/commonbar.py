# from threading import Timer

import asyncio

from tqdm.autonotebook import tqdm

from .basebar import BaseBarManager

MIN_WIDTH = 40


class CommonBar:
    def __init__(self):
        self.bar = None

        self.stop = False

        self.bytes_counted = 0
        self.bytes_total = 0

        self._content = ""
        self.now_content = ""

    # async def timer(self):
    #     while not self.stop:
    #         await asyncio.sleep(0.5)
    #         self.scroll_text()

    def set_info(self, file_size, total_path):
        # self.content = total_path.ljust(MIN_WIDTH, " ")
        self.bytes_total = file_size
        self._content = total_path
        self.now_content = " " * MIN_WIDTH + self._content

        self.bar = self.create_bar()

    def update(self, n):
        self.bytes_counted += n
        self.bar.update(n)

        if self.is_finished():
            self.stop = True
            self.bar.set_description_str(self._content)
            self.bar.close()

    def is_finished(self):
        return self.bytes_counted == self.bytes_total

    def create_bar(self):
        bar_format = "| {desc} | {percentage: >6.2f}% |{bar:20}| {n_fmt:>6} / {total_fmt:<6} [{rate_fmt:<8} {elapsed}>{remaining}]"
        bar = tqdm(
            total=self.bytes_total,
            bar_format=bar_format,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        )
        return bar

    def scroll_text(self):
        self.now_content = self.now_content[1:] + self.now_content[0]
        # self.bar.set_description_str((self.content[:MIN_WIDTH]).ljust(MIN_WIDTH, " "))
        self.bar.set_description_str(self.now_content[:MIN_WIDTH])


class CommonBarManager(BaseBarManager):
    def __init__(self):
        super(CommonBarManager, self).__init__()
        self._bars = []

        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(self.timer(), loop)

    async def timer(self):
        while True:
            for bar in self._bars:
                if not bar.stop:
                    bar.scroll_text()
            await asyncio.sleep(0.5)

    def get_bar(self):
        bar = CommonBar()
        self._bars.append(bar)
        return bar
