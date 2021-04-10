import argparse
import json
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class _Args:
    _args = {}

    def __setattr__(self, name, value):
        self._args[name] = value

    def __getattr__(self, name):
        return self._args[name]


Args = _Args()


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-C",
        "--conf",
        default=os.path.join(BASE_DIR, "..", "conf.json"),
        type=str,
        help="Path to the config file.",
    )

    parser.add_argument(
        "-B", "--bar", default="common", type=str, help="Name of the progress bar."
    )

    parser.add_argument(
        "-I",
        "--interval",
        default=1,
        type=float,
        help="Interval time when putting workers into thread pool.",
    )

    parser.add_argument(
        "-W", "--max-workers", default=10, type=int, help="The number of workers."
    )

    parser.add_argument(
        "-R", "--max-retries", default=3, type=int, help="The times of retries."
    )

    parser.add_argument(
        "--chunk-size",
        default=20 * (1024 ** 2),
        type=int,
        help="Size of single request in multiple chunk uploading.",
    )

    parser.add_argument(
        "--step-size",
        default=(1024 ** 2),
        type=int,
        help="Size of chunk when updating the progress bar.",
    )

    parser.add_argument(
        "--buffer-size",
        default=20 * (1024 ** 2),
        type=int,
        help="Size of buffer when downloading.",
    )

    parser.add_argument(
        "--download-chunk-size",
        default=10 * (1024 ** 2),
        type=int,
        help="Size of single request in downloading.",
    )

    parser.add_argument(
        "--max-download-workers",
        default=3,
        type=int,
        help="Max workers when downloading the file.",
    )

    parser.add_argument(
        "--proxy",
        default=None,
        type=str,
        help="HTTP proxy",
    )

    parser.add_argument(
        "--client-sleep-time",
        default=10,
        type=float,
        help="Time to sleep when client has been throttled.",
    )

    parser.add_argument(
        "--max-clients", default=10, type=int, help="The number of clients."
    )

    parser.add_argument(
        "--max-page-size",
        default=100,
        type=int,
        help="Max size of single page when listing files.",
    )

    parser.add_argument(
        "--aria2-polling-interval",
        default=1,
        type=int,
        help="Aria2 polling interval.",
    )

    parser.add_argument(
        "--failed-task-save-path",
        default="failed-{ts}.txt",
        type=str,
        help="path to save the failed task.",
    )

    args, paths = parser.parse_known_args()

    if os.path.exists(args.conf):
        config = json.load(open(args.conf, "r"))
        return paths, args, config["setting"], config["transport"], config["bar"]
    else:
        raise FileNotFoundError("Config file does not exist.")
