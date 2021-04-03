import argparse
import json
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
args_dict = {
    "CHUNK_SIZE": 20 * (1024 ** 2),
    "STEP_SIZE": 1024 ** 2,
    "DOWNLOAD_CHUNK_SIZE": 1024 ** 2,
    "PROXY": None,
    "CLIENT_SLEEP_TIME": 10,
    "MAX_CLIENTS": 10,
    "MAX_PAGE_SIZE": 100,
    "MAX_DOWNLOAD_WORKERS": 3,
    "ARIA2_POLLING_INTERVAL": 1,
}


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-I",
        "--interval",
        default=0.05,
        type=float,
        help="Interval time when putting workers into thread pool.",
    )

    parser.add_argument(
        "-W", "--max-workers", default=10, type=int, help="The number of workers."
    )

    parser.add_argument(
        "-B", "--bar", default="common", type=str, help="Name of the progress bar."
    )

    parser.add_argument(
        "-C",
        "--conf",
        default=os.path.join(BASE_DIR, "..", "conf.json"),
        type=str,
        help="Path to the config file.",
    )

    parser.add_argument(
        "--chunk-size",
        default=20 * (1024 ** 2),
        type=int,
        help="Size of single request in multiple chunk uploading.",
    )

    parser.add_argument(
        "--step-size",
        default=1024 ** 2,
        type=int,
        help="Size of chunk when updating the progress bar.",
    )

    parser.add_argument(
        "--download-chunk-size",
        default=1024 ** 2,
        type=int,
        help="Size of single request in downloading.",
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
        "--max-download-workers",
        default=3,
        type=int,
        help="Max workers when downloading the file.",
    )

    parser.add_argument(
        "--aria2-polling-interval",
        default=1,
        type=int,
        help="Aria2 polling interval.",
    )

    args, paths = parser.parse_known_args()

    if os.path.exists(args.conf):
        config = json.load(open(args.conf, "r"))

        args_dict["INTERVAL"] = args.interval
        args_dict["MAX_WORKERS"] = args.max_workers
        args_dict["MAX_RETRIES"] = args.max_retries

        args_dict["CHUNK_SIZE"] = args.chunk_size
        args_dict["STEP_SIZE"] = args.step_size
        args_dict["DOWNLOAD_CHUNK_SIZE"] = args.download_chunk_size

        args_dict["PROXY"] = args.proxy
        args_dict["CLIENT_SLEEP_TIME"] = args.client_sleep_time

        args_dict["MAX_CLIENTS"] = args.max_clients
        args_dict["MAX_PAGE_SIZE"] = args.max_page_size
        args_dict["MAX_DOWNLOAD_WORKERS"] = args.max_download_workers
        args_dict["ARIA2_POLLING_INTERVAL"] = args.aria2_polling_interval

        return paths, args, config
    else:
        raise FileNotFoundError("Config file does not exist.")
