import importlib
import logging

from speedclone.args import parse_args
from speedclone.log import init_logger
from speedclone.manager import TransferManager, init_uv

BASE_IMPORT_PATH = "speedclone."


def update_setting(setting, args):
    args_dict = vars(args)
    for k in setting.keys():
        setting[k].update(args_dict)
    return setting


def seperate_urls(urls):
    def split(url):
        sep_url = url.split(":/")
        return {"name": sep_url.pop(0), "path": ":/".join(sep_url)}

    return list(map(split, urls))


def import_cls(mod, cls):
    return getattr(importlib.import_module(BASE_IMPORT_PATH + mod), cls,)


def transfer_single(transfers):
    def import_trans(transfer):
        return import_cls(
            mod=transfer["trans_import"]["mod"],
            cls=transfer["trans_import"]["cls"][transfer["as"]],
        ).get_trans(transfer["path"], transfer["config"])

    trans = list(map(import_trans, transfers))
    return trans


def main():
    init_logger()
    init_uv()

    args, rest, conf = parse_args()

    setting = conf["setting"]
    trans_map = conf["transmitter"]
    bars = conf["bar"]

    transfer_chain = seperate_urls(rest)
    for transfer in transfer_chain:
        try:
            config = setting[transfer["name"]]
        except Exception as e:
            logging.error("Could not find config named '{}'".format(transfer["name"]))
            raise e
        else:
            transfer["config"] = config

        trans_name = config["transmitter"]
        transfer["trans_import"] = trans_map[trans_name]

    bar_manager = import_cls(**bars[args.bar]).get_bar_manager()

    for i in range(len(transfer_chain) - 1):

        transfers = transfer_chain[i : i + 2]

        transfers[0]["as"] = "source"
        transfers[1]["as"] = "target"
        trans = transfer_single(transfers)
        trans = list(trans)
        transfer_manager = TransferManager(
            trans=trans,
            bar_manager=bar_manager,
            interval=args.interval,
            max_workers=args.max_workers,
        )
        transfer_manager.run()


if __name__ == "__main__":
    main()
