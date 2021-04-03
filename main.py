import asyncio
import importlib
import logging

from speedclone.args import parse_args
from speedclone.log import init_logger
from speedclone.manager import TransferManager, init_uvloop

BASE_IMPORT_PATH = "speedclone."


def update_setting(setting, args):
    args_dict = vars(args)
    for k in setting.keys():
        setting[k].update(args_dict)
    return setting


def get_transports(paths):
    def _get_transport_from_path(path):
        sep_path = path.split(":/")
        return {"name": sep_path.pop(0), "path": ":/".join(sep_path)}

    return tuple(map(_get_transport_from_path, paths))


def import_cls(mod, cls):
    return getattr(
        importlib.import_module(BASE_IMPORT_PATH + mod),
        cls,
    )


def get_transport_instance_peer(transport_peer):
    def _get_transport_instance(t):
        mod = t["transport"]["mod"]
        cls = t["transport"]["cls"][t["as"]]
        return import_cls(mod=mod, cls=cls).transport_factory(
            path=t["path"], **t["config"]
        )

    return tuple(map(_get_transport_instance, transport_peer))


async def main():
    init_logger()
    init_uvloop()

    paths, args, config = parse_args()

    setting = config["setting"]
    transport_map = config["transport"]
    bar_map = config["bar"]

    transport_chain = get_transports(paths)
    for transport in transport_chain:
        try:
            config = setting[transport["name"]]
        except KeyError as e:
            logging.error(f"Could not find config named '{transport['name']}'")
            raise e
        else:
            transport["transport"] = transport_map[config["transport"]]
            config.pop("transport")
            transport["config"] = config

    bar_manager_class = import_cls(**bar_map[args.bar])
    bar_manager = bar_manager_class()

    for i in range(len(transport_chain) - 1):
        source, target = transport_chain[i : i + 2]
        source["as"] = "source"
        target["as"] = "target"

        source, target = get_transport_instance_peer((source, target))
        transfer_manager = TransferManager(
            source=source,
            target=target,
            bar_manager=bar_manager,
        )

        await transfer_manager.run(
            interval=args.interval,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            chunk_size=args.chunk_size,
            step_size=args.step_size,
            download_chunk_size=args.download_chunk_size,
            proxy=args.proxy,
            client_sleep_time=args.client_sleep_time,
            max_clients=args.max_clients,
            max_page_size=args.max_page_size,
            max_download_workers=args.max_download_workers,
            aria2_polling_interval=args.aria2_polling_interval,
        )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
