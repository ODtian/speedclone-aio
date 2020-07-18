import asyncio
import logging
import os
import random
from itertools import chain, groupby
from json.decoder import JSONDecodeError

import aiostream
from httpx import HTTPError

from .. import ahttpx
from ..client.google import (
    FileSystemServiceAccountTokenBackend,
    FileSystemTokenBackend,
    GoogleDrive,
)
from ..error import TaskExistError, TaskFailError

# from ..manager import TransferManager
from ..utils import (
    iter_path,
    norm_path,
    compose_path,
    aenumerate,
    aiter_bytes,
    aiter_data,  # console_write,
)


class GoogleDriveTransferDownloadTask:
    http = {}

    def __init__(self, file_id, relative_path, size, client):
        self.file_id = file_id
        self.relative_path = relative_path
        self.size = size
        self.client = client

    async def iter_data(self, chunk_size=(10 * 1024 ** 2), copy=False):
        if copy:
            yield self.file_id
        else:
            r = await self.client.get_download_request(self.file_id)
            r.raise_for_status()
            async for data in aiter_bytes(r.aiter_bytes(), chunk_size=chunk_size):
                yield data

    def get_relative_path(self):
        return self.relative_path

    def get_total(self):
        return self.size


class GoogleDriveTransferUploadTask:
    chunk_size = 10 * 1024 ** 2
    step_size = 1024 ** 2
    http = {}

    def __init__(self, task, bar, client):
        self.task = task
        self.bar = bar
        self.client = client

    def _handle_request_error(self, request):
        if request.status_code == 429:
            sleep_time = request.headers.get("Retry-After")
            seconds = self.client.sleep(sleep_time)
            raise Exception("Client Limit Exceeded. Sleep for {}s".format(seconds))

        if request.status_code == 400 and "LimitExceeded" in request.text:
            seconds = self.client.sleep()
            raise Exception("Client Limit Exceeded. Sleep for {}s".format(seconds))

        try:
            request.raise_for_status()
        except HTTPError as e:
            try:
                message = (
                    e.response.json().get("error", {}).get("message", "Empty message")
                )
            except JSONDecodeError:
                message = "No json response returned."
            raise Exception(message)

    async def _do_copy(self, folder_id, name):
        if self.client.sleeping:
            raise TaskFailError(
                task=self.task, msg="Client is sleeping, will retry later."
            )

        try:
            async for file_id in self.task.iter_data(copy=True):
                result = await self.client.copy_to(file_id, folder_id, name)
        except Exception as e:
            self.bar.close()
            raise TaskFailError(exce=e, task=self.task, msg=str(e))
        else:
            if result is False:
                raise TaskExistError(task=self.task)
            else:
                self._handle_request_error(result)

            file_size = self.task.get_total()
            self.bar.init_bar(file_size, self.task.get_relative_path())
            self.bar.update(file_size)
            self.bar.close()

    async def run(self, folder_id, name):
        if self.client.sleeping:
            raise TaskFailError(
                task=self.task, msg="Client is sleeping, will retry later."
            )

        try:
            upload_url_request = await self.client.get_upload_url(folder_id, name)
        except Exception as e:
            raise TaskFailError(exce=e, task=self.task, msg=str(e))
        else:
            if upload_url_request is False:
                raise TaskExistError(task=self.task)

        try:
            self._handle_request_error(upload_url_request)
            upload_url = upload_url_request.headers.get("Location")
            file_size = self.task.get_total()

            self.bar.init_bar(file_size, self.task.get_relative_path())

            async for i, file_piece in aenumerate(
                self.task.iter_data(chunk_size=self.chunk_size)
            ):
                chunk_length = len(file_piece)

                start = i * self.chunk_size
                end = start + chunk_length - 1
                headers = {
                    "Content-Range": "bytes {}-{}/{}".format(start, end, file_size),
                    "Content-Length": str(chunk_length),
                }
                data = aiter_data(file_piece, self.step_size, self.bar)

                r = await ahttpx.put(
                    upload_url, data=data, headers=headers, **self.client.http
                )
                self._handle_request_error(r)

                if r.status_code == 308:
                    header_range = r.headers.get("Range")
                    if not header_range or header_range.lstrip("bytes=0-") != str(end):
                        raise Exception(
                            "Range missed when uploading file {} ".format(name)
                        )

                elif "id" not in r.json().keys():
                    raise Exception("Upload not successful {}".format(name))

        except Exception as e:
            raise TaskFailError(exce=e, task=self.task, msg=str(e))
        finally:
            self.bar.close()


class GoogleDriveTransferManager:
    max_page_size = 100

    def __init__(self, path, clients):
        self.path = path
        self.clients = clients

        # self.root, *self.base_path, self.base_name = self.path.split("/")
        # self.base_path = norm_path(*self.base_path)
        self.root, *self.base_path = self.path.split("/")
        # self.root = root
        self.dir_create_list = []
        # self.dir_cache = {}
        # self.dir_cache = {"": root}
        # self.dir_create_list = []
        # self.dir_level = 0
        self.list_files_set = set()

    def _get_client(self):
        while True:
            client = self.clients.pop(0)
            self.clients.append(client)
            if client.sleeping:
                continue
            else:
                return client

    # async def _create_dir(self, path):
    #     client = self._get_client()

    #     parent_path, name = os.path.split(path)
    #     parent_id = await self._get_cache_dir_id(parent_path)

    #     has_folder = (
    #         (await client.get_files_by_name(parent_id, name, fields=("files/id",)))
    #         .json()
    #         .get("files")
    #     )

    #     if has_folder:
    #         folder_id = has_folder[0].get("id")
    #     else:
    #         folder_id = (
    #             (await client.create_file_by_name(parent_id, name)).json().get("id")
    #         )

    #     self.dir_cache[path] = folder_id
    #     return folder_id

    # async def _create_dir(self, parent_id, name):
    #     client = self._get_client()

    #     # parent_path, name = os.path.split(path)
    #     # parent_id = await self._get_cache_dir_id(parent_path)

    #     has_folder = (
    #         (await client.get_files_by_name(parent_id, name, fields=("files/id",)))
    #         .json()
    #         .get("files")
    #     )

    #     if has_folder:
    #         folder_id = has_folder[0].get("id")
    #     else:
    #         logging.debug("No folder found, creat one.")
    #         folder_id = (
    #             (await client.create_file_by_name(parent_id, name)).json().get("id")
    #         )

    #     # self.dir_cache[path] = folder_id
    #     return folder_id
    # async def _get_dir_id(self, parent_id, name):
    #     client = self._get_client()

    #     # parent_path, name = os.path.split(path)
    #     # parent_id = await self._get_cache_dir_id(parent_path)

    #     has_folder = (
    #         (
    #             await client.get_files_by_name(
    #                 parent_id, name, mime="folder", fields=("files/id",)
    #             )
    #         )
    #         .json()
    #         .get("files")
    #     )

    #     if has_folder:
    #         folder_id = has_folder[0].get("id")
    #     else:
    #         logging.debug("No folder found, creat one.")
    #         folder_id = (
    #             (await client.create_file_by_name(parent_id, name, mime="folder"))
    #             .json()
    #             .get("id")
    #         )

    #     # self.dir_cache[path] = folder_id
    #     return folder_id

    # async def _get_cache_dir_id(self, path):
    #     return self.dir_cache.get(path) or await self._create_dir(path)

    # async def _get_root_name(self):
    #     client = self._get_client()
    #     # root_id = self.dir_cache[""]
    #     r = await client.get_file(self.root, "name")
    #     return r.json()["name"]

    # async def _list_files(self, path):
    #     client = self._get_client()
    #     dir_path, name = os.path.split(path)
    #     parent_dir_id = await self._get_cache_dir_id(dir_path)
    #     is_file = (
    #         (
    #             await client.get_files_by_name(
    #                 parent_dir_id,
    #                 name,
    #                 mime="file",
    #                 fields=("files/id", "files/name", "files/mimeType", "files/size"),
    #             )
    #         )
    #         .json()
    #         .get("files", [])
    #     )
    #     for i in is_file:
    #         yield i.get("id", ""), i.get("name", ""), int(i.get("size", 0))
    async def _get_base_item(self):
        client = self._get_client()
        if self.base_path:
            parent_id = self.root
            for i in self.base_path.split("/"):
                item = await client.get_files_by_name(parent_id, i)
                parent_id = item["id"]
            return item
        else:
            p = {"q": "id = {} and trashed = false".format(self.root)}
            return await client.get_files_by_p(p).json().get("files", [None])[0]

    async def _list_items(self, item, page_token=None, client=None):
        try:
            client = client or self._get_client()

            p = {
                "q": " and ".join(
                    ["'{parent_id}' in parents", "trashed = false"]
                ).format(parent_id=item["id"]),
                "pageSize": self.max_page_size,
                "fields": ", ".join(
                    (
                        "nextPageToken",
                        "files/id",
                        "files/name",
                        "files/size",
                        "files/mimeType",
                    )
                ),
            }

            if page_token:
                p.update({"pageToken": page_token})

            r = await client.get_files_by_p(p).json()

            def fl(i):
                i["name"] = norm_path(item["name"], i["name"])

                if i["mimeType"] == "application/vnd.google-apps.folder":
                    return True
                else:
                    result = (i["id"], i["name"], int(i["size"]))
                    if result not in self.list_files_set:
                        self.list_files_set.add(result)
                        yield result

            folders = filter(fl, r.get("files", []))
            next_token = r.get("nextPageToken")

            if next_token:
                async for i in self._list_dirs(item, next_token, client):
                    yield i

            async with aiostream.stream.merge(
                *map(self._list_dirs, folders)
            ).stream() as streamer:
                async for i in streamer:
                    yield i

        except Exception:
            logging.error(
                "Error occur when fetching files {}".format(item["name"]), exc_info=True
            )
            async for i in self._list_dirs(item):
                yield i

    # async def _list_dirs(self, path, page_token=None, client=None):
    #     try:
    #         client = client or self._get_client()

    #         abs_path = norm_path(self.base_path, path)
    #         dir_id = await self._get_cache_dir_id(abs_path)

    #         p = {
    #             "q": " and ".join(
    #                 ["'{parent_id}' in parents", "trashed = false"]
    #             ).format(parent_id=dir_id),
    #             "pageSize": self.max_page_size,
    #             "fields": ", ".join(
    #                 (
    #                     "nextPageToken",
    #                     "files/id",
    #                     "files/name",
    #                     "files/size",
    #                     "files/mimeType",
    #                 )
    #             ),
    #         }

    #         if page_token:
    #             p.update({"pageToken": page_token})

    #         r = await client.get_files_by_p(p)
    #         result = r.json()

    #         folders = []

    #         for file in result.get("files", []):
    #             relative_path = norm_path(path, file.get("name", ""))
    #             if file["mimeType"] == "application/vnd.google-apps.folder":
    #                 folders.append(relative_path)
    #             else:
    #                 file_id = file.get("id", "")
    #                 file_size = int(file.get("size", 0))

    #                 item = (file_id, relative_path, file_size)
    #                 if item not in self.list_files_set:
    #                     self.list_files_set.add(item)
    #                     yield item

    #         next_token = result.get("nextPageToken")
    #         if next_token:
    #             async for item in self._list_dirs(path, next_token, client):
    #                 yield item

    #         async with aiostream.stream.merge(
    #             *[self._list_dirs(folder_path) for folder_path in folders]
    #         ).stream() as streamer:
    #             async for item in streamer:
    #                 yield item

    #     except Exception:
    #         logging.error(
    #             "Error occur when fetching files {}".format(path), exc_info=True
    #         )
    #         async for item in self._list_dirs(path):
    #             yield item

    @classmethod
    def get_transfer(cls, conf, path, args):

        GoogleDriveTransferUploadTask.chunk_size = args.chunk_size
        GoogleDriveTransferUploadTask.step_size = args.step_size
        GoogleDrive.sleep_time = args.sleep
        cls.max_page_size = args.max_page_size

        if args.copy:
            GoogleDriveTransferUploadTask.run = GoogleDriveTransferUploadTask._do_copy

        GoogleDrive.http = conf.get("http", {})
        FileSystemTokenBackend.http = conf.get("http", {})

        token_path = conf.get("token_path")

        if os.path.exists(token_path):
            use_service_account = conf.get("service_account", False)

            path = norm_path(conf.get("root", ""), path)
            drive = conf.get("drive_id")
            cred = conf.get("client")

            clients = []

            for p in iter_path(token_path):
                if use_service_account:
                    token_backend = FileSystemServiceAccountTokenBackend(cred_path=p)
                else:
                    token_backend = FileSystemTokenBackend(cred=cred, token_path=p)
                client = GoogleDrive(token_backend=token_backend, drive=drive)
                clients.append(client)

            random.shuffle(clients)

            clients_num = conf.get("clients", 0)
            if clients_num > 0:
                clients = clients[:clients_num]

            return cls(path=path, clients=clients)
        else:
            raise Exception("Token path not exists")

    async def iter_tasks(self):
        base_item = await self._get_base_item()
        if base_item:
            if base_item["mimeType"] != "application/vnd.google-apps.folder":
                yield (
                    base_item["id"],
                    base_item["name"],
                    int(base_item["size"]),
                    self._get_client(),
                )
            else:
                async for file_id, relative_path, size in self._list_items(base_item):
                    yield GoogleDriveTransferDownloadTask(
                        file_id, relative_path, size, self._get_client()
                    )

        # # async for file_id, relative_path, size in self._list_files(self.path):
        # #     yield GoogleDriveTransferDownloadTask(
        # #         file_id, relative_path, size, self._get_client()
        # #     )
        # #     return

        # root_name = "" if self.path else await self._get_root_name()

        # async for file_id, relative_path, size in self._list_dirs(self.base_name):
        #     yield GoogleDriveTransferDownloadTask(
        #         file_id, norm_path(root_name, relative_path), size, self._get_client()
        #     )

    async def get_worker(self, task):

        total_path = norm_path(self.path, task.get_relative_path())
        dir_path, name = os.path.split(total_path)

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self.dir_create_list.append((dir_path, future))

        if getattr(task, "end", False):

            async def handle_dir_create():
                sorted_dirs = sorted(
                    set(
                        chain.from_iterable(
                            [compose_path(d) for d, _ in self.dir_create_list]
                        )
                    )
                )
                groups = groupby(sorted_dirs, key=lambda item: item.count("/"))

                dir_cache = {"": self.root}

                async def create_dir(dir_path):
                    client = self._get_client()

                    parent_path, name = os.path.split(dir_path)
                    parent_id = dir_cache.get(parent_path, self.root)

                    has_folder = (
                        (
                            await client.get_files_by_name(
                                parent_id, name, mime="folder", fields=("files/id",)
                            )
                        )
                        .json()
                        .get("files")
                    )

                    folder_id = (
                        has_folder[0].get("id")
                        if has_folder
                        else (
                            (
                                await client.create_file_by_name(
                                    parent_id, name, mime="folder"
                                )
                            )
                            .json()
                            .get("id")
                        )
                    )

                    dir_cache[dir_path] = folder_id

                for _, g in groups:
                    await asyncio.gather(*map(create_dir, g))

                for d, f in self.dir_create_list:
                    f.set_result(dir_cache.get(d, self.root))

            asyncio.run_coroutine_threadsafe(handle_dir_create(), loop)
        # dir_level = dir_path.count("/")
        # if dir_level < self.dir_level:
        #     dir_id = await self._get_cache_dir_id(dir_path)
        #     future.set_result(dir_id)
        # else:
        #     self.dir_create_list.append((dir_path, future))

        #     if dir_level > self.dir_level or TransferManager.me.pusher_finished:
        #         sorted_list = sorted(self.dir_create_list, key=lambda item: item[0])
        #         self.dir_level = dir_level
        #         self.dir_create_list = []

        #         async def handle_dir_create():
        #             groups = groupby(sorted_list, key=lambda item: item[0])

        #             for k, g in groups:
        #                 dir_id = await self._get_cache_dir_id(k)
        #                 for _, f in g:
        #                     f.set_result(dir_id)

        #         asyncio.run_coroutine_threadsafe(handle_dir_create(), loop)

        # try:
        client = self._get_client()

        async def worker(bar):
            dir_id = await future
            w = GoogleDriveTransferUploadTask(task, bar, client)
            await w.run(dir_id, name)

        return worker
