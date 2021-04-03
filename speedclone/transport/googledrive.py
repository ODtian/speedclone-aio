import asyncio
import json

# import logging
import os
import random
import time

import aiofiles
import jwt

from .. import ahttpx
from ..args import args_dict
from ..error import (
    HttpStatusError,
    TaskError,
    TaskExistError,
    TaskFailError,
    TaskNotDoneError,
)
from ..utils import (
    aiter_data,
    format_path,
    iter_path,
    parse_params,
    raise_for_status,
)

from ..filereader import HttpFileReader

CHUNK_SIZE = args_dict["CHUNK_SIZE"]
STEP_SIZE = args_dict["STEP_SIZE"]
DOWNLOAD_CHUNK_SIZE = args_dict["DOWNLOAD_CHUNK_SIZE"]

CLIENT_SLEEP_TIME = args_dict["CLIENT_SLEEP_TIME"]

MAX_CLIENTS = args_dict["MAX_CLIENTS"]
MAX_PAGE_SIZE = args_dict["MAX_PAGE_SIZE"]
MAX_DOWNLOAD_WORKERS = args_dict["MAX_DOWNLOAD_WORKERS"]


TOKEN_URL = "https://oauth2.googleapis.com/token"
SERVICE_ACCOUNT_TOKEN_SCOPE = "https://www.googleapis.com/auth/drive"
TOKEN_EXPIRES_IN = 3600

DRIVE_URL = "https://www.googleapis.com/drive/v3/files"
DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"


class GoogleTokenBackend:
    def __init__(self, token_path, credit):
        self.token_path = token_path
        self.credit = credit

        if os.path.exists(self.token_path):
            with open(self.token_path, "r") as f:
                self.token = json.load(f)
        else:
            raise ValueError("No token file found.")

    async def _update_tokenfile(self):
        async with aiofiles.open(self.token_path, "w") as f:
            data = json.dumps(self.token)
            await f.write(data)

    def _token_expired(self):
        if self.token:
            expired_time = self.token.get("expires_in") + self.token.get("get_time", 0)
            return expired_time <= int(time.time())
        else:
            return True

    async def _refresh_accesstoken(self):
        now_time = int(time.time())
        refresh_token = self.token.get("refresh_token")

        data = {"refresh_token": refresh_token, "grant_type": "refresh_token"}
        data.update(self.credit)

        r = await ahttpx.post(TOKEN_URL, data=data)
        raise_for_status(r)

        self.token.update(r.json())
        self.token["get_time"] = now_time

        await self._update_tokenfile()

    async def get_access_token(self):
        if self._token_expired():
            await self._refresh_accesstoken()
        return self.token["token_type"] + " " + self.token["access_token"]


class GoogleServiceAccountTokenBackend(GoogleTokenBackend):
    def __init__(self, credit_path):
        self.credit_path = credit_path
        self.token = {}

        if os.path.exists(self.credit_path):
            with open(self.credit_path, "r") as f:
                self.config = json.load(f)
        else:
            raise Exception("No credit file found.")

    async def _refresh_accesstoken(self):
        now_time = int(time.time())
        token_data = {
            "iss": self.config["client_email"],
            "scope": SERVICE_ACCOUNT_TOKEN_SCOPE,
            "aud": TOKEN_URL,
            "exp": now_time + TOKEN_EXPIRES_IN,
            "iat": now_time,
        }

        auth_jwt = jwt.encode(
            token_data,
            self.config["private_key"].encode("utf-8"),
            algorithm="RS256",
        )

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": auth_jwt,
        }
        r = await ahttpx.post(TOKEN_URL, data=data)
        raise_for_status(r)

        self.token.update(r.json())
        self.token["get_time"] = now_time


class GoogleDriveClient:
    def __init__(self, token_backend, drive_id=""):
        self.token_backend = token_backend
        self.drive_id = drive_id

        self._sleep_until = 0
        self._sem = asyncio.Semaphore(1)

    async def _get_headers(self, content_type="application/json"):
        token = await self.token_backend.get_access_token()
        headers = {
            "Authorization": token,
            "Content-Type": content_type,
        }
        return headers

    def _get_params(self, params={}):
        params.update(
            {
                "supportsAllDrives": "true",
            }
        )
        if self.drive_id:
            params.update(
                {
                    "corpora": "drive",
                    "includeItemsFromAllDrives": "true",
                    "driveId": self.drive_id,
                }
            )
        return params

    async def _wait_for_sleep(self):
        async with self._sem:
            seconds = self._sleep_until - int(time.time())
            if seconds > 0:
                await asyncio.sleep(seconds)

    def _handle_status_error(self, e):
        if e.status_code == 429:
            client_sleep_time = int(e.response.headers.get("Retry-After"))
            self._sleep(client_sleep_time)
        elif e.status_code // 100 == 4 and "LimitExceeded" in e.response.text:
            self._sleep(CLIENT_SLEEP_TIME)

        raise e

    def _sleep(self, seconds=CLIENT_SLEEP_TIME):
        self._sleep_until = int(time.time()) + seconds

    async def get_file_by_id(self, file_id, fields=("id", "name", "mimeType")):
        await self._wait_for_sleep()

        params = {"fields": ", ".join(fields), "supportsAllDrives": "true"}
        headers = await self._get_headers()

        r = await ahttpx.get(DRIVE_URL + "/" + file_id, headers=headers, params=params)
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self._handle_status_error(e)

        return r.json()

    async def list_files_by_params(self, params):
        await self._wait_for_sleep()

        headers = await self._get_headers()

        r = await ahttpx.get(
            DRIVE_URL, headers=headers, params=self._get_params(params)
        )
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self._handle_status_error(e)

        return r.json()

    async def list_files_by_query(
        self,
        query,
        fields=("files/id", "files/name", "files/mimeType"),
    ):
        params = {
            "q": query,
            "fields": ", ".join(fields),
        }

        return await self.list_files_by_params(params)

    async def list_files_by_name(
        self,
        parent_id,
        name,
        is_folder=None,
        fields=("files/id", "files/name", "files/mimeType"),
    ):
        escape_name = name.replace("'", r"\'")
        query_list = [
            f"'{parent_id}' in parents",
            f"name = '{escape_name}'",
            "trashed = false",
        ]

        if is_folder is not None:
            query_list.append(
                f"mimeType {'=' if is_folder else '!='} '{FOLDER_MIMETYPE}'"
            )

        query = " and ".join(query_list)
        return await self.list_files_by_query(query, fields)

    async def _create_file_by_name(self, parent_id, name, is_folder):
        await self._wait_for_sleep()

        params = {"supportsAllDrives": "true"}
        headers = await self._get_headers()

        data = {"name": name, "parents": [parent_id]}
        if is_folder:
            data.update({"mimeType": FOLDER_MIMETYPE})

        r = await ahttpx.post(DRIVE_URL, headers=headers, params=params, json=data)
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self._handle_status_error(e)

        return r.json()

    async def create_file_by_name(self, parent_id, name, is_folder):
        exist_files = (
            await self.list_files_by_name(
                parent_id, name, is_folder=is_folder, fields=("files/id",)
            )
        ).get("files", [])

        if exist_files:
            return exist_files[0]
        else:
            return await self._create_file_by_name(parent_id, name, is_folder)

    async def _copy_file_by_name(self, source_id, dest_id, name):
        await self._wait_for_sleep()

        params = {"supportsAllDrives": "true"}
        data = {"name": name, "parents": [dest_id]}
        headers = await self._get_headers()

        r = await ahttpx.post(
            DRIVE_URL + "/" + source_id + "/copy",
            headers=headers,
            json=data,
            params=params,
        )
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self._handle_status_error(e)

        return r.json()

    async def copy_file_by_name(self, source_id, dest_id, name):
        exist_files = (
            await self.list_files_by_name(
                dest_id, name, is_folder=False, fields=("files/id",)
            )
        ).get("files", [])

        if exist_files:
            return True
        else:
            return await self._copy_file_by_name(source_id, dest_id, name)

    async def _get_upload_url(self, parent_id, name):
        await self._wait_for_sleep()

        params = {"uploadType": "resumable", "supportsAllDrives": "true"}
        data = {"name": name, "parents": [parent_id]}
        headers = await self._get_headers()

        r = await ahttpx.post(
            DRIVE_UPLOAD_URL, headers=headers, json=data, params=params
        )
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self._handle_status_error(e)

        return r.headers["Location"]

    async def get_upload_url(self, parent_id, name):
        exist_files = (
            await self.list_files_by_name(
                parent_id, name, is_folder=False, fields=("files/id",)
            )
        ).get("files", [])

        if exist_files:
            return True
        else:
            return await self._get_upload_url(parent_id, name)

    async def get_download_info(self, file_id):
        url = DRIVE_URL + "/" + file_id
        params = {"alt": "media", "supportsAllDrives": "true"}
        headers = await self._get_headers()
        return url, params, headers


class GoogleDriveFile:
    def __init__(self, file_id, relative_path, size, client):
        self.file_id = file_id
        self.relative_path = relative_path
        self.size = size
        self.client = client

    async def get_download_info(self):
        url, params, headers = await self.client.get_download_info(self.file_id)
        return url + parse_params(params), headers

    def get_relative_path(self):
        return self.relative_path

    def get_size(self):
        return self.size

    async def get_reader(self, start=0, end=None):
        url, params, headers = await self.client.get_download_info(self.file_id)
        return HttpFileReader(
            url,
            params=params,
            headers=headers,
            data_range=(start, end or self.size, DOWNLOAD_CHUNK_SIZE),
            max_workers=MAX_DOWNLOAD_WORKERS,
        )


class GoogleDriveTask:
    def __init__(self, total_path, file, folder_id_future, name, client):
        self.total_path = total_path
        self.file = file
        self.folder_id_future = folder_id_future
        self.name = name
        self.client = client

        self.bar = None

        self._upload_url = None
        self._uploaded_bytes = 0

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    async def _upload_single_chunk(self, reader, start):
        end = min(start + CHUNK_SIZE, self.file.get_size())
        length = end - start

        headers = {
            "Content-Range": f"bytes {start}-{end - 1}/{self.file.get_size()}",
            "Content-Length": str(length),
        }

        data = aiter_data(reader, self.bar.update, STEP_SIZE, length=length)

        r = await ahttpx.put(self._upload_url, data=data, headers=headers)
        try:
            raise_for_status(r)
        except HttpStatusError as e:
            self.client._handle_status_error(e)

        if r.status_code == 308:
            header_range = r.headers.get("Range")
            if header_range is None or header_range.lstrip("bytes=0-") != str(end - 1):
                raise TaskFailError(
                    path=self.total_path,
                    error_msg="Range was missed when uploading file.",
                )

        elif "id" not in r.json().keys():
            raise TaskFailError(
                path=self.total_path, error_msg="Upload was not successful."
            )
        else:
            self._uploaded_bytes += length

    async def run(self):
        try:
            folder_id = await self.folder_id_future

            if isinstance(self.file, GoogleDriveFile):
                copy_r = await self.client.copy_file_by_name(
                    self.file.file_id, folder_id, self.name
                )

                if copy_r is True:
                    raise TaskExistError(path=self.total_path)

                if "id" in copy_r:
                    self.bar.update(self.file.get_size())
            else:
                if self._upload_url is None:
                    upload_url_r = await self.client.get_upload_url(
                        folder_id, self.name
                    )
                    if upload_url_r is True:
                        raise TaskExistError(path=self.total_path)

                    self._upload_url = upload_url_r

                async with (
                    await self.file.get_reader(start=self._uploaded_bytes)
                ) as reader:
                    for start in range(
                        self._uploaded_bytes, self.file.get_size(), CHUNK_SIZE
                    ):
                        await self._upload_single_chunk(reader, start)

        except TaskError as e:
            raise e

        except HttpStatusError as e:
            raise TaskFailError(path=self.total_path, error_msg=str(e), traceback=False)

        except Exception as e:
            raise TaskFailError(path=self.total_path, error_msg=type(e).__name__)

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path)


class GoogleDriveBase:
    _clients = []

    @classmethod
    def transport_factory(
        cls,
        path,
        token_path="",
        root_folder_id="root",
        service_account=False,
        drive_id="",
        credit={"client_id": "", "client_secret": ""},
    ):
        if os.path.exists(token_path):
            path = format_path(root_folder_id, path)
            clients = []

            if service_account:
                for credit_path in iter_path(token_path):
                    token_backend = GoogleServiceAccountTokenBackend(
                        credit_path=credit_path
                    )
                    client = GoogleDriveClient(
                        token_backend=token_backend, drive_id=drive_id
                    )
                    clients.append(client)
            else:
                token_backend = GoogleTokenBackend(token_path=token_path, credit=credit)
                client = GoogleDriveClient(
                    token_backend=token_backend, drive_id=drive_id
                )
                clients.append(client)

            random.shuffle(clients)

            clients = clients[: max(MAX_CLIENTS, 1)]

            return cls(path=path, clients=clients)
        else:
            raise Exception("Token path not exists")

    def _get_client(self):
        client = self._clients.pop(0)
        self._clients.append(client)
        return client


class GoogleDriveFiles(GoogleDriveBase):
    def __init__(self, path, clients):
        self._path = path
        self._clients = clients

        self._root, *base_path = self._path.split("/")
        self._base_path = format_path(*base_path)

    async def _get_base_item(self):
        client = self._get_client()

        if self._base_path:
            parent_id = self._root
            item = None
            paths = self._base_path.split("/")

            for i, name in enumerate(paths):
                items = (
                    await client.list_files_by_name(
                        parent_id,
                        name,
                        fields=(
                            "files/id",
                            "files/name",
                            "files/mimeType",
                            "files/size",
                        ),
                    )
                ).get("files", [])

                if not items:
                    now_path = "root:/" + format_path(*paths[:i])
                    raise FileNotFoundError(
                        f"No file or folder '{name}' found in '{now_path}'"
                    )
                item = items[0]

                if i == len(paths) - 1:
                    return item
                else:
                    parent_id = item["id"]
        else:
            return await client.get_file_by_id(
                self._root, fields=("id", "name", "mimeType", "size")
            )

    async def _list_items(self, item, page_token=None, client=None):
        client = client or self._get_client()

        params = {
            "q": f"'{item['id']}' in parents and trashed = false",
            "pageSize": MAX_PAGE_SIZE,
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
            params.update({"pageToken": page_token})

        resp = await client.list_files_by_params(params)

        folders = []
        for i in resp.get("files", []):
            i["name"] = format_path(item["name"], i["name"])

            if i["mimeType"] == FOLDER_MIMETYPE:
                folders.append(i)
            else:
                yield (i["id"], i["name"], int(i["size"]))

        page_token = resp.get("nextPageToken")
        if page_token:
            async for i in self._list_items(item, page_token):
                yield i

        for folder in folders:
            async for i in self._list_items(folder):
                yield i

    async def iter_file(self):
        base_item = await self._get_base_item()
        if base_item["mimeType"] != FOLDER_MIMETYPE:
            yield GoogleDriveFile(
                base_item["id"],
                base_item["name"],
                int(base_item["size"]),
                self._get_client(),
            )
        else:
            async for file_id, relative_path, size in self._list_items(base_item):
                yield GoogleDriveFile(file_id, relative_path, size, self._get_client())


class GoogleDriveTasks(GoogleDriveBase):
    def __init__(self, path, clients):
        self._path = path
        self._clients = clients

        self._root, *base_path = self._path.split("/")
        self._base_path = format_path(*base_path)

        self._loop = asyncio.get_event_loop()
        root_future = self._loop.create_future()
        root_future.set_result(self._root)

        self._dir_create = {"": root_future}

    def _create_folder_future(self, path):
        exist_future = self._dir_create.get(path)
        if exist_future:
            return exist_future
        else:
            future = self._loop.create_future()
            self._dir_create[path] = future

            async def set_folder_id():
                try:
                    parent_path, name = os.path.split(path)

                    parent_id_future = self._create_folder_future(parent_path)
                    parent_id = await parent_id_future

                    client = self._get_client()
                    resp = await client.create_file_by_name(
                        parent_id, name, is_folder=True
                    )

                    folder_id = resp["id"]
                    future.set_result(folder_id)
                except Exception as e:
                    future.set_exception(e)

            asyncio.run_coroutine_threadsafe(set_folder_id(), self._loop)

            return future

    async def get_task(self, file):
        total_path = format_path(self._base_path, file.get_relative_path())
        base_path, name = os.path.split(total_path)
        folder_id_future = self._create_folder_future(base_path)

        client = self._get_client()
        return GoogleDriveTask(total_path, file, folder_id_future, name, client)
