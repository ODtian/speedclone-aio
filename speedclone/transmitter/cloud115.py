import asyncio
import base64
import datetime
import hashlib
import hmac

# import logging
import os
import time
from urllib import parse

import httpx
import xmltodict

from .. import ahttpx
from ..args import args_dict
from ..error import HttpStatusError, TaskFailError, TaskNotDoneError
from ..utils import (
    aiter_data,
    format_path,
    get_gmtdatetime,
    parse_headers,
    parse_params,
    utc_to_datetime,
    raise_for_status,
)

from ..filereader import HttpFileReader

# UPLOAD_INFO_URL = "https://uplb.115.com/3.0/getuploadinfo.php"
USER_INFO_URL = "https://proapi.115.com/app/uploadinfo"
INIT_UPLOAD_URL = "https://uplb.115.com/3.0/initupload.php"
LIST_FILE_URL = "https://webapi.115.com/files"
CREATE_FOLDER_URL = "https://webapi.115.com/files/add"
DOWNLOAD_URL = "https://webapi.115.com/files/download"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36 115Browser/24.0.0.34"


CHUNK_SIZE = args_dict["CHUNK_SIZE"]
STEP_SIZE = args_dict["STEP_SIZE"]
DOWNLOAD_CHUNK_SIZE = args_dict["DOWNLOAD_CHUNK_SIZE"]
MAX_PAGE_SIZE = args_dict["MAX_PAGE_SIZE"]


class OSSTokenBackend:
    def __init__(self, token_url):
        self.token_url = token_url

        self.token = None
        self.expired_time = None

    def _token_expired(self):
        if self.token:
            return self.expired_time <= datetime.datetime.utcnow()
        else:
            return True

    async def _refresh_token(self):
        r = await ahttpx.get(self.token_url)
        raise_for_status(r)

        self.token = r.json()
        self.expired_time = utc_to_datetime(self.token["Expiration"])

    async def get_token(self):
        if self._token_expired():
            await self._refresh_token()
        return self.token


class OSSClient:
    def __init__(self, token_backend, endpoint):
        self.token_backend = token_backend
        self.endpoint = endpoint

    def get_signature(
        self, key_secret, method, now_gmt_time, fmt_headers, fmt_resources
    ):
        signature_str = "\n".join(
            [method, "", "", now_gmt_time, fmt_headers, fmt_resources]
        )

        h = hmac.new(
            key_secret.encode(encoding="utf-8"),
            signature_str.encode(encoding="utf-8"),
            hashlib.sha1,
        )

        return base64.b64encode(h.digest()).decode("utf-8")

    async def get_oss_headers(
        self,
        method,
        x_oss_headers={},
        oss_resources={"resources": [], "subresources": {}},
    ):

        oss_token = await self.token_backend.get_token()
        x_oss_headers.update({"x-oss-security-token": oss_token["SecurityToken"]})

        now_gmt_time = get_gmtdatetime()

        fmt_headers = parse_headers(x_oss_headers)

        fmt_resources = (
            "/"
            + "/".join(oss_resources["resources"])
            + parse_params(oss_resources["subresources"])
        )

        sign = self.get_signature(
            oss_token["AccessKeySecret"],
            method,
            now_gmt_time,
            fmt_headers,
            fmt_resources,
        )

        headers = {
            "Authorization": f"OSS {oss_token['AccessKeyId']}:{sign}",
            "Date": now_gmt_time,
        }
        headers.update(x_oss_headers)
        return headers


class OSSSimpleUpload:
    def __init__(self, oss_client, oss_resources, url):
        self.oss_client = oss_client
        self.oss_resources = oss_resources
        self.url = url

    async def upload(self, data, callback, callback_var):
        headers = await self.oss_client.get_oss_headers(
            method="PUT",
            x_oss_headers={
                "x-oss-callback": callback,
                "x-oss-callback-var": callback_var,
            },
            oss_resources=self.oss_resources,
        )
        r = await ahttpx.put(self.url, headers=headers, data=data)
        raise_for_status(r)


class OSSMultipartUpload:
    def __init__(self, oss_client, oss_resources, url):
        self.oss_client = oss_client
        self.oss_resources = oss_resources
        self.url = url

        self.upload_id = None
        self.parts = []

        self.uploaded_bytes = 0

    async def init_multipart_upload(self):
        params = self.oss_resources["subresources"] = {"uploads": ""}
        headers = await self.oss_client.get_oss_headers(
            method="POST",
            oss_resources=self.oss_resources,
        )

        r = await ahttpx.post(self.url, headers=headers, params=params)
        raise_for_status(r)

        self.upload_id = xmltodict.parse(r.text)["InitiateMultipartUploadResult"][
            "UploadId"
        ]

    async def upload_part(self, part_number, data):
        params = self.oss_resources["subresources"] = {
            "partNumber": part_number,
            "uploadId": self.upload_id,
        }
        headers = await self.oss_client.get_oss_headers(
            method="PUT", oss_resources=self.oss_resources
        )

        r = await ahttpx.put(self.url, headers=headers, params=params, data=data)
        raise_for_status(r)

        self.parts.append({"PartNumber": part_number, "ETag": r.headers["ETag"]})

    async def complete_upload(self, callback, callback_var):
        params = self.oss_resources["subresources"] = {"uploadId": self.upload_id}
        headers = await self.oss_client.get_oss_headers(
            method="POST",
            x_oss_headers={
                "x-oss-callback": callback,
                "x-oss-callback-var": callback_var,
            },
            oss_resources=self.oss_resources,
        )
        data = xmltodict.unparse({"CompleteMultipartUpload": {"Part": self.parts}})

        r = await ahttpx.post(self.url, headers=headers, params=params, data=data)
        raise_for_status(r)


class Cloud115Client:
    def __init__(self, cookies, oss_client):
        self.headers = {
            "Cookie": cookies,
            "User-Agent": USER_AGENT,
        }
        self.user_id = self.user_key = None
        self.oss_client = oss_client

    def init_user_info(self):
        r = httpx.get(USER_INFO_URL, headers=self.headers)
        raise_for_status(r)

        user_info = r.json()
        try:
            self.user_id = str(user_info["user_id"])
            self.user_key = user_info["userkey"].upper()
        except KeyError:
            raise ValueError("Cookie has expired")

    async def upload_file_fast(self, cid, block_hash, total_hash, file_size, file_name):
        target = "U_1_" + cid

        id_sig = hashlib.sha1(
            "".join([self.user_id, total_hash, target, "0"]).encode("utf-8")
        ).hexdigest()

        sig = (
            hashlib.sha1("".join([self.user_key, id_sig, "000000"]).encode("utf-8"))
            .hexdigest()
            .upper()
        )

        id_md5 = hashlib.md5(self.user_id.encode("utf-8")).hexdigest()
        ts = int(time.time())
        token = hashlib.md5(
            "".join(
                [total_hash, str(file_size), block_hash, self.user_id, str(ts), id_md5]
            ).encode("utf-8")
        ).hexdigest()

        params = {
            "appid": 0,
            "appfrom": 12,
            "appversion": "2.0.0.0",
            "format": "json",
            "isp": 0,
            "sig": sig,
            "t": ts,
            "topupload": 0,
            "rt": 0,
            "token": token,
        }

        data = {
            "api_version": "2.0.0.0",
            "fileid": total_hash,
            "filename": file_name,
            "filesize": file_size,
            "preid": block_hash,
            "target": target,
            "userid": self.user_id,
        }

        r = await ahttpx.post(
            INIT_UPLOAD_URL, params=params, headers=self.headers, data=data
        )
        raise_for_status(r)

        json_resp = r.json()

        if json_resp["status"] == 2:
            return True

        status_code = json_resp["statuscode"]
        if status_code != 0:
            raise HttpStatusError(r, int(f"1{status_code}"))

        callback = json_resp["callback"]

        b64_callback = base64.b64encode(callback["callback"].encode("utf-8")).decode(
            "utf-8"
        )
        b64_callback_var = base64.b64encode(
            callback["callback_var"].encode("utf-8")
        ).decode("utf-8")

        return json_resp["bucket"], json_resp["object"], b64_callback, b64_callback_var

    def create_upload(self, bucket_name, object_name, is_simple=True):
        oss_resources = {"resources": [bucket_name, object_name], "subresources": {}}
        url = f"https://{bucket_name}.{self.oss_client.endpoint}/{object_name}"

        if is_simple:
            return OSSSimpleUpload(
                oss_client=self.oss_client, oss_resources=oss_resources, url=url
            )
        else:
            return OSSMultipartUpload(
                oss_client=self.oss_client, oss_resources=oss_resources, url=url
            )

    async def list_files(self, cid, offset=0):
        params = {
            "cid": cid,
            "aid": 1,
            "limit": MAX_PAGE_SIZE,
            "show_dir": 1,
            "offset": offset,
        }
        r = await ahttpx.get(LIST_FILE_URL, headers=self.headers, params=params)
        raise_for_status(r)

        return r.json()

    async def create_folder(self, pid, name):
        data = {"pid": pid, "cname": name}
        r = await ahttpx.post(CREATE_FOLDER_URL, headers=self.headers, data=data)
        raise_for_status(r)

        json_resp = r.json()
        if json_resp.get("errno") == 20004:
            files = await self.list_files(cid=pid)
            return list(
                filter(
                    lambda f: f["n"] == name and f.get("fid") is None,
                    files["data"],
                )
            )[0]["cid"]
        else:
            return json_resp["cid"]

    async def get_download_info(self, pick_code):
        headers = {**self.headers}
        params = {"pickcode": pick_code, "_": int(time.time() * 1000)}
        r = await ahttpx.get(DOWNLOAD_URL, headers=headers, params=params)
        raise_for_status(r)
        print(r.text)
        download_url = r.json()["file_url"].replace("http", "https")
        download_headers = {
            "Origin": "https://115.com",
            "Referer": "https://115.com/",
            "User-Agent": USER_AGENT,
            # "Cookie": r.headers["Set-Cookie"].split(";")[0],
        }

        return download_url, download_headers


class Cloud115File:
    def __init__(
        self,
        relative_path,
        size,
        client,
        pick_code=None,
        total_hash=None,
        block_hash=None,
    ):
        self.total_hash = total_hash
        self.block_hash = block_hash
        self.pick_code = pick_code

        self.relative_path = relative_path
        self.size = size
        self.client = client

    async def get_download_info(self):
        url, headers = await self.client.get_download_info(self.pick_code)
        return url, headers

    def get_relative_path(self):
        return self.relative_path

    def get_size(self):
        return self.size

    async def get_reader(self, start=0, end=None):
        url, headers = await self.client.get_download_info(self.pick_code)
        return HttpFileReader(
            url,
            headers=headers,
            data_range=(start, end or self.size, DOWNLOAD_CHUNK_SIZE),
            max_workers=2,
        )


class Cloud115Task:
    def __init__(self, total_path, file, cid_future, client):
        self.total_path = total_path
        self.file = file
        self.cid_future = cid_future
        self.client = client

        self._block_hash = None
        self._total_hash = None
        self.bar = None

    def set_bar(self, bar):
        if self.bar is None:
            self.bar = bar
            self.set_bar_info()

    def set_bar_info(self):
        self.bar.set_info(file_size=self.file.get_size(), total_path=self.total_path)

    async def hash_file(self):
        is_115file = isinstance(self.file, Cloud115File)

        if is_115file and hasattr(self.file, "block_hash"):
            block_hash = self.file.block_hash
        elif self._block_hash:
            block_hash = self._block_hash
        else:
            async with (await self.file.get_reader(end=1024 * 128)) as reader:
                block_hash = self._block_hash = (
                    hashlib.sha1(await reader.read(1024 * 128)).hexdigest().upper()
                )

        if is_115file and hasattr(self.file, "total_hash"):
            total_hash = self.file.total_hash
        elif self._total_hash:
            total_hash = self._total_hash
        else:
            total_sha1 = hashlib.sha1()
            async with (await self.file.get_reader()) as reader:
                while True:
                    data = await reader.read(CHUNK_SIZE)
                    if not data:
                        break
                    total_sha1.update(data)

            total_hash = self._total_hash = total_sha1.hexdigest().upper()

        return block_hash, total_hash

    async def run(self):
        try:
            cid = await self.cid_future
            block_hash, total_hash = await self.hash_file()
            file_size = self.file.get_size()
            file_name = os.path.basename(self.file.get_relative_path())

            upload_fast = await self.client.upload_file_fast(
                cid=cid,
                block_hash=block_hash,
                total_hash=total_hash,
                file_size=file_size,
                file_name=file_name,
            )

            if upload_fast is True:
                self.bar.update(file_size)
                return

            bucket_name, object_name, callback, callback_var = upload_fast

            if file_size <= 100 * 1024:

                oss_upload = self.client.create_upload(
                    bucket_name, object_name, is_simple=True
                )

                async with (await self.file.get_reader()) as reader:
                    data = aiter_data(reader, self.bar.update, STEP_SIZE)
                    await oss_upload.upload(
                        data=data, callback=callback, callback_var=callback_var
                    )

            else:
                oss_upload = self.client.create_upload(
                    bucket_name, object_name, is_simple=False
                )

                await oss_upload.init_multipart_upload()

                async with (
                    await self.file.get_reader(start=oss_upload.uploaded_bytes)
                ) as reader:
                    for i, start in enumerate(
                        range(
                            oss_upload.uploaded_bytes, self.file.get_size(), CHUNK_SIZE
                        ),
                        len(oss_upload.parts) + 1,
                    ):
                        length = min(start + CHUNK_SIZE, self.file.get_size()) - start
                        await oss_upload.upload_part(
                            part_number=i,
                            data=aiter_data(
                                reader, self.bar.update, STEP_SIZE, length=length
                            ),
                        )
                        oss_upload.uploaded_bytes += length

        except HttpStatusError as e:
            raise TaskFailError(
                path=self.total_path,
                task=self,
                error_msg=str(e),
                task_exit=e.status_code == 1414,
                traceback=False,
            )

        except Exception as e:
            raise TaskFailError(
                path=self.total_path, task=self, error_msg=type(e).__name__
            )

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path, task=self)


class Cloud115Base:
    @classmethod
    def get_trans(cls, path, config):
        endpoint = parse.urlparse(config["oss_endpoint"]).netloc

        oss_token_backend = OSSTokenBackend(token_url=config["oss_token_url"])
        oss_client = OSSClient(endpoint=endpoint, token_backend=oss_token_backend)
        client = Cloud115Client(cookies=config["cookies"], oss_client=oss_client)
        client.init_user_info()

        return cls(path=path, client=client)


class Cloud115Files(Cloud115Base):
    def __init__(self, path, client):
        self.source_path = path
        self.client = client

        self.nodes = self.source_path.split("/")
        self.nodes_len = len(self.nodes)

    async def _get_base_objects(self):
        cid = "0"

        for i, n in enumerate(self.nodes, 1):
            files = await self.client.list_files(cid=cid)
            files_same_name = tuple(filter(lambda f: f["n"] == n, files["data"]))

            if i != self.nodes_len:
                next_folders = filter(lambda f: f.get("fid") is None, files_same_name)
                cid = tuple(next_folders)[0]["cid"]
            else:
                result = (
                    [
                        (f["pc"], f["sha"], f["n"], f["s"])
                        for f in files_same_name
                        if f.get("fid") is not None
                    ],
                    [f["cid"] for f in files_same_name if f.get("fid") is None],
                )
                return result

        raise ValueError("No such file.")

    async def _list_files(self, cid, offset=0):
        data = await self.client.list_files(cid, offset=offset)

        base_path = format_path(*[p["name"] for p in data["path"][self.nodes_len :]])
        dirs = []

        for f in data["data"]:
            if f.get("fid") is None:
                dirs.append(f)
            else:
                yield (f["pc"], f["sha"], format_path(base_path, f["n"]), f["s"])

        files_length = len(data["data"])
        now_seek = files_length + offset

        if now_seek < data["count"]:
            async for f in self._list_files(cid, offset=now_seek):
                yield f

        for d in dirs:
            async for f in self._list_files(d["cid"]):
                yield f

    async def iter_file(self):
        files, folders = await self._get_base_objects()

        for pick_code, sha1, name, size in files:
            yield Cloud115File(
                name, size, self.client, pick_code=pick_code, total_hash=sha1
            )

        for cid in folders:
            async for pick_code, sha1, relative_path, size in self._list_files(cid=cid):
                yield Cloud115File(
                    relative_path,
                    size,
                    self.client,
                    pick_code=pick_code,
                    total_hash=sha1,
                )


class Cloud115HashFiles(Cloud115Base):
    def __init__(self, path, client):
        self.source_path = path
        self.client = client

        self.files = []
        if os.path.isfile(self.source_path):
            with open(self.source_path, "r", encoding="utf-8") as f:
                for l in f:

                    self.files.append(self._split_link(l.strip()))
        else:
            self.files.append(self._split_link(self.source_path))

    def _split_link(self, link):
        name, size, total_hash, block_hash = link.lstrip("115://").split("|")
        return name, int(size), total_hash, block_hash

    async def iter_file(self):
        for name, size, total_hash, block_hash in self.files:
            yield Cloud115File(
                name,
                size,
                self.client,
                total_hash=total_hash,
                block_hash=block_hash,
            )


class Cloud115Tasks(Cloud115Base):
    def __init__(self, path, client):
        self.target_path = path
        self.client = client

        self.loop = asyncio.get_event_loop()

        root = self.loop.create_future()
        root.set_result("0")

        self.dir_create = {"": root}

    def _create_folder_future(self, path):
        exist_future = self.dir_create.get(path)
        if exist_future:
            return exist_future
        else:
            future = self.loop.create_future()
            self.dir_create[path] = future

            async def set_folder_id():
                parent_path, name = os.path.split(path)
                parent_id_future = self._create_folder_future(parent_path)
                parent_id = await parent_id_future
                cid = await self.client.create_folder(parent_id, name)
                future.set_result(cid)

            asyncio.run_coroutine_threadsafe(set_folder_id(), self.loop)

            return future

    async def get_task(self, file):
        total_path = format_path(self.target_path, file.get_relative_path())
        base_path, _ = os.path.split(total_path)
        folder_id_future = self._create_folder_future(base_path)
        return Cloud115Task(total_path, file, folder_id_future, self.client)
