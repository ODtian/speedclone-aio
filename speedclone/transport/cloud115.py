import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import math
import os
import time
from urllib import parse

import httpx
import xmltodict
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.PublicKey import RSA

from .. import ahttpx
from ..args import Args
from ..error import FileListError, HttpStatusError, TaskFailError, TaskNotDoneError
from ..filereader import HttpFileReader
from ..utils import (
    aiter_data,
    format_path,
    get_gmtdatetime,
    parse_headers,
    parse_params,
    raise_for_status,
    utc_to_datetime,
)

# UPLOAD_INFO_URL = "https://uplb.115.com/3.0/getuploadinfo.php"
USER_INFO_URL = "https://proapi.115.com/app/uploadinfo"
INIT_UPLOAD_URL = "https://uplb.115.com/3.0/initupload.php"
LIST_FILE_URL = "https://webapi.115.com/files"
CREATE_FOLDER_URL = "https://webapi.115.com/files/add"
DOWNLOAD_URL = "https://proapi.115.com/app/chrome/downurl"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36 115Browser/24.0.0.34"


PUBLIC_KEY = (
    "-----BEGIN RSA PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDR3rWmeYnRClwLBB0Rq0dlm8Mr\n"
    "PmWpL5I23SzCFAoNpJX6Dn74dfb6y02YH15eO6XmeBHdc7ekEFJUIi+swganTokR\n"
    "IVRRr/z16/3oh7ya22dcAqg191y+d6YDr4IGg/Q5587UKJMj35yQVXaeFXmLlFPo\n"
    "kFiz4uPxhrB7BGqZbQIDAQAB\n"
    "-----END RSA PUBLIC KEY-----"
)
PRIVATE_KEY = (
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICXAIBAAKBgQCMgUJLwWb0kYdW6feyLvqgNHmwgeYYlocst8UckQ1+waTOKHFC\n"
    "TVyRSb1eCKJZWaGa08mB5lEu/asruNo/HjFcKUvRF6n7nYzo5jO0li4IfGKdxso6\n"
    "FJIUtAke8rA2PLOubH7nAjd/BV7TzZP2w0IlanZVS76n8gNDe75l8tonQQIDAQAB\n"
    "AoGANwTasA2Awl5GT/t4WhbZX2iNClgjgRdYwWMI1aHbVfqADZZ6m0rt55qng63/\n"
    "3NsjVByAuNQ2kB8XKxzMoZCyJNvnd78YuW3Zowqs6HgDUHk6T5CmRad0fvaVYi6t\n"
    "viOkxtiPIuh4QrQ7NUhsLRtbH6d9s1KLCRDKhO23pGr9vtECQQDpjKYssF+kq9iy\n"
    "A9WvXRjbY9+ca27YfarD9WVzWS2rFg8MsCbvCo9ebXcmju44QhCghQFIVXuebQ7Q\n"
    "pydvqF0lAkEAmgLnib1XonYOxjVJM2jqy5zEGe6vzg8aSwKCYec14iiJKmEYcP4z\n"
    "DSRms43hnQsp8M2ynjnsYCjyiegg+AZ87QJANuwwmAnSNDOFfjeQpPDLy6wtBeft\n"
    "5VOIORUYiovKRZWmbGFwhn6BQL+VaafrNaezqUweBRi1PYiAF2l3yLZbUQJAf/nN\n"
    "4Hz/pzYmzLlWnGugP5WCtnHKkJWoKZBqO2RfOBCq+hY4sxvn3BHVbXqGcXLnZPvo\n"
    "YuaK7tTXxZSoYLEzeQJBAL8Mt3AkF1Gci5HOug6jT4s4Z+qDDrUXo9BlTwSWP90v\n"
    "wlHF+mkTJpKd5Wacef0vV+xumqNorvLpIXWKwxNaoHM=\n"
    "-----END RSA PRIVATE KEY-----"
)


class Secret:
    def __init__(self):
        self.keyS = b"\x29\x23\x21\x5E"
        self.keyL = b"\x42\xDA\x13\xBA\x78\x76\x8D\x37\xE8\xEE\x04\x91"
        self.kts = b"\xF0\xE5\x69\xAE\xBF\xDC\xBF\x5A\x1A\x45\xE8\xBE\x7D\xA6\x73\x88\xDE\x8F\xE7\xC4\x45\xDA\x86\x94\x9B\x69\x92\x0B\x6A\xB8\xF1\x7A\x38\x06\x3C\x95\x26\x6D\x2C\x56\x00\x70\x56\x9C\x36\x38\x62\x76\x2F\x9B\x5F\x0F\xF2\xFE\xFD\x2D\x70\x9C\x86\x44\x8F\x3D\x14\x27\x71\x93\x8A\xE4\x0E\xC1\x48\xAE\xDC\x34\x7F\xCF\xFE\xB2\x7F\xF6\x55\x9A\x46\xC8\xEB\x37\x77\xA4\xE0\x6B\x72\x93\x7E\x51\xCB\xF1\x37\xEF\xAD\x2A\xDE\xEE\xF9\xC9\x39\x6B\x32\xA1\xBA\x35\xB1\xB8\xBE\xDA\x78\x73\xF8\x20\xD5\x27\x04\x5A\x6F\xFD\x5E\x72\x39\xCF\x3B\x9C\x2B\x57\x5C\xF9\x7C\x4B\x7B\xD2\x12\x66\xCC\x77\x09\xA6"
        self.public_cipher = Cipher_pkcs1_v1_5.new(RSA.importKey(PUBLIC_KEY))
        self.private_cipher = Cipher_pkcs1_v1_5.new(RSA.importKey(PRIVATE_KEY))

    def getkey(self, length, key):
        if key is not None:
            results = []

            i = j = 0
            ref = length
            while j < ref if ref >= 0 else j > ref:
                results.append(
                    ((key[i] + self.kts[length * i]) & 0xFF)
                    ^ self.kts[length * (length - 1 - i)]
                )

                if ref >= 0:
                    j += 1
                else:
                    j -= 1

                i = j
            return results

        if length == 12:
            return self.keyL

        return self.keyS

    def xor115Enc(self, src, srclen, key, keylen):
        mod4 = srclen % 4
        ret = []
        if mod4 != 0:
            i = j = 0
            ref = mod4
            while j < ref if ref >= 0 else j > ref:
                ret.append(src[i] ^ key[i % keylen])
                if ref >= 0:
                    j += 1
                else:
                    j -= 1
                i = j

        i = k = ref1 = mod4
        ref2 = srclen
        while k < ref2 if ref1 <= ref2 else k > ref2:
            ret.append(src[i] ^ key[(i - mod4) % keylen])

            if ref1 <= ref2:
                k += 1
            else:
                k -= 1
            i = k
        return ret

    def md5(self, data):
        obj = hashlib.md5()
        obj.update(data)
        return obj.hexdigest()

    def asymEncode(self, src, srclen):
        m = 128 - 11
        ret = b""
        i = j = 0
        ref = math.floor((srclen + m - 1) / m)

        while j < ref if ref >= 0 else j > ref:
            ret += self.public_cipher.encrypt(src[i * m : min((i + 1) * m, srclen)])

            if ref >= 0:
                j += 1
            else:
                j -= 1
            i = j
        return base64.b64encode(ret)

    def asymDecode(self, src, srclen):
        m = 128
        ret = b""
        i = j = 0
        ref = math.floor((srclen + m - 1) / m)
        while j < ref if ref >= 0 else j > ref:
            ret += self.private_cipher.decrypt(
                src[i * m : min((i + 1) * m, srclen)], sentinel="1"
            )

            if ref >= 0:
                j += 1
            else:
                j -= 1
            i = j
        return ret

    def symEncode(self, src, srclen, key1, key2):
        k1 = self.getkey(4, key1)
        k2 = self.getkey(12, key2)
        ret = self.xor115Enc(src, srclen, k1, 4)
        ret.reverse()
        ret = self.xor115Enc(ret, srclen, k2, 12)
        return ret

    def symDecode(self, src, srclen, key1, key2):
        k1 = self.getkey(4, key1)
        k2 = self.getkey(12, key2)
        ret = self.xor115Enc(src, srclen, k2, 12)
        ret.reverse()
        ret = self.xor115Enc(ret, srclen, k1, 4)
        return ret

    def encode(self, string, timestamp):
        key = self.md5(f"!@###@#{timestamp}DFDR@#@#".encode()).encode()
        temp = string.encode()
        temp = self.symEncode(temp, len(temp), key, None)
        temp = key[:16] + bytes(temp)
        return self.asymEncode(temp, len(temp)), key

    def decode(self, string, key):
        temp = base64.b64decode(string)
        temp = self.asymDecode(temp, len(temp))
        return bytes(self.symDecode(temp[16:], len(temp) - 16, key, temp[:16]))


secret_115 = Secret()


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
            (self.user_id + total_hash + target + "0").encode()
        ).hexdigest()

        sig = (
            hashlib.sha1((self.user_key + id_sig + "000000").encode())
            .hexdigest()
            .upper()
        )

        params = {
            "appid": 0,
            "appfrom": 10,
            "appversion": "2.0.0.0",
            "format": "json",
            "isp": 0,
            "sig": sig,
            "t": int(time.time()),
            "topupload": 0,
            "rt": 0,
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

        if status_code == 414:
            return True
        elif status_code != 0:
            raise HttpStatusError(r, status_code)

        callback = json_resp["callback"]

        b64_callback = base64.b64encode(callback["callback"].encode()).decode()
        b64_callback_var = base64.b64encode(callback["callback_var"].encode()).decode()

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
            "limit": Args.MAX_PAGE_SIZE,
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
        ts = int(time.time())

        headers = {"Cookie": self.headers["Cookie"]}
        params = {"t": ts}

        encrypt_data, key = secret_115.encode(json.dumps({"pickcode": pick_code}), ts)

        r = await ahttpx.post(
            DOWNLOAD_URL, headers=headers, params=params, data={"data": encrypt_data}
        )
        raise_for_status(r)

        decrypt_data = json.loads(secret_115.decode(r.json()["data"], key))
        download_url = tuple(decrypt_data.values())[0]["url"]["url"]

        return download_url, {}


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
            data_range=(start, end or self.size, Args.DOWNLOAD_CHUNK_SIZE),
            max_workers=2,
        )


class Cloud115Task:
    def __init__(self, total_path, file, cid_future, client):
        self.total_path = total_path
        self.file = file
        self.cid_future = cid_future
        self.client = client

        self.bar = None

        self._block_hash = None
        self._total_hash = None
        self._oss_upload = None

    def set_bar(self, bar):
        self.bar = bar
        self.bar.set_info(
            total=self.file.get_size(), content=f"Task '/{self.total_path}'"
        )

    async def _get_block_hash(self):
        if isinstance(self.file, Cloud115File) and self.file.block_hash:
            return self.file.block_hash
        elif self._block_hash:
            return self._block_hash
        else:
            async with (await self.file.get_reader(end=1024 * 128)) as reader:
                self._block_hash = (
                    hashlib.sha1(await reader.read(1024 * 128)).hexdigest().upper()
                )
                return self._block_hash

    async def _get_total_hash(self):
        if isinstance(self.file, Cloud115File) and self.file.total_hash:
            return self.file.total_hash
        elif self._total_hash:
            return self._total_hash
        else:
            sub_bar = self.bar.get_sub_bar()
            sub_bar.set_info(
                total=self.file.get_size(),
                content=f"Culcuating SHA1 '{os.path.basename(self.file.get_relative_path())}'",
            )

            total_sha1 = hashlib.sha1()
            async with (await self.file.get_reader()) as reader:
                while True:
                    data = await reader.read(1024)
                    if not data:
                        break
                    total_sha1.update(data)
                    sub_bar.update(len(data))

            self._total_hash = total_sha1.hexdigest().upper()
            sub_bar.disappear()

            return self._total_hash

    async def _upload_simple(self, bucket_name, object_name, callback, callback_var):
        oss_upload = self.client.create_upload(bucket_name, object_name, is_simple=True)

        async with (await self.file.get_reader()) as reader:
            data = aiter_data(
                reader, self.bar.update, Args.STEP_SIZE, self.file.get_size()
            )
            await oss_upload.upload(
                data=data, callback=callback, callback_var=callback_var
            )

    async def _upload_multipart(self, bucket_name, object_name, callback, callback_var):
        if not self._oss_upload:
            self._oss_upload = self.client.create_upload(
                bucket_name, object_name, is_simple=False
            )

        oss_upload = self._oss_upload

        await oss_upload.init_multipart_upload()

        async with (
            await self.file.get_reader(start=oss_upload.uploaded_bytes)
        ) as reader:
            self.bar.update(oss_upload.uploaded_bytes)

            upload_range = range(
                oss_upload.uploaded_bytes, self.file.get_size(), Args.CHUNK_SIZE
            )

            for i, start in enumerate(upload_range, len(oss_upload.parts) + 1):
                length = min(start + Args.CHUNK_SIZE, self.file.get_size()) - start

                await oss_upload.upload_part(
                    part_number=i,
                    data=aiter_data(reader, self.bar.update, Args.STEP_SIZE, length),
                )
                oss_upload.uploaded_bytes += length

    async def run(self):
        try:
            cid = await self.cid_future
            block_hash = await self._get_block_hash()
            total_hash = await self._get_total_hash()
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
                import logging

                logging.info("1")
                self.bar.update(file_size)
                return

            if file_size <= 100 * 1024:
                await self._upload_simple(*upload_fast)
            else:
                await self._upload_multipart(*upload_fast)

        except HttpStatusError as e:
            raise TaskFailError(
                path=self.total_path,
                error_msg="Bad response",
                extra_msg=e.build_raw_response(),
                traceback=False,
            )

        except Exception as e:
            raise TaskFailError(path=self.total_path, error_msg=type(e).__name__)

        else:
            if not self.bar.is_finished():
                raise TaskNotDoneError(path=self.total_path)


class Cloud115Base:
    @classmethod
    def transport_factory(
        cls,
        path,
        oss_token_url="https://uplb.115.com/3.0/gettoken.php",
        oss_endpoint="https://oss-cn-shenzhen.aliyuncs.com",
        cookies="",
    ):
        endpoint = parse.urlparse(oss_endpoint).netloc

        oss_token_backend = OSSTokenBackend(token_url=oss_token_url)
        oss_client = OSSClient(endpoint=endpoint, token_backend=oss_token_backend)
        client = Cloud115Client(cookies=cookies, oss_client=oss_client)
        client.init_user_info()

        return cls(path=path, client=client)


class Cloud115Files(Cloud115Base):
    def __init__(self, path, client):
        self._path = path
        self._client = client

        self._path_nodes = self._path.split("/")

    async def _get_base_items(self):
        cid = "0"

        for i, node in enumerate(self._path_nodes):
            items = await self._client.list_files(cid=cid)
            same_name_items = tuple(item for item in items["data"] if item["n"] == node)

            if not same_name_items:
                now_path = "root:/" + format_path(*self._path_nodes[:i])
                raise FileNotFoundError(
                    f"No file or folder '{node}' found in '{now_path}'"
                )

            folders = tuple(
                item["cid"] for item in same_name_items if item.get("fid") is None
            )

            if i == len(self._path_nodes) - 1:
                files = tuple(
                    (item["pc"], item["sha"], item["n"], item["s"])
                    for item in same_name_items
                    if item.get("fid") is not None
                )
                return files, folders

            else:
                cid = folders[0]

    async def _list_items(self, cid, offset=0):
        r = await self._client.list_files(cid, offset=offset)
        base_path = format_path(
            *[path["name"] for path in r["path"][len(self._path_nodes) :]]
        )

        data = r["data"]
        folders = []

        for item in data:
            if item.get("fid") is None:
                folders.append(item)
            else:
                yield (
                    item["pc"],
                    item["sha"],
                    format_path(base_path, item["n"]),
                    item["s"],
                )

        now_seek = len(data) + offset

        if now_seek < r["count"]:
            async for item in self._list_items(cid, offset=now_seek):
                yield item

        for folder in folders:
            async for item in self._list_items(folder["cid"]):
                yield item

    async def list_file(self):
        try:
            files, folders = await self._get_base_items()

            for pick_code, sha1, name, size in files:
                yield Cloud115File(
                    name, size, self._client, pick_code=pick_code, total_hash=sha1
                )

            for cid in folders:
                async for pick_code, sha1, relative_path, size in self._list_items(cid):
                    yield Cloud115File(
                        relative_path,
                        size,
                        self._client,
                        pick_code=pick_code,
                        total_hash=sha1,
                    )

        except HttpStatusError as e:
            raise FileListError(
                path=self._path,
                error_msg="Bad response",
                extra_msg=e.build_raw_response(),
                traceback=False,
            )
        except Exception as e:
            raise FileListError(self._path, type(e).__name__)


class Cloud115HashFiles(Cloud115Base):
    def __init__(self, path, client):
        self._path = path
        self._client = client

        self.files = []
        if os.path.isfile(self._path):
            with open(self._path, "r") as f:
                for line in f:
                    self.files.append(self._split_link(line.strip()))
        else:
            self.files.append(self._split_link(self._path))

    def _split_link(self, link):
        name, size, total_hash, block_hash = link.lstrip("115://").split("|")
        return name, int(size), total_hash, block_hash

    async def list_file(self):
        for name, size, total_hash, block_hash in self.files:
            yield Cloud115File(
                name,
                size,
                self._client,
                total_hash=total_hash,
                block_hash=block_hash,
            )


class Cloud115Tasks(Cloud115Base):
    def __init__(self, path, client):
        self._path = path
        self._client = client

        self._loop = asyncio.get_event_loop()

        self._folders = {}
        self._create_root_future()

    def _create_root_future(self):
        future = self._loop.create_future()
        future.set_result("0")
        self._folders[""] = future

    def _create_folder_future(self, path):
        exist_future = self._folders.get(path)
        if (
            exist_future
            and not exist_future.cancelled()
            and not (exist_future.done() and exist_future.exception())
        ):
            return exist_future
        else:
            future = self._loop.create_future()
            self._folders[path] = future

            async def set_folder_id():
                try:
                    parent_path, name = os.path.split(path)

                    parent_id_future = self._create_folder_future(parent_path)
                    parent_id = await parent_id_future
                    cid = await self._client.create_folder(parent_id, name)
                    future.set_result(cid)
                except Exception as e:
                    future.set_exception(e)

            asyncio.create_task(set_folder_id())
            return future

    async def get_task(self, file):
        total_path = format_path(self._path, file.get_relative_path())
        base_path, _ = os.path.split(total_path)
        folder_id_future = self._create_folder_future(base_path)
        return Cloud115Task(total_path, file, folder_id_future, self._client)
