import os
from urllib import parse

import httpx

from .. import ahttpx
from ..args import Args
from ..error import FileListError, HttpStatusError
from ..filereader import HttpFileReader
from ..utils import format_path, parse_cookies, raise_for_status

DOWNLOAD_URL = "https://{tenant_name}/personal/{account_name}/_layouts/15/download.aspx?UniqueId={unique_id}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36 Edg/87.0.664.41"
STREAM_LIST_DATA = {
    "parameters": {
        "__metadata": {"type": "SP.RenderListDataParameters"},
        "RenderOptions": 5707527,
        "AllowMultipleValueFilterForTaxonomyFields": True,
        "AddRequiredFields": True,
    }
}


class OnedriveShareFile:
    def __init__(self, download_url, relative_path, size, cookies):
        self.download_url = download_url
        self.relative_path = relative_path
        self.size = size
        self.cookies = parse_cookies(cookies)

    async def get_download_info(self):
        return self.download_url, {"Cookie": self.cookies}

    def get_relative_path(self):
        return self.relative_path

    def get_size(self):
        return self.size

    async def get_reader(self, start=0, end=None):
        return HttpFileReader(
            self.download_url,
            headers={"Cookie": self.cookies},
            data_range=(start, end or self.size, Args.DOWNLOAD_CHUNK_SIZE),
            max_workers=Args.MAX_DOWNLOAD_WORKERS,
        )


class OnedriveShareFiles:
    def __init__(self, path, cookies, tenant_name, account_name):
        self._path = path
        self._cookies = cookies
        self._tenant_name = tenant_name
        self._account_name = account_name

    @classmethod
    def transport_factory(cls, path):
        parsed_url = parse.urlparse(path)
        tenant_name = parsed_url.netloc
        account_name = parsed_url.path.split("/")[4]

        r = httpx.get(path, headers={"User-Agent": USER_AGENT})
        raise_for_status(r)

        cookies = r.cookies

        base_path = format_path(
            *parse.unquote(
                r.history[0]  # 获取302请求
                .headers["Location"]  # 获取重定向URL
                .split("/")[7]  # 获取分享链接文件的路径
                .split("&")[0]
                .lstrip("onedrive.aspx?id=")
            ).split("/")[4:]
        )

        return cls(
            path=base_path,
            cookies=cookies,
            tenant_name=tenant_name,
            account_name=account_name,
        )

    async def _list_items(self, ref_path):
        url = f"https://{self._tenant_name}/personal/{self._account_name}/_api/web/GetListUsingPath(DecodedUrl=@a1)/RenderListDataAsStream"

        params = {
            "@a1": f"'/personal/{self._account_name}/Documents'",
            "RootFolder": f"/personal/{self._account_name}/Documents/{ref_path}",
            "TryNewExperienceSingle": "True",
        }

        headers = {"Content-Type": "application/json; odata=verbose"}

        r = await ahttpx.post(
            url,
            params=params,
            headers=headers,
            cookies=self._cookies,
            json=STREAM_LIST_DATA,
        )

        list_data = r.json()["ListData"]["Row"]

        folders = []

        for row in list_data:
            is_folder = row[".fileType"] == "" and row[".hasPdf"] == ""
            path = format_path(*row["FileRef"].split("/")[4:])

            if is_folder:
                folders.append(path)
            else:
                unique_id = row["UniqueId"].lstrip("{").rstrip("}")
                size = int(row["FileSizeDisplay"])
                yield unique_id, path, size

        for folder in folders:
            async for item in self._list_items(folder):
                yield item

    async def list_file(self):
        try:
            base_path, _ = os.path.split(self._path)
            async for unique_id, path, size in self._list_items(self._path):
                relative_path = path[len(base_path) :]
                download_url = DOWNLOAD_URL.format(
                    tenant_name=self._tenant_name,
                    account_name=self._account_name,
                    unique_id=unique_id,
                )
                yield OnedriveShareFile(
                    download_url=download_url,
                    relative_path=relative_path,
                    size=size,
                    cookies=self._cookies,
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
