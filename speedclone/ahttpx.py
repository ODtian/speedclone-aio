from httpx import AsyncClient


class Client:
    def __init__(self):
        self._client = None
        self.current_client_args = {}

    def _create_client(
        self, cert=None, verify=True, timeout=None, trust_env=True, proxies=None
    ):
        return AsyncClient(
            cert=cert,
            verify=verify,
            timeout=timeout,
            trust_env=trust_env,
            proxies=proxies,
        )

    async def request(
        self,
        method: str,
        url,
        *,
        params=None,
        data=None,
        files=None,
        json=None,
        headers=None,
        cookies=None,
        auth=None,
        timeout=None,
        allow_redirects=True,
        verify=True,
        cert=None,
        trust_env=True,
        proxies=None,
        stream=False
    ):

        client_args = {
            "cert": cert,
            "verify": verify,
            "timeout": timeout,
            "trust_env": trust_env,
            "proxies": proxies,
        }

        if client_args != self.current_client_args:

            if self._client:
                await self._client.__aexit__()

            self.current_client_args = client_args
            self._client = self._create_client(**self.current_client_args)

        return await getattr(self._client, "stream" if stream else "request")(
            method=method,
            url=url,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            allow_redirects=allow_redirects,
        )


client = Client()

# class client:
#     @classmethod
#     async def request(
#         cls,
#         method: str,
#         url,
#         *,
#         params=None,
#         data=None,
#         files=None,
#         json=None,
#         headers=None,
#         cookies=None,
#         auth=None,
#         timeout=None,
#         allow_redirects=True,
#         verify=True,
#         cert=None,
#         trust_env=True,
#         proxies=None,
#         stream=False
#     ):
#         async with AsyncClient(
#             cert=cert,
#             verify=verify,
#             timeout=timeout,
#             trust_env=trust_env,
#             proxies=proxies,
#         ) as client:
#             return await getattr(client, "stream" if stream else "request")(
#                 method=method,
#                 url=url,
#                 data=data,
#                 files=files,
#                 json=json,
#                 params=params,
#                 headers=headers,
#                 cookies=cookies,
#                 auth=auth,
#                 allow_redirects=allow_redirects,
#             )


async def get(
    url,
    *,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "GET",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def options(
    url,
    *,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "OPTIONS",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def head(
    url,
    *,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=False,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "HEAD",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def post(
    url,
    *,
    data=None,
    files=None,
    json=None,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "POST",
        url,
        data=data,
        files=files,
        json=json,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def put(
    url,
    *,
    data=None,
    files=None,
    json=None,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "PUT",
        url,
        data=data,
        files=files,
        json=json,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def patch(
    url,
    *,
    data=None,
    files=None,
    json=None,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "PATCH",
        url,
        data=data,
        files=files,
        json=json,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )


async def delete(
    url,
    *,
    params=None,
    headers=None,
    cookies=None,
    auth=None,
    allow_redirects=True,
    cert=None,
    verify=True,
    timeout=None,
    trust_env=True,
    proxies=None,
    stream=False
):
    return await client.request(
        "DELETE",
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        auth=auth,
        allow_redirects=allow_redirects,
        cert=cert,
        verify=verify,
        timeout=timeout,
        trust_env=trust_env,
        proxies=proxies,
    )
