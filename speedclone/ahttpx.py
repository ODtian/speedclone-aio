from httpx import AsyncClient
from .manager import on_close_callbacks
from .args import args_dict

PROXY = args_dict["PROXY"]
# sock_transport = None

# try:
#     from httpx_socks import AsyncProxyTransport
# except ImportError:
#     import logging

#     logging.warning(
#         "httpx-socks hasn't installed yet, it might cause some problems when using socks proxies."
#     )
# else:
#     socks_transport = AsyncProxyTransport


# def get_socks_proxies(proxies):
#     if isinstance(proxies, str) and proxies.startswith("socks"):
#         return proxies

#     elif isinstance(proxies, dict):
#         socks_proxies = [v for v in proxies.values() if v.startswith("socks")]

#         if socks_proxies:
#             return socks_proxies[0]


# def create_

# def create_client(cert=None, verify=True, timeout=None, trust_env=True, proxies=None):
#     transport = None
#     socks_proxies = get_socks_proxies(proxies)
#     if socks_proxies:
#         transport = socks_transport.from_url(socks_proxies)
#         proxies = None

#     return AsyncClient(
#         cert=cert,
#         verify=verify,
#         timeout=timeout,
#         trust_env=trust_env,
#         proxies=proxies,
#         transport=transport,
#     )


class Client:
    def __init__(self):
        self._client = None
        self.current_client_args = {}

    async def close(self):
        if self._client is not None:
            await self._client.aclose()

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
    ):
        client_args = (cert, verify, timeout, trust_env, proxies)

        if client_args != self.current_client_args:
            self.current_client_args = client_args

            if self._client:
                await self._client.aclose()

            self._client = AsyncClient(
                cert=cert,
                verify=verify,
                timeout=timeout,
                trust_env=trust_env,
                proxies=proxies or PROXY,
            )

        return await self._client.request(
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
on_close_callbacks.append(client.close())


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
