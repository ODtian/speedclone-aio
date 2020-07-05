import random

from httpx import AsyncClient


class Client:
    def __init__(self, clients_size=10):
        self._client = []
        self.size = clients_size

    async def _create_client(
        self, cert=None, verify=True, timeout=None, trust_env=True, proxies=None
    ):
        async with AsyncClient(
            cert=cert,
            verify=verify,
            timeout=timeout,
            trust_env=trust_env,
            proxies=proxies,
        ) as client:
            return client

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
        if len(self._client) < self.size:
            client = await self._create_client(
                cert=cert,
                verify=verify,
                timeout=timeout,
                trust_env=trust_env,
                proxies=proxies,
            )
            self._client.append(client)

        return await getattr(
            random.choice(self._client), "stream" if stream else "request"
        )(
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


CLIENT = Client()


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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
    return await CLIENT.request(
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
