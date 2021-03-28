import datetime
import os


def format_path(*path):
    formated_path = [
        os.path.normpath(p).replace("\\", "/").strip("/") for p in path if p
    ]
    return "/".join(formated_path)


def iter_path(path):
    if os.path.isfile(path):
        yield path
    else:
        for root, _, files in os.walk(path):
            for filename in files:
                yield os.path.join(root, filename)


async def aenumerate(asequence, start=0):
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1


async def aiter_data(reader, update, step_size, length=None):
    if length is None:
        while True:
            data = await reader.read(step_size)
            if not data:
                break
            yield data
            update(len(data))
    else:
        while length > 0:
            data_len = min(length, step_size)
            data = await reader.read(step_size)
            yield data
            length -= data_len
            update(data_len)


GMT_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_gmtdatetime():
    return datetime.datetime.utcnow().strftime(GMT_FORMAT)


def utc_to_datetime(utc_string):
    return datetime.datetime.strptime(utc_string, UTC_FORMAT)


def parse_headers(headers, join=":"):
    return "\n".join([k + join + v for k, v in headers.items()])


def parse_params(params):
    parsed_params = "&".join([f"{k}{'=' if v else ''}{v}" for k, v in params.items()])
    if parsed_params:
        parsed_params = "?" + parsed_params
    return parsed_params


def parse_cookies(cookies):
    return "; ".join([f"{k}={v}" for k, v in cookies.items()])


def parse_ranges(ranges):
    return "bytes=" + ", ".join([f"{start}-{end}" for start, end in ranges])


def raise_for_status(response):
    error_code = response.status_code // 100
    if error_code == 4 or error_code == 5:
        from .error import HttpStatusError

        raise HttpStatusError(response=response)
