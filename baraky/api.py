import logging
import math
import aiohttp
import asyncio

from typing import List

logger = logging.getLogger("baraky.api")


class SrealityException(Exception):
    pass


class SrealityApi:
    def __init__(self, url_base="https://www.sreality.cz/api/cs/v2/"):
        self.url_base = url_base

    async def query(
        self,
        query_params=None,
        per_page: int = 100,
        headers: dict = {},
    ) -> List[dict]:
        if query_params is None:
            query_params = {}

        async with aiohttp.ClientSession() as session:
            page_1 = await self._read_page(
                session, query_params, per_page=per_page, headers=headers
            )
            result_size = page_1["result_size"]
            tasks = []
            pages_total = math.ceil(result_size / per_page) + 1
            for page in range(2, pages_total):
                task = self._read_page(
                    session, query_params, page=page, per_page=per_page
                )
                tasks.append(task)
            page_dicts = await asyncio.gather(*tasks, return_exceptions=True)
            dicts_list = [parse_query_result_page(p) for p in page_dicts]
            return sum(dicts_list, [])

    async def detail(self, id: int) -> dict:
        async with aiohttp.ClientSession() as session:
            return await self._detail_with_session(session, id)

    async def details(self, ids: List[int]) -> List[dict]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for id in ids:
                task = self._detail_with_session(session, id)
                tasks.append(task)
            return await asyncio.gather(*tasks, return_exceptions=True)

    async def _detail_with_session(self, session, id: int) -> dict:
        url = format_url(self.url_base,f"estates/{id}")
        return await _request_json(session, url)

    async def _read_page(
        self,
        session: aiohttp.ClientSession,
        query_params: dict,
        page: int = 1,
        per_page: int = 100,
        headers: dict = {},
    ) -> dict:
        paged_query = page_query(query_params, page, per_page)
        url = format_url(self.url_base, "estates", paged_query)
        return await _request_json(session, url, headers=headers)



async def _request_json(session, url, method="get", headers={}):
    if "User-Agent" not in headers:
        headers["User-Agent"] = "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"

    async with session.request(method, url, headers=headers) as resp:
        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError as e:
            logger.error(
                f"Failed to get %s with status %s error %s", url, resp.status, e
            )
            return {}
        return await resp.json()


def _to_query_string(query_params):
    return "&".join([f"{k}={v}" for k, v in query_params.items()])


def format_url(url_base, path, query_params={}):
    query = _to_query_string(query_params)
    return f"{url_base}{path}?{query}"


def page_query(query_params, page, per_page):
    return query_params | {"per_page": per_page, "page": page}


def parse_query_result_page(page_dict: dict):
    return page_dict.get("_embedded", {}).get("estates", [])
