import logging
import math
import aiohttp
import asyncio

from typing import List, Dict
from baraky import settings

logger = logging.getLogger("baraky.api")


class SrealityEstatesClient:
    def __init__(self, base_url: str | None = None):
        defaults = settings.SrealityClientSettings(
            base_url=base_url,
        )
        self.base_url = defaults.base_url

    async def query(
        self,
        query_params=None,
        per_page: int = 100,
        headers: dict = {},
    ) -> List[dict]:
        """
        Query the Sreality Api with the given query parameters

        :param query_params dict: query parameters dict
        :param per_page: number of items per page
        :param headers: optional http headers
        :return: list of estate overviews
        """

        if query_params is None:
            query_params = {}

        async with aiohttp.ClientSession() as session:
            page_1 = await self._read_page(
                session, query_params, per_page=per_page, headers=headers
            )
            if page_1 is None:
                logger.warning("Failed to get first page of the query")
                return []
            result_size = page_1["result_size"]
            pages_total = math.ceil(result_size / per_page) + 1
            tasks = []
            for page in range(2, pages_total):
                if page is None:
                    continue
                task = self._read_page(
                    session, query_params, page=page, per_page=per_page
                )
                tasks.append(task)
            page_dicts = await asyncio.gather(*tasks, return_exceptions=True)
            page_dicts.insert(0, page_1)
            dicts_list = [parse_query_result_page(p) for p in page_dicts]
            return sum(dicts_list, [])

    async def detail(self, id: int) -> dict:
        """
        Detail of the estate

        :param id: id of the estate
        :return: estate dict
        """
        async with aiohttp.ClientSession() as session:
            return await self._detail_with_session(session, id)

    async def details(self, ids: List[int]) -> List[dict]:
        """
        Details of the estates. This is a batch version of the detail method.
        It is faster then calling detail multiple times.

        :param ids: list of ids
        :return: list of estates
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for id in ids:
                task = self._detail_with_session(session, id)
                tasks.append(task)
            return await asyncio.gather(*tasks, return_exceptions=True)

    async def _detail_with_session(self, session, id: int) -> dict:
        url = format_url(self.base_url, f"estates/{id}")
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
        url = format_url(self.base_url, "estates", paged_query)
        return await _request_json(session, url, headers=headers)


async def _request_json(session, url, method="get", headers={}) -> Dict | None:
    if "User-Agent" not in headers:
        # Sreality returns random area and price if the user agent is not set
        headers["User-Agent"] = (
            "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
        )

    async with session.request(method, url, headers=headers) as resp:
        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError as e:
            logger.error(
                "Failed to get %s with status %s error %s", url, resp.status, e
            )
            return None
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
