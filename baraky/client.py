import logging
import math
import aiohttp
import asyncio

from typing import List, Dict
from baraky import settings

from baraky.models import EstateOverview
from pydantic import ValidationError

logger = logging.getLogger("baraky.client")

class SrealityEstatesClient:
    def __init__(
        self,
        query_params,
        headers: Dict = {},
        base_url: str | None = None,
        detail_url_template_house: str | None = None,
        detail_url_template_flat: str | None = None,
    ):
        defaults = settings.SrealityClientSettings(
            base_url=base_url
        )
        self.base_url = defaults.base_url
        self.detail_url_template_flat = detail_url_template_flat or defaults.detail_url_template_flat
        self.detail_url_template_house =  detail_url_template_house or defaults.detail_url_template_house
        self.per_page = defaults.per_page
        self.query_params = query_params
        if "User-Agent" not in headers:
            # Sreality returns random area and price if the user agent is not set
            headers["User-Agent"] = (
                "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
            )
        self.headers = headers


    async def read_all(self) -> List[EstateOverview]:
        try:
            async with aiohttp.ClientSession() as session:
                page_1 = await self._read_page(
                    session,
                    self.query_params,
                    page=1,
                    per_page=self.per_page,
                    headers=self.headers,
                )
                if page_1 is None:
                    logger.warning("Failed to get first page of the query")
                    return []
                result_size = page_1["result_size"]
                pages_total = math.ceil(result_size / self.per_page) + 1
                tasks = []
                for page in range(2, pages_total):
                    if page is None:
                        continue
                    task = self._read_page(
                        session,
                        self.query_params,
                        page=page,
                        per_page=self.per_page,
                    )
                    tasks.append(task)

                page_dicts = await asyncio.gather(*tasks, return_exceptions=True)
        except aiohttp.ClientConnectionError:
            logger.exception("Failed to connect to the server")
            return []

        page_dicts.insert(0, page_1)
        page_dicts = [p for p in page_dicts if p is not None]
        dicts_list = [parse_query_result_page(p) for p in page_dicts]
        records = sum(dicts_list, [])
        
        return self._map_to_model(records)

    def _map_to_model(self, records):
        valid = []
        for record in records:
            record_type = _parse_type(record)
            try:
                match record_type:
                    case "house":
                        estate_overview = EstateOverview.parse_house(
                            record,
                            self.detail_url_template_house,
                        )
                    case "flat":
                        estate_overview = EstateOverview.parse_flat(
                            record,
                            self.detail_url_template_flat,
                        )
                    case _:
                        raise ValueError("Encountered unknown listing case")
                valid.append(estate_overview)
            except ValidationError:
                logger.exception("Failed to validate estate %s", record)
                
            
        return valid

    async def _detail_with_session(self, session, id: int) -> Dict:
        url = format_url(self.base_url, f"estates/{id}")
        return await _request_json(session, url)

    async def _read_page(
        self,
        session: aiohttp.ClientSession,
        query_params: dict,
        page: int,
        per_page: int,
        headers: dict = {},
    ) -> Dict:
        paged_query = page_query(query_params, page, per_page)
        url = format_url(self.base_url, "estates", paged_query)
        return await _request_json(session, url, headers=headers)


async def _request_json(session, url, method="get", headers={}) -> Dict | None:
    async with session.request(method, url, headers=headers) as resp:
        try:
            resp.raise_for_status()
            return await resp.json()
        except aiohttp.ClientResponseError:
            logger.exception("Failed to get %s with status %s error", url, resp.status)
            return None


def _to_query_string(query_params):
    return "&".join([f"{k}={v}" for k, v in query_params.items()])


def format_url(url_base, path, query_params={}):
    query = _to_query_string(query_params)
    return f"{url_base}{path}?{query}"


def page_query(query_params, page, per_page):
    return query_params | {"per_page": per_page, "page": page}


def parse_query_result_page(page_dict: dict) -> List[dict]:
    return page_dict.get("_embedded", {}).get("estates", [])


def _parse_type(record):
    name = record['name']
    name_parts = name.split()
    if len(name_parts) < 3 or name_parts[0].lower() != "prodej":
        raise ValueError(f"Unexpected listing title: {name}.")
    
    if name_parts[1].lower() == "bytu":
        return "flat"
    
    elif name_parts[1].lower() == "rodinného" and name_parts[2].lower() == "domu" and len(name_parts)>3:
        return "house"
    else:
        return "unknown"
