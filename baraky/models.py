from pydantic import ConfigDict, BaseModel

from typing import Tuple


class EstateOverview(BaseModel, extra="allow"):
    model_config = ConfigDict(extra="allow")
    link: str
    price: int
    id: str
    gps: Tuple[float, float]

    @classmethod
    def from_record(cls, record: dict, detail_url: str):
        return cls(
            link=_extract_link(record, detail_url),
            price=_extract_price(record),
            id=_extract_id(record),
            gps=_extract_gps(record),
        )


def _extract_id(json_dict: dict) -> str:
    path_part = json_dict.get("_links", {}).get("self", {}).get("href")
    return path_part.split("/")[-1]


def _extract_link(json_dict: dict, detail_url: str) -> str:
    seo = json_dict["seo"]["locality"]
    estate_id = _extract_id(json_dict)
    return "{}/{}/{}".format(
        str(detail_url).strip("/"),
        seo.strip("/"),
        estate_id.strip("/"),
    )


def _extract_gps(json_dict: dict):
    lat = json_dict.get("gps", {}).get("lat")
    lon = json_dict.get("gps", {}).get("lon")
    return (lat, lon)


def _extract_price(json_dict: dict):
    return json_dict.get("price_czk", {}).get("value_raw")
