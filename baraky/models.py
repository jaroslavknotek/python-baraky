from pydantic import ConfigDict, BaseModel
from typing import Any, Dict

from typing import Tuple

Gps = Tuple[float, float]


class PIDCommuteFeature(BaseModel):
    time_minutes: int | None
    transfers_count: int | None
    from_station: str
    to_station: str
    gps_stop_distance: float


class PIDResponse(BaseModel):
    time_minutes: int
    transfers_count: int


class EstateOverview(BaseModel, extra="allow"):
    model_config = ConfigDict(extra="allow")
    link: str
    price: int
    id: str
    gps: Tuple[float, float]
    features: Dict[str, Any] = {}  # This is supposed to be mutable ATM

    @classmethod
    def from_record(cls, record: dict, detail_url: str):
        return cls(
            link=_extract_link(record, detail_url),
            price=_extract_price(record),
            id=_extract_id(record),
            gps=_extract_gps(record),
        )


class EstateQueueMessage(BaseModel):
    link: str
    price: int
    id: str
    pid_commute_time_min: int
    transfers_count: int
    station_nearby: str

    @classmethod
    def map_from_estate_overview(cls, model: EstateOverview):
        pid_commute_time: PIDCommuteFeature = model.features.get("pid_commute_time")
        if pid_commute_time is None:
            raise ValueError("PID commute time not found")
        return cls(
            link=model.link,
            price=model.price,
            id=model.id,
            pid_commute_time_min=pid_commute_time.time_minutes,
            transfers_count=pid_commute_time.transfers_count,
            station_nearby=pid_commute_time.from_station,
        )


class EstateReaction(BaseModel):
    estate_id: str
    username: str
    reaction: str


class MinioObject(BaseModel):
    data: str
    full_name: str


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
