from baraky.models import (
    EstateOverview,
    PIDResponse,
    PIDCommuteFeature,
    Gps,
)
import numpy as np
from typing import Dict
from baraky.settings import (
    PIDClientSettings,
    PIDCommuteFeatureEnhancerSettings,
)
from pandas.tseries.offsets import BDay
import datetime
import scipy
from urllib.parse import quote
import aiohttp
from baraky.client import _request_json
import json


class PIDClient:
    def __init__(self, settings: PIDClientSettings | None = None):
        if settings is None:
            settings = PIDClientSettings()

        self.settings = settings

    async def get_route(self, stop_from, stop_to) -> PIDResponse | None:
        query = list(self.settings.query)
        next_bday = next_business_day_str()
        query.extend(
            [
                ["stop_from", stop_from],
                ["stop_to", stop_to],
                ["date", next_bday],
            ]
        )
        query_part = "&".join([f"{quote(k)}={quote(v)}" for k, v in query])
        url = f"{self.settings.url_base}?{query_part}"

        async with aiohttp.ClientSession() as session:
            resp = await _request_json(session, url)

        if resp is None or len(resp.get("data", [])) == 0:
            return None
        routes_data = [d for d in resp["data"]]
        routes_mins = [to_min(r) for r in routes_data]
        min_idx = np.argmin(routes_mins)
        min_routes = routes_data[min_idx]

        transfers_str = min_routes["transfers"].split()[0]
        transfers = 0
        if not transfers_str.startswith("bez"):
            transfers = int(transfers_str)
        route_list = min_routes["route"]
        path_pieces = [
            (r["displayStation"], r["destinationStation"], r["class"])
            for r in route_list
        ]
        path_lines = [f"{f}->{t} ({c})" for f, t, c in path_pieces]
        path_info = "\n".join(path_lines[: transfers + 1])
        mins = routes_mins[min_idx]

        return PIDResponse(
            time_minutes=mins, transfers_count=transfers, path_info=path_info
        )


class PIDCommuteFeatureEnhancer:
    def __init__(
        self,
        stops_data: Dict[str, Gps] = None,
        pid_client: PIDClient | None = None,
        settings: PIDCommuteFeatureEnhancerSettings | None = None,
    ):
        if pid_client is None:
            pid_client = PIDClient()
        if settings is None:
            settings = PIDCommuteFeatureEnhancerSettings()

        if stops_data is None:
            with open(settings.stops_path) as f:
                stops = json.load(f)
            stops_data = {
                s["idosName"]: (s["avgLat"], s["avgLon"]) for s in stops["stopGroups"]
            }

        self.pid_client = PIDClient()
        self.stops_names = list(stops_data.keys())
        kddata = np.array(list(stops_data.values()))
        self.stops_tree = scipy.spatial.KDTree(kddata)
        self.desired_stop = settings.desired_stop

    async def calculate(self, estate_overview: EstateOverview):
        distance, idx = self.stops_tree.query(estate_overview.gps)

        found_stop = self.stops_names[idx]
        resp = await self.pid_client.get_route(found_stop, self.desired_stop)

        if resp is None:
            return PIDCommuteFeature(
                time_minutes=None,
                transfers_count=None,
                from_station=found_stop,
                to_station=self.desired_stop,
                gps_stop_distance=distance,
                path_info=None,
            )
        else:
            return PIDCommuteFeature(
                time_minutes=resp.time_minutes,
                transfers_count=resp.transfers_count,
                from_station=found_stop,
                to_station=self.desired_stop,
                gps_stop_distance=distance,
                path_info=resp.path_info,
            )


def find_closest(distances):
    k = list(distances.keys())
    kms_to_station = [v for v in distances.values()]
    idx = np.argmin(kms_to_station)
    return k[idx], kms_to_station[idx]


def next_business_day_str():
    today = datetime.datetime.today()
    next_bday = today + BDay(1)
    return next_bday.strftime("%d.%m.%Y")


def to_min(record):
    timeLength = record["timeLength"]
    splits = timeLength.split(" ")[::-1]

    kv = {}
    for i in range(0, len(splits), 2):
        kv[splits[i]] = int(splits[i + 1])

    return kv.get("min", 0) + 60 * kv.get("hod", 0)
