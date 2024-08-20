import geopy.distance
from baraky.models import (
    CommuteTimeFeatures,
    EstateOverview,
    PIDResponse,
    PIDCommuteFeature,
    Gps,
)
import numpy as np
from datetime import timedelta
from typing import Dict
from baraky.settings import (
    CommuteTimeFeatureSettings,
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


class CommuteTimeFeature:
    def __init__(
        self, locations: Dict, settings: CommuteTimeFeatureSettings | None = None
    ):
        if settings is None:
            settings = CommuteTimeFeatureSettings()

        self.locations = locations
        self.minutes_per_km = settings.minutes_per_km

    async def calculate(self, estate_overview: EstateOverview):
        gps_coords = estate_overview.gps

        loc_to_km_min = calculate_distances(gps_coords, self.locations)
        closest_place, kms = find_closest(loc_to_km_min)
        time_to_base_td = timedelta(
            minutes=self.locations[closest_place]["trip_base_min"]
        )
        time_to_base_min = time_to_base_td.total_seconds() / 60
        commute_time = time_to_base_min + kms * self.minutes_per_km

        return CommuteTimeFeatures(
            closest_place=closest_place,
            km_to_closest=kms,
            minutes_spent=commute_time,
        )


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
        routes = [d for d in resp["data"]]
        routes_mins = [to_min(r) for r in routes]
        min_idx = np.argmin(routes_mins)

        transfers_str = routes[min_idx]["transfers"].split()[0]
        transfers = 0
        if not transfers_str.startswith("bez"):
            transfers = int(transfers_str)

        mins = routes_mins[min_idx]
        return PIDResponse(time_minutes=mins, transfers_count=transfers)


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
            )
        else:
            return PIDCommuteFeature(
                time_minutes=resp.time_minutes,
                transfers_count=resp.transfers_count,
                from_station=found_stop,
                to_station=self.desired_stop,
                gps_stop_distance=distance,
            )


def calculate_distances(coords_from, locations):
    loc_to_km_min = {}
    for k, v in locations.items():
        coords_to = v["gps"]
        distance = geopy.distance.geodesic(coords_from, coords_to)
        loc_to_km_min[k] = distance.km

    return loc_to_km_min


def find_closest(distances):
    k = list(distances.keys())
    kms_to_station = [v for v in distances.values()]
    idx = np.argmin(kms_to_station)
    return k[idx], kms_to_station[idx]


def find_closest_way(coords_from, stations):
    distances = calculate_distances(coords_from, stations)

    closest_place, kms = find_closest(distances)
    time_to_base = timedelta(minutes=stations[closest_place]["trip_base_min"])
    return closest_place, kms, time_to_base


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
