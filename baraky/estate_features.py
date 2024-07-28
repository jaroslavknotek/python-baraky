import geopy.distance
from baraky.models import CommuteTimeFeatures, EstateOverview
import numpy as np
from datetime import timedelta
from typing import Dict
from baraky.settings import CommuteTimeFeatureSettings


class CommuteTimeFeature:
    def __init__(
        self, locations: Dict, settings: CommuteTimeFeatureSettings | None = None
    ):
        if settings is None:
            settings = CommuteTimeFeatureSettings()

        self.locations = locations
        self.minutes_per_km = settings.minutes_per_km

    def calculate(self, estate_overview: EstateOverview):
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
