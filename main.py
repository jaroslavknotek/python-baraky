import asyncio
from baraky.estate_features import CommuteTimeFeature
import logging
from baraky.storages import MinioStorage
from baraky.estate_watcher import EstateWatcher
from baraky.models import EstateOverview
from baraky.client import SrealityEstatesClient
import argparse

import baraky.io as io

logger = logging.getLogger("baraky")
logging.basicConfig(level=logging.INFO)


def setup_args():
    parser = argparse.ArgumentParser(description="Baraky")

    parser.add_argument(
        "--query-path",
        type=str,
        help="Path to query json file",
        required=True,
    )
    parser.add_argument(
        "--locations-path",
        type=str,
        help="Path to locations json file",
        required=True,
    )
    return parser.parse_args()


class StdOutQueue:
    def put(self, estate):
        print(
            estate.price,
            estate.link,
            estate.features["commute_time"],
        )


def _filter_close_to_prague(
    estate_overview: EstateOverview, commute_time: CommuteTimeFeature
):
    is_close = commute_time.km_to_closest < 30
    is_prague = commute_time.closest_place == "Praha"

    return is_close and is_prague and estate_overview.price < 10_000_000


def _filter_close_to_any_station(
    estate_overview: EstateOverview, commute_time: CommuteTimeFeature
):
    return commute_time.km_to_closest < 8 and estate_overview.price < 8_000_000


def _filter_fast_commuting(
    estate_overview: EstateOverview, commute_time: CommuteTimeFeature
):
    return commute_time.minutes_spent < 75 and estate_overview.price < 6_000_000


def filter(estate_overview: EstateOverview):
    commute_time = estate_overview.features.get("commute_time")
    close_to_prague = _filter_close_to_prague(estate_overview, commute_time)
    close_to_any_station = _filter_close_to_any_station(estate_overview, commute_time)
    fast_commuting = _filter_fast_commuting(estate_overview, commute_time)
    return close_to_prague or close_to_any_station or fast_commuting


async def main():
    parser = setup_args()

    query_params = await io.read_json(parser.query_path)
    locations = await io.read_json(parser.locations_path)

    client = SrealityEstatesClient(query_params)

    minio_storage = MinioStorage()

    feature_calculators = {"commute_time": CommuteTimeFeature(locations)}
    watcher = EstateWatcher(
        client=client,
        storage=minio_storage,
        output_queue=StdOutQueue(),
        feature_calculators=feature_calculators,
        filter_fn=filter,
    )

    await watcher.update()


if __name__ == "__main__":
    asyncio.run(main())
