import asyncio
from baraky.estate_features import CommuteTimeFeature, PIDCommuteFeatureEnhancer
import logging
from baraky.storages import MinioStorage
from baraky.estate_watcher import EstateWatcher
from baraky.models import EstateOverview, PIDCommuteFeature
from baraky.client import SrealityEstatesClient
import argparse
from baraky.queues import RabbitQueueProducer
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


def _filter_close_to_prague(
    estate_overview: EstateOverview, commute_time: PIDCommuteFeature
) -> bool:
    if commute_time.time_minutes is None or commute_time.transfers_count is None:
        return False
    is_close = commute_time.time_minutes < 60
    max_1_transfer = commute_time.transfers_count <= 1
    return max_1_transfer and is_close and estate_overview.price < 10_000_000


def filter_fn(estate_overview: EstateOverview) -> bool:
    commute_time = estate_overview.features.get("pid_commute_time")
    if commute_time is None:
        return False
    return _filter_close_to_prague(estate_overview, commute_time)


async def main():
    parser = setup_args()

    query_params = await io.read_json(parser.query_path)
    locations = await io.read_json(parser.locations_path)

    client = SrealityEstatesClient(query_params)

    minio_storage = MinioStorage()

    feature_calculators = {
        "commute_time": CommuteTimeFeature(locations),
        "pid_commute_time": PIDCommuteFeatureEnhancer(),
    }
    queue = RabbitQueueProducer("estates_hits")
    watcher = EstateWatcher(
        client=client,
        storage=minio_storage,
        output_queue=queue,
        feature_calculators=feature_calculators,
        filter_fn=filter_fn,
    )

    await watcher.update()


if __name__ == "__main__":
    asyncio.run(main())
