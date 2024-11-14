import asyncio
from baraky.notifications import TelegramNotificationsBot
from baraky.estate_features import PIDCommuteFeatureEnhancer
import logging
from baraky.storages import (
    EstatesStorage,
    MinioStorage,
    EstatesHitQueue,
    ReactionsStorage,
)
from baraky.estate_watcher import EstateWatcher
from baraky.models import EstateOverview, PIDCommuteFeature
from baraky.client import SrealityEstatesClient
import argparse
import baraky.io as io

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("baraky")
logger.setLevel(logging.DEBUG)

logging.getLogger("httpx").setLevel(logging.WARNING)


def main():
    args = setup_args()
    args.func(args)


def setup_args():
    parser = argparse.ArgumentParser(description="Baraky")

    subparsers = parser.add_subparsers(help="Commands")

    parser_watcher = subparsers.add_parser("watcher", help="Watch for new estates")
    parser_watcher.add_argument(
        "--query-path",
        type=str,
        help="Path to query json file",
        required=True,
    )
    parser_watcher.set_defaults(func=watcher_command)

    parser_sync = subparsers.add_parser("sync", help="Watch for new estates ONCE")
    parser_sync.add_argument(
        "--query-path",
        type=str,
        help="Path to query json file",
        required=True,
    )
    parser_sync.set_defaults(func=sync_command)

    parser_notifier = subparsers.add_parser("notifier", help="Notify about new estates")
    parser_notifier.set_defaults(func=notifier_command)

    return parser.parse_args()


def watcher_command(args):
    watcher = setup_watcher(args)
    asyncio.run(watcher.watch())


def sync_command(args):
    watcher = setup_watcher(args)
    asyncio.run(watcher.update())


def notifier_command(args):
    reactions_minio_storage = MinioStorage("reactions")
    reactions_storage = ReactionsStorage("estate/", reactions_minio_storage)
    hits_minio_storage = MinioStorage("hitqueue")
    queue = EstatesHitQueue("filtered/", hits_minio_storage)
    bot = TelegramNotificationsBot(
        queue,
        reactions_storage,
    )
    bot.start()


def _filter_house_close_to_prague(
    estate_overview: EstateOverview, commute_time: PIDCommuteFeature
) -> bool:
    if estate_overview.type != "house":
        return False
    if commute_time.time_minutes is None or commute_time.transfers_count is None:
        return False
    is_close = commute_time.time_minutes <= 75
    max_1_transfer = commute_time.transfers_count <= 4
    return max_1_transfer and is_close and estate_overview.price <= 8_000_000

def _filter_flat_close_to_prague(
    estate_overview: EstateOverview, commute_time: PIDCommuteFeature
) -> bool:
    if estate_overview.type != "flat":
        return False
    if commute_time.time_minutes is None or commute_time.transfers_count is None:
        return False
    is_close = commute_time.time_minutes <= 55
    max_transfer = commute_time.transfers_count <= 4
    return max_transfer and is_close and estate_overview.price <= 6_500_000


def filter_fn(estate_overview: EstateOverview) -> bool:
    commute_time = estate_overview.features.get("pid_commute_time")
    if commute_time is None:
        return False
    is_close_house = _filter_house_close_to_prague(estate_overview, commute_time)
    is_close_flat = _filter_flat_close_to_prague(estate_overview, commute_time)
    
    return is_close_house or is_close_flat


def setup_watcher(args):
    query_params = io.read_json_sync(args.query_path)
    client = SrealityEstatesClient(query_params)
    estates_minio_storage = MinioStorage("estates")
    storage = EstatesStorage("estate/", estates_minio_storage)
    hits_minio_storage = MinioStorage("hitqueue")
    queue = EstatesHitQueue("filtered/", hits_minio_storage)

    feature_calculators = {
        "pid_commute_time": PIDCommuteFeatureEnhancer(),
    }
    return EstateWatcher(
        client=client,
        storage=storage,
        output_queue=queue,
        feature_calculators=feature_calculators,
        filter_fn=filter_fn,
    )


if __name__ == "__main__":
    main()
