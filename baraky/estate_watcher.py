from datetime import timedelta, datetime
import asyncio
import logging
from tqdm.auto import tqdm
from typing import Callable
from baraky.models import EstateOverview, EstateQueueMessage

logger = logging.getLogger("baraky.estate_watcher")


class EstateWatcher:
    def __init__(
        self,
        client,
        storage,
        output_queue,
        feature_calculators={},
        filter_fn: Callable[[EstateOverview], bool] | None = None,
        interval_sec=600,
        progress=True,
    ):
        self.client = client
        self.storage = storage

        interval = timedelta(seconds=interval_sec)
        self.timer = CycleTimer(interval)
        self.output_queue = output_queue
        self.feature_calculators = feature_calculators
        self.filter_fn = filter_fn or (lambda _: True)
        self.tqdm_disabled = not progress

    async def watch(self):
        while True:
            try:
                if self.timer.elapsed():
                    await self.update()
                    self.timer.reset()
                await self.timer.wait()
            except asyncio.CancelledError:
                logger.info("Watcher stopping gracefully")
                break

    async def update(self):
        logger.info("Running update cycle")
        new_estates = await self._read_new()
        self._notify(new_estates)
        await self.storage.save_many(new_estates)

    async def _read_new(self):
        stored_estates_list = self.storage.get_all()
        overviews = await self.client.read_all()

        stored_estates = {e.id: e for e in stored_estates_list}

        new_or_updated = []

        for received_estate in overviews:
            stored = stored_estates.get(received_estate.id)
            if stored is None or stored.price != received_estate.price:
                new_or_updated.append(received_estate)

        logger.debug(
            "Found existing: %d new: %d", len(stored_estates), len(new_or_updated)
        )

        await self.enhance_estates(new_or_updated)
        return new_or_updated

    def _notify(self, estates):
        filtered = [e for e in estates if self.filter_fn(e)]
        logger.debug(f"Found {len(filtered)} new (filtered) estates")
        for estate in filtered:
            model = EstateQueueMessage.map_from_estate_overview(estate)
            self.output_queue.put(model)

    async def enhance_estates(self, estates):
        logger.info(f"Enhancing {len(estates)} estates with features")

        estates_it = tqdm(
            estates,
            desc="Enhancing estates",
            disable=self.tqdm_disabled or len(estates) == 0,
        )
        for estate in estates_it:
            for name, calculator in self.feature_calculators.items():
                if name == "pid_commute_time":
                    await asyncio.sleep(0.1)
                feature_data = await calculator.calculate(estate)
                estate.features[name] = feature_data


class CycleTimer:
    def __init__(self, interval):
        self.interval = interval
        self.last = None

    def reset(self):
        self.last = datetime.now()

    def elapsed(self):
        if self.last is None:
            return True
        return (datetime.now() - self.last) > self.interval

    async def wait(self):
        while not self.elapsed():
            await asyncio.sleep(1)
