from datetime import timedelta, datetime
import asyncio
import logging

from typing import Callable
from baraky.models import EstateOverview

logger = logging.getLogger("baraky.estate_watcher")


class EstateWatcher:
    def __init__(
        self,
        client,
        storage,
        output_queue,
        feature_calculators={},
        filter_fn: Callable[EstateOverview, bool] | None = None,
        interval_sec=600,
    ):
        self.client = client
        self.storage = storage

        interval = timedelta(seconds=interval_sec)
        self.timer = CycleTimer(interval)
        self.output_queue = output_queue
        self.feature_calculators = feature_calculators
        self.filter_fn = filter_fn or (lambda _: True)

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
        new_estates = await self._read_new()
        self._notify(new_estates)
        await self.storage.save(new_estates)

    async def _read_new(self):
        existing_ids = await self.storage.get_ids()
        overviews = await self.client.read_all()
        all_ids = {o.id: o for o in overviews}
        new_ids = set(list(all_ids.keys())) - set(existing_ids)
        new_raw_estates = [all_ids[id] for id in new_ids]

        self.enhance_estates(new_raw_estates)
        return new_raw_estates

    def _notify(self, estates):
        filtered = [e for e in estates if self.filter_fn(e)]
        logger.info(f"Found {len(filtered)} new (filtered) estates")
        for estate in filtered:
            self.output_queue.put(estate)

    def enhance_estates(self, estates):
        for name, calculator in self.feature_calculators.items():
            for estate in estates:
                feature_data = calculator.calculate(estate)
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
