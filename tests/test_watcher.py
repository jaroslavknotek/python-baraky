import pytest
from . import models as test_models
from baraky.models import EstateOverview


async def test_watcher_update_cycle(watcher):
    queue = watcher.output_queue
    storage = watcher.storage
    client = watcher.client

    stored_estates = [
        EstateOverview(id="1", price=1000, link="https://www.example.com/1", gps=(1, 1))
    ]
    new_estates = [
        EstateOverview(id="2", price=2000, link="https://www.example.com/2", gps=(2, 2))
    ]
    storage.data = stored_estates
    client.data = new_estates

    await watcher.update()

    assert len(storage.data) == 2
    assert len(queue.data) == 1
    assert len(queue.data) == len(new_estates)
    assert queue.data[0] == new_estates[0]
    assert storage.data[1] == new_estates[0]


async def test_watcher_cycles(watcher, monkeypatch):
    timer = test_models.DeterministicCycleTimer(max_elapsed=2)

    update_invoked_times = 0

    async def update_patch():
        nonlocal update_invoked_times
        update_invoked_times += 1

    monkeypatch.setattr(watcher, "update", update_patch)

    watcher.timer = timer
    assert timer.elapsed_count == 0

    with pytest.raises(test_models.MaxElapsedError) as _:
        await watcher.watch()

    assert timer.elapsed_count == 2
    assert update_invoked_times == 2


async def test_watcher_enhance_estates():
    pass
