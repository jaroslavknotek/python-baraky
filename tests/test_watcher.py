from . import models


async def test_watcher_update_cycle(watcher):
    pass


# this is not very deterministic
async def test_watcher_cycles(watcher, monkeypatch):
    timer = models.DeterministicCycleTimer(max_elapsed=2)

    update_invoked_times = 0

    async def update_patch():
        nonlocal update_invoked_times
        update_invoked_times += 1

    monkeypatch.setattr(watcher, "update", update_patch)

    watcher.timer = timer
    assert timer.elapsed_count == 0

    try:
        await watcher.watch()
        assert False, "Should have raised MaxElapsedError"
    except models.MaxElapsedError:
        pass

    assert timer.elapsed_count == 2
    assert update_invoked_times == 2


async def test_watcher_enhance_estates():
    pass
