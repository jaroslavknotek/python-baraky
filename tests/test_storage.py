import baraky.storages as storages
from baraky.models import EstateOverview


async def test_fs_storage_get_ids(fs_storage, monkeypatch):
    stored_paths = [
        fs_storage.root / "1.json",
        fs_storage.root / "2.json",
    ]

    assert await fs_storage.get_ids() == []

    def _patch_glob_files(_, __):
        return stored_paths

    monkeypatch.setattr(storages, "glob_files", _patch_glob_files)
    ids = await fs_storage.get_ids()
    assert "1" in ids
    assert "2" in ids


async def test_fs_storage_save(fs_storage, monkeypatch):
    models = [
        EstateOverview(
            id="1",
            price=1000,
            link="https://www.example.com/1",
            gps=(1, 1),
        )
    ]

    written_models = []

    async def _patch_write_model_json(_, model):
        nonlocal written_models
        written_models.append(model)

    monkeypatch.setattr(storages, "write_model_json", _patch_write_model_json)
    await fs_storage.save(models)

    assert len(written_models) == 1
    assert written_models[0] == models[0]
