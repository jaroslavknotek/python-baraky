import pytest
from .dummy_server import create_dummy_server
from baraky.client import SrealityEstatesClient
from baraky.estate_watcher import EstateWatcher
from . import models
from baraky.storages import FileSystemStorage

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(name="fs_storage")
def _fix_fs_storage(tmp_path):
    tmp_storage = tmp_path / "test_storage"
    return FileSystemStorage(tmp_storage)


@pytest.fixture(name="watcher")
def _fix_watcher(mock_queue, mock_storage, mock_client):
    watcher = EstateWatcher(mock_client, mock_storage, mock_queue)
    watcher.timer = models.DeterministicCycleTimer()
    return watcher


@pytest.fixture(name="mock_queue")
async def _fix_mock_queue():
    return models.MockQueue()


@pytest.fixture(name="mock_storage")
async def _fix_mock_storage():
    return models.MockStorage()


@pytest.fixture(name="mock_client")
async def _fix_mock_client():
    return models.MockClient()


@pytest.fixture(name="dummy_server")
async def _fix_dummy_server(aiohttp_server):
    return await create_dummy_server(aiohttp_server)


@pytest.fixture(name="estates_client")
def _fix_estates_client(dummy_server):
    url = str(dummy_server.make_url("/"))
    return SrealityEstatesClient(query_params={}, base_url=url)
