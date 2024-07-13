import pytest
from .dummy_server import create_dummy_server
from baraky.client import SrealityEstatesClient

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(name="dummy_server")
async def _fix_dummy_server(aiohttp_server):
    return await create_dummy_server(aiohttp_server)


@pytest.fixture(name="estates_client")
async def _fix_estates_client(dummy_server):
    url = str(dummy_server.make_url("/"))
    return SrealityEstatesClient(base_url=url)
