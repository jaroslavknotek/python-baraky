import pytest


async def test_estate_detail(estates_client, dummy_server):
    dummy_server.app["data"]["estates"] = [
        {
            "_links": {
                "self": {"href": "/estate/1"},
            },
        }
    ]

    estate = await estates_client.detail(1)
    assert estate
    assert estate.get("_links", {}).get("self", {}).get("href") == "/estate/1"

    not_estate = await estates_client.detail(999)
    assert not_estate is None


@pytest.mark.filterwarnings("ignore:DeprecationWarning")
async def test_estate_overview(estates_client, dummy_server):
    return None
    dummy_server.app["data"]["estates"] = [
        {
            "_links": {
                "self": {"href": f"/estate/{i}"},
            },
        }
        for i in range(10)
    ]

    estates = await estates_client.query(per_page=3)
    assert estates, "Expected some estates"
    assert len(estates) == 10, f"Expected 10 estates, got {len(estates)}"

    for i, estate in enumerate(estates):
        assert estate
        received_href = estate.get("_links", {}).get("self", {}).get("href")
        expected_href = f"/estate/{i}"
        print(received_href, expected_href)
        assert received_href == expected_href
