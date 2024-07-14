import pytest


# async def test_estate_detail(estates_client, dummy_server):
#     dummy_server.app["data"]["estates"] = [
#         {
#             "_links": {
#                 "self": {"href": "/estate/1"},
#             },
#         }
#     ]
#
#     estate = await estates_client.detail(1)
#     assert estate
#     assert estate.get("_links", {}).get("self", {}).get("href") == "/estate/1"
#
#     not_estate = await estates_client.detail(999)
#     assert not_estate is None


@pytest.mark.filterwarnings("ignore:DeprecationWarning")
async def test_estate_overview(estates_client, dummy_server):
    dummy_server.app["data"]["estates"] = [
        {
            "_links": {
                "self": {"href": f"/estate/{i}"},
            },
            "seo": {"locality": f"locality_{i}"},
            "price_czk": {"value_raw": i * 1000000},
            "gps": (50.0, 14.0 + i),
        }
        for i in range(10)
    ]

    estates = await estates_client.read_all()
    assert estates, "Expected some estates"
    assert len(estates) == 10, f"Expected 10 estates, got {len(estates)}"

    for i, estate in enumerate(estates):
        assert estate.id == str(i)
        assert estate.price == i * 1000000
