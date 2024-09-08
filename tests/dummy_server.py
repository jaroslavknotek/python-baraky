from aiohttp import web
from aiohttp.web_response import Response, json_response


async def create_dummy_server(create_server):
    app = web.Application()
    app.add_routes(
        [
            web.route("*", "/200/", return_200),
            web.get("/estates", estates_overview),
            web.get("/estates/{estate_id}", estates_detail),
        ]
    )
    server = await create_server(app)
    app.update(
        server_name=f"http://localhost:{server.port}",
        data={"estates": []},
    )
    return server


async def return_200(request):
    return Response()


async def estates_overview(request):
    per_page = int(request.query.get("per_page", 100))
    page = int(request.query.get("page", 1))

    idx_start = (page - 1) * per_page
    idx_end = page * per_page

    total_estates = len(request.app["data"]["estates"])
    estates = request.app["data"]["estates"][idx_start:idx_end]
    return json_response(
        {
            "result_size": total_estates,
            "_embedded": {"estates": estates},
        }
    )


async def estates_detail(request):
    assert "estate_id" in request.match_info, "Expected estate_id in the request"
    estate_id = request.match_info["estate_id"]
    data = request.app["data"]["estates"]
    for estate in data:
        if estate["_links"]["self"]["href"] == f"/estate/{estate_id}":
            return json_response(estate)
    return json_response({}, status=404)
