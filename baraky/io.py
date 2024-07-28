import aiofiles
import json


async def write_model_json(file_path, model):
    json = model.model_dump_json()
    async with aiofiles.open(file_path, "w") as afp:
        await afp.write(json)


async def read_json(file_path):
    async with aiofiles.open(file_path, "r") as afp:
        content = await afp.read()
        return json.loads(content)


def glob_files(file_path, glob_pattern):
    return file_path.glob(glob_pattern)
