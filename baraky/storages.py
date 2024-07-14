import aiofiles
from pathlib import Path


class FileSystemStorage:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    async def get_ids(self):
        file_names = glob_files(self.root, "*.json")
        return [file.stem for file in file_names]

    async def save(self, estates):
        for estate in estates:
            file_path = self.root / f"{estate.id}.json"
            await write_model_json(file_path, estate)


async def write_model_json(file_path, model):
    json = model.model_dump_json()
    async with aiofiles.async_open(file_path, "w") as afp:
        await afp.write(json)


def glob_files(file_path, glob_pattern):
    return file_path.glob(glob_pattern)
