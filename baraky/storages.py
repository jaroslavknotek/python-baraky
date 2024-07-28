import asyncio
from pathlib import Path
from baraky.models import EstateOverview
from typing import List
from tqdm.auto import tqdm
import logging
from minio import Minio
from baraky.settings import MinioClientSettings
import io
import baraky.io as bio


# Is using async over sync here a good idea?
class MinioStorage:
    def __init__(self):
        settings = MinioClientSettings()
        self.client = Minio(
            settings.endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=False,  # TODO make sure to address this
        )
        self.bucket_name = settings.bucket_name
        self.logger = logging.getLogger("baraky.storage.minio")

    def _get_ids_sync(self):
        objects = self.client.list_objects(
            self.bucket_name,
            prefix="estate/house/",
        )
        return [Path(obj.object_name).stem for obj in objects]

    async def get_ids(self):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_ids_sync)

    def _save_sync(self, estates: List[EstateOverview]):
        return
        for estate in tqdm(estates, desc="Saving"):
            json_text = estate.model_dump_json()
            data_stream = io.BytesIO(json_text.encode("utf-8"))
            object_name = f"estate/house/{estate.id}.json"
            content_type = "application/json"
            self.client.put_object(
                self.bucket_name, object_name, data_stream, len(json_text), content_type
            )

    async def save(self, estates: List[EstateOverview]):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_sync, estates)

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            self.logger.info(f"Created bucket {self.bucket_name}")


class FileSystemStorage:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    async def get_ids(self):
        file_names = bio.glob_files(self.root, "*.json")
        return [file.stem for file in file_names]

    async def save(self, estates):
        for estate in estates:
            file_path = self.root / f"{estate.id}.json"
            await bio.write_model_json(file_path, estate)
