import asyncio
from pathlib import Path
from datetime import datetime
from baraky.models import EstateOverview, EstateQueueMessage
from typing import List, Tuple
from tqdm.auto import tqdm
import logging
from minio import Minio
from baraky.settings import MinioClientSettings
import io
logger = logging.getLogger("baraky.storage.minio")

class MinioStorage:
    def __init__(
            self, 
            bucket_name:str, 
            settings:MinioClientSettings|None=None
        ):
        if settings is None:
            settings = MinioClientSettings()
        self.client = Minio(
            settings.endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=False,  # TODO make sure to address this
        )
        self.bucket_name = bucket_name
        self._bucket_ensured = False

    
    def list_ids_sync(self,prefix:str):
        self._ensure_bucket()
        objects = self.client.list_objects(
            self.bucket_name,
            prefix=prefix,
        )
        return [Path(obj.object_name).stem for obj in objects]

    def save_sync(
            self, 
            object_name, 
            object_body:str,
            content_type="application/json"
        ):
        self._ensure_bucket()
        data_stream = io.BytesIO(object_body.encode("utf-8"))
        self.client.put_object(
            self.bucket_name, object_name, data_stream, len(object_body), content_type
        )
    
    def get_sync(self, object_name:str) -> str|None:
        self._ensure_bucket()
        try:
            response =  self.client.get_object(self.bucket_name, object_name)
            return response.data.decode()
        except Exception:
            logger.exception("Error while getting object %s", object_name)
            return None
        finally:
            response.close()
            response.release_conn()
    def remove_sync(self, object_name:str): 
        self._ensure_bucket()
        self.client.remove_object(self.bucket_name, object_name)

    def _ensure_bucket(self):
        if not self._bucket_ensured and not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            self._bucket_ensured = True
            logger.info(f"Created bucket {self.bucket_name}")

class EstatesHitQueue:
    def __init__(self, object_prefix, storage):
        self.storage = storage
        self.object_prefix = object_prefix

    def put(self,estate: EstateQueueMessage): 
        json_text = estate.model_dump_json()
        timestamp = get_timestamp()
        prefix = self.object_prefix.rstrip('/')
        object_name = f"{prefix}/{timestamp}_{estate.id}.json"
        self.storage.save_sync(object_name, json_text)

    def peek(self)->Tuple[str,EstateQueueMessage]|None:
        ids = self.storage.list_ids_sync(self.object_prefix)
        if not ids:
            return None
        
        min_id = min(ids)
        prefix = self.object_prefix.rstrip('/')
        object_name = f"{prefix}/{min_id}"
        data = self.storage.client.get_object(self.storage.bucket_name, object_name)
        return min_id,EstateQueueMessage.model_load_json(data.read())

    def delete(self, object_id): 
        prefix = self.object_prefix.rstrip('/')
        object_name = f"{prefix}/{object_id}"
        self.storage.remove_sync(object_name)


# Is using async over sync here a good idea?
class EstatesStorage:
    def __init__(self, object_prefix, storage):
        self.storage = storage
        self.object_prefix = object_prefix

    def list_ids_sync(self):
        return self.storage.list_ids_sync(self.object_prefix)

    async def list_ids(self):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.list_ids_sync)

    def save_many_sync(self, estates: List[EstateOverview]):
        logger.debug("Saving %d estates", len(estates))
        prefix = self.object_prefix.rstrip('/')
        for estate in tqdm(estates, desc="Saving"):
            json_text = estate.model_dump_json()
            object_name = f"{prefix}/{estate.id}.json"
            self.storage.save_sync(object_name, json_text)

    async def save_many(self, estates: List[EstateOverview]):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.save_many_sync, estates)


def get_timestamp():
    return datetime.strftime(datetime.now(),'%Y%m%d%H%M%S')

