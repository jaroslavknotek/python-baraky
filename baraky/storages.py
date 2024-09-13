import asyncio

from pathlib import Path
from datetime import datetime
from baraky.models import (
    EstateOverview,
    EstateQueueMessage,
    EstateReaction,
    MinioObject,
)
from typing import List, Tuple
import logging
from minio import Minio
from minio.datatypes import Object
from baraky.settings import MinioClientSettings
import io

logger = logging.getLogger("baraky.storage.minio")


class MinioStorage:
    def __init__(self, bucket_name: str, settings: MinioClientSettings | None = None):
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

    def _list_objects(self, prefix: str) -> List[Object]:
        self._ensure_bucket()
        return self.client.list_objects(
            self.bucket_name,
            prefix=prefix,
        )

    def get_objects(self, prefix: str) -> List[MinioObject]:
        objects = self._list_objects(prefix)
        names = [o.object_name for o in objects]
        data_list = [self.get_sync(object_name) for object_name in names]

        return [
            MinioObject(
                data=data,
                full_name=name,
            )
            for data, name in zip(data_list, names)
            if data is not None
        ]

    def list_ids_sync(self, prefix: str):
        objects = self._list_objects(prefix)
        return [Path(obj.object_name).stem for obj in objects]

    def save_sync(self, object_name, object_body: str, content_type="application/json"):
        self._ensure_bucket()
        values_as_bytes = object_body.encode("utf-8")
        data_stream = io.BytesIO(values_as_bytes)
        length = len(values_as_bytes)

        self.client.put_object(
            self.bucket_name, object_name, data_stream, length, content_type
        )

    def get_sync(self, object_name: str) -> str | None:
        self._ensure_bucket()
        # logger.debug(
        #     "Getting object %s from bucket %s",
        #     object_name,
        #     self.bucket_name,
        # )
        response = self.client.get_object(self.bucket_name, object_name)
        try:
            return response.data.decode()
        except Exception:
            logger.exception(
                "Error while getting object %s from bucket %s",
                object_name,
                self.bucket_name,
            )
            return None
        finally:
            response.close()
            response.release_conn()

    def remove_sync(self, object_name: str):
        logger.debug(
            "Removing object %s from bucket %s",
            object_name,
            self.bucket_name,
        )
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

    def total(self):
        ids = self.storage.list_ids_sync(self.object_prefix)
        return len(ids)

    def put(self, estate: EstateQueueMessage):
        json_text = estate.model_dump_json()
        timestamp = get_timestamp()
        prefix = self.object_prefix.rstrip("/")
        object_name = f"{prefix}/{timestamp}_{estate.id}.json"
        self.storage.save_sync(object_name, json_text)

    def peek(self) -> Tuple[str, EstateQueueMessage] | None:
        ids = self.storage.list_ids_sync(self.object_prefix)
        if not ids or not any(ids):
            return None

        min_id = min(ids)
        prefix = self.object_prefix.rstrip("/")
        object_name = f"{prefix}/{min_id}.json"
        data = self.storage.get_sync(object_name)
        return min_id, EstateQueueMessage.model_validate_json(data)

    def delete(self, object_id):
        prefix = self.object_prefix.rstrip("/")
        object_name = f"{prefix}/{object_id}.json"
        self.storage.remove_sync(object_name)


class ReactionsStorage:
    def __init__(self, object_prefix, storage):
        self.storage = storage
        self.object_prefix = object_prefix

    def write(self, estate_reaction: EstateReaction):
        prefix = self.object_prefix.rstrip("/")
        reaction = estate_reaction.reaction
        user = estate_reaction.username
        estate_id = estate_reaction.estate_id
        object_name = f"{prefix}/{estate_id}/{user}.json"
        self.storage.save_sync(object_name, reaction, content_type="text/plain")

    def read_by_estate(self, estate_id: str) -> List[EstateReaction]:
        prefix = self.object_prefix.rstrip("/")
        object_prefix = f"{prefix}/{estate_id}/"
        data = self.storage.get_objects(object_prefix)

        return [
            EstateReaction(
                estate_id=estate_id, username=Path(d.full_name).stem, reaction=d.data
            )
            for d in data
        ]


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

    async def get_all(self):
        all_estates = self.storage.get_objects(self.object_prefix)

        return [
            EstateOverview.model_validate_json(estate.data) for estate in all_estates
        ]

    def save_many_sync(self, estates: List[EstateOverview]):
        logger.debug("Saving %d estates", len(estates))
        prefix = self.object_prefix.rstrip("/")
        for estate in estates:
            json_text = estate.model_dump_json()
            object_name = f"{prefix}/{estate.id}.json"
            self.storage.save_sync(object_name, json_text)

    async def save_many(self, estates: List[EstateOverview]):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.save_many_sync, estates)


def get_timestamp():
    return datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
