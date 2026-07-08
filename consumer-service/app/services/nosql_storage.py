import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
import asyncio
from app.models import EnrichedUserActivityEvent

logger = logging.getLogger(__name__)


class NoSqlStorage:
    def __init__(
        self,
        mongo_uri: str,
        db_name: str,
        collection_name: str = "events",
    ):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.collection: Optional[AsyncIOMotorCollection] = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.collection = self.client[self.db_name][self.collection_name]
        await self._ensure_indexes()
        logger.info(
            "Connected to MongoDB",
            extra={
                "db_name": self.db_name,
                "collection_name": self.collection_name,
            },
        )
    def connect_sync(self) -> None:
        import asyncio
        asyncio.run(self.connect())

    def disconnect_sync(self) -> None:
        import asyncio
        asyncio.run(self.disconnect())

    async def _ensure_indexes(self) -> None:
        await self.collection.create_index(
            "event_id",
            unique=True,
            name="idx_event_id_unique",
        )
        await self.collection.create_index(
            [("user_id", 1), ("timestamp", -1)],
            name="idx_user_id_timestamp",
        )
        await self.collection.create_index(
            [("event_type", 1), ("timestamp", -1)],
            name="idx_event_type_timestamp",
        )
        logger.info("Database indexes ensured")

    def save_event(self, event: EnrichedUserActivityEvent) -> bool:
        document = event.model_dump()

        result = self.collection.delegate.update_one(
            {"event_id": event.event_id},
            {"$setOnInsert": document},
            upsert=True,
        )

        if result.upserted_id is not None:
            logger.info(
                "Event stored successfully",
                extra={"event_id": event.event_id},
            )
            return True
        else:
            logger.warning(
                "Duplicate event ignored",
                extra={"event_id": event.event_id},
            )
            return True

    async def disconnect(self) -> None:
        if self.client is not None:
            self.client.close()
            logger.info("Disconnected from MongoDB")