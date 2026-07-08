import logging
from typing import Optional, List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from app.models import ActivityLogQueryParams

logger = logging.getLogger(__name__)


class NoSqlQuery:
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
        logger.info(
            "Connected to MongoDB for queries",
            extra={
                "db_name": self.db_name,
                "collection_name": self.collection_name,
            },
        )

    async def disconnect(self) -> None:
        if self.client is not None:
            self.client.close()
            logger.info("Disconnected from MongoDB query service")

    async def fetch_activity_logs(
        self, params: ActivityLogQueryParams
    ) -> List[Dict[str, Any]]:
        query_filter: Dict[str, Any] = {}

        if params.user_id:
            query_filter["user_id"] = params.user_id

        if params.event_type:
            query_filter["event_type"] = params.event_type

        if params.start_date or params.end_date:
            timestamp_filter: Dict[str, Any] = {}
            if params.start_date:
                timestamp_filter["$gte"] = params.start_date
            if params.end_date:
                timestamp_filter["$lte"] = params.end_date
            query_filter["timestamp"] = timestamp_filter

        cursor = self.collection.find(query_filter)
        cursor = cursor.sort("timestamp", -1)
        cursor = cursor.skip(params.offset)
        cursor = cursor.limit(params.limit)

        documents = await cursor.to_list(length=params.limit)

        for doc in documents:
            doc["_id"] = str(doc["_id"])

        return documents