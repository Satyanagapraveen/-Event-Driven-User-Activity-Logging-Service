import pytest
import mongomock
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

from app.services.nosql_query import NoSqlQuery


@pytest.fixture
def mock_mongo_client():
    client = mongomock.MongoClient()
    return client


@pytest.fixture
async def seeded_query_service(mock_mongo_client):
    collection = mock_mongo_client["test_db"]["events"]

    test_events = [
        {
            "event_id": "evt-001",
            "user_id": "user-1",
            "event_type": "login",
            "timestamp": datetime(2023, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
            "processing_timestamp": datetime(2023, 10, 1, 10, 0, 1, tzinfo=timezone.utc),
            "details": {"ip": "10.0.0.1"},
        },
        {
            "event_id": "evt-002",
            "user_id": "user-1",
            "event_type": "view_product",
            "timestamp": datetime(2023, 10, 2, 11, 0, 0, tzinfo=timezone.utc),
            "processing_timestamp": datetime(2023, 10, 2, 11, 0, 1, tzinfo=timezone.utc),
            "details": {"product": "abc"},
        },
        {
            "event_id": "evt-003",
            "user_id": "user-2",
            "event_type": "login",
            "timestamp": datetime(2023, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
            "processing_timestamp": datetime(2023, 10, 3, 12, 0, 1, tzinfo=timezone.utc),
            "details": {"ip": "10.0.0.2"},
        },
        {
            "event_id": "evt-004",
            "user_id": "user-2",
            "event_type": "purchase",
            "timestamp": datetime(2023, 10, 4, 13, 0, 0, tzinfo=timezone.utc),
            "processing_timestamp": datetime(2023, 10, 4, 13, 0, 1, tzinfo=timezone.utc),
            "details": {"order": "ord-99"},
        },
        {
            "event_id": "evt-005",
            "user_id": "user-1",
            "event_type": "logout",
            "timestamp": datetime(2023, 10, 5, 14, 0, 0, tzinfo=timezone.utc),
            "processing_timestamp": datetime(2023, 10, 5, 14, 0, 1, tzinfo=timezone.utc),
            "details": None,
        },
    ]

    for event in test_events:
        collection.insert_one(event)

    query_service = NoSqlQuery(
        mongo_uri="mongodb://localhost:27017/",
        db_name="test_db",
    )
    query_service.client = AsyncIOMotorClient("mongodb://localhost:27017/")
    query_service.collection = collection

    yield query_service