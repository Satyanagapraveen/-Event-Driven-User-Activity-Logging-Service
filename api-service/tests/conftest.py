import pytest
import mongomock
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
            "timestamp": "2023-10-01T10:00:00Z",
            "processing_timestamp": "2023-10-01T10:00:01Z",
            "details": {"ip": "10.0.0.1"},
        },
        {
            "event_id": "evt-002",
            "user_id": "user-1",
            "event_type": "view_product",
            "timestamp": "2023-10-02T11:00:00Z",
            "processing_timestamp": "2023-10-02T11:00:01Z",
            "details": {"product": "abc"},
        },
        {
            "event_id": "evt-003",
            "user_id": "user-2",
            "event_type": "login",
            "timestamp": "2023-10-03T12:00:00Z",
            "processing_timestamp": "2023-10-03T12:00:01Z",
            "details": {"ip": "10.0.0.2"},
        },
        {
            "event_id": "evt-004",
            "user_id": "user-2",
            "event_type": "purchase",
            "timestamp": "2023-10-04T13:00:00Z",
            "processing_timestamp": "2023-10-04T13:00:01Z",
            "details": {"order": "ord-99"},
        },
        {
            "event_id": "evt-005",
            "user_id": "user-1",
            "event_type": "logout",
            "timestamp": "2023-10-05T14:00:00Z",
            "processing_timestamp": "2023-10-05T14:00:01Z",
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