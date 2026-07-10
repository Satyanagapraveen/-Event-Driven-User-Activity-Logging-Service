import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone

from app.main import app
from app.models import ActivityLogQueryParams
from app.routers.activity_logs import get_query_service

class TestActivityLogsAPI:

    @pytest.fixture
    async def client(self, seeded_query_service):
        app.dependency_overrides[get_query_service] = lambda: seeded_query_service
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
        app.dependency_overrides.clear()

    async def test_get_all_events_default_pagination(self, client):
        response = await client.get("/api/v1/activity-logs")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5
        assert data[0]["event_id"] == "evt-005"

    async def test_pagination_limit(self, client):
        response = await client.get("/api/v1/activity-logs?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    async def test_pagination_offset(self, client):
        response = await client.get("/api/v1/activity-logs?limit=2&offset=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["event_id"] == "evt-003"

    async def test_filter_by_user_id(self, client):
        response = await client.get("/api/v1/activity-logs?user_id=user-1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        for event in data:
            assert event["user_id"] == "user-1"

    async def test_filter_by_event_type(self, client):
        response = await client.get("/api/v1/activity-logs?event_type=login")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        for event in data:
            assert event["event_type"] == "login"

    async def test_filter_by_date_range(self, client):
        response = await client.get(
            "/api/v1/activity-logs?start_date=2023-10-02T00:00:00Z&end_date=2023-10-04T00:00:00Z"
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        event_ids = [e["event_id"] for e in data]
        assert "evt-002" in event_ids
        assert "evt-003" in event_ids

    async def test_filter_by_user_and_type(self, client):
        response = await client.get(
            "/api/v1/activity-logs?user_id=user-1&event_type=login"
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["event_id"] == "evt-001"

    async def test_invalid_limit_returns_error(self, client):
        response = await client.get("/api/v1/activity-logs?limit=500")
        assert response.status_code == 422

    async def test_invalid_date_returns_error(self, client):
        response = await client.get("/api/v1/activity-logs?start_date=not-a-date")
        assert response.status_code == 422

    async def test_no_results(self, client):
        response = await client.get("/api/v1/activity-logs?user_id=nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 0