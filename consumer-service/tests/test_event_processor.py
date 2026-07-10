import json
from datetime import datetime, timezone

import pytest

from app.models import RawUserActivityEvent, EnrichedUserActivityEvent
from app.services.event_processor import EventProcessor


class TestEventProcessor:

    def test_process_valid_event(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)

        event_data = {
            "user_id": "user-001",
            "event_type": "login",
            "timestamp": "2023-10-27T10:00:00Z",
            "details": {"ip_address": "192.168.1.1"},
        }
        message_body = json.dumps(event_data).encode("utf-8")

        result = processor.process(message_body)

        assert result is True
        assert len(mock_storage.saved_events) == 1

        saved_event = mock_storage.saved_events[0]
        assert isinstance(saved_event, EnrichedUserActivityEvent)
        assert saved_event.user_id == "user-001"
        assert saved_event.event_type == "login"
        assert saved_event.timestamp == datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc)
        assert saved_event.details == {"ip_address": "192.168.1.1"}
        assert saved_event.event_id is not None
        assert len(saved_event.event_id) == 36
        assert saved_event.processing_timestamp is not None

    def test_process_missing_required_field(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)

        event_data = {
            "user_id": "user-001",
        }
        message_body = json.dumps(event_data).encode("utf-8")

        result = processor.process(message_body)

        assert result is False
        assert len(mock_storage.saved_events) == 0

    def test_process_invalid_json(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)

        message_body = b"this is not json"

        result = processor.process(message_body)

        assert result is False
        assert len(mock_storage.saved_events) == 0

    def test_process_non_utf8_bytes(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)

        message_body = b"\xff\xfe\xfd"

        result = processor.process(message_body)

        assert result is False
        assert len(mock_storage.saved_events) == 0

    def test_process_storage_failure(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)
        mock_storage.should_fail = True

        event_data = {
            "user_id": "user-001",
            "event_type": "login",
            "timestamp": "2023-10-27T10:00:00Z",
        }
        message_body = json.dumps(event_data).encode("utf-8")

        result = processor.process(message_body)

        assert result is False

    def test_idempotent_event_id_generation(self, mock_storage):
        processor = EventProcessor(storage=mock_storage)

        event_data = {
            "user_id": "user-001",
            "event_type": "login",
            "timestamp": "2023-10-27T10:00:00Z",
        }
        message_body = json.dumps(event_data).encode("utf-8")

        processor.process(message_body)
        processor.process(message_body)

        assert len(mock_storage.saved_events) == 2
        first_id = mock_storage.saved_events[0].event_id
        second_id = mock_storage.saved_events[1].event_id
        assert first_id != second_id