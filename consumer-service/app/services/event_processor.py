import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import ValidationError

from app.models import RawUserActivityEvent, EnrichedUserActivityEvent

logger = logging.getLogger(__name__)


class EventProcessor:
    def __init__(self, storage):
        self.storage = storage

    async def process(self, message_body: bytes) -> bool:
        raw_event = self._parse_and_validate(message_body)
        if raw_event is None:
            return False

        enriched_event = self._enrich(raw_event)

        success = await self.storage.save_event(enriched_event)
        return success

    def _parse_and_validate(self, message_body: bytes) -> RawUserActivityEvent | None:
        try:
            body_str = message_body.decode("utf-8")
        except UnicodeDecodeError:
            logger.error(
                "Failed to decode message body as UTF-8",
                extra={"message_preview": str(message_body[:100])},
            )
            return None

        try:
            data = json.loads(body_str)
        except json.JSONDecodeError:
            logger.error(
                "Failed to parse message body as JSON",
                extra={"message_preview": body_str[:200]},
            )
            return None

        try:
            event = RawUserActivityEvent(**data)
        except ValidationError as e:
            logger.error(
                "Event validation failed",
                extra={
                    "validation_errors": e.errors(),
                    "message_preview": body_str[:200],
                },
            )
            return None

        return event

    def _enrich(self, event: RawUserActivityEvent) -> EnrichedUserActivityEvent:
        enriched = EnrichedUserActivityEvent(
            event_id=str(uuid4()),
            user_id=event.user_id,
            event_type=event.event_type,
            timestamp=event.timestamp,
            processing_timestamp=datetime.now(timezone.utc),
            details=event.details,
        )
        logger.info(
            "Event enriched",
            extra={
                "event_id": enriched.event_id,
                "event_type": enriched.event_type,
                "user_id": enriched.user_id,
            },
        )
        return enriched