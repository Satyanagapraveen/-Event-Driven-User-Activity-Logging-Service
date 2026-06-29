from datetime import datetime, timezone
from typing import Optional, Any
from uuid import uuid4

from pydantic import BaseModel, Field


class RawUserActivityEvent(BaseModel):
    user_id: str
    event_type: str
    timestamp: datetime
    details: Optional[dict[str, Any]] = None


class EnrichedUserActivityEvent(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: datetime
    processing_timestamp: datetime
    details: Optional[dict[str, Any]] = None