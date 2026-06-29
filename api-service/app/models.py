from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field
class ActivityLogResponse(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: datetime
    processing_timestamp: datetime
    details: Optional[dict[str, Any]] = None


class ActivityLogQueryParams(BaseModel):
    user_id: Optional[str] = None
    event_type: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = Field(default=10, ge=1, le=100)
    offset: int = Field(default=0, ge=0)