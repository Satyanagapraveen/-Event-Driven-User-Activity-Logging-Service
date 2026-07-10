from typing import List, Dict, Any
import logging
from fastapi import APIRouter, Depends, HTTPException

from app.models import ActivityLogQueryParams
from app.services.nosql_query import NoSqlQuery
logger=logging.getLogger(__name__)
def get_query_service() -> NoSqlQuery:
    from app.main import query_service
    return query_service
router = APIRouter(prefix="/api/v1", tags=["activity-logs"])

@router.get("/activity-logs", response_model=List[Dict[str, Any]])
async def get_activity_logs(
    params: ActivityLogQueryParams = Depends(),
    query_service: NoSqlQuery = Depends(get_query_service),
):
    try:
        documents = await query_service.fetch_activity_logs(params)
        return documents
    except Exception as e:
        logger.exception("Failed to fetch activity logs")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch activity logs: {str(e)}",
        )