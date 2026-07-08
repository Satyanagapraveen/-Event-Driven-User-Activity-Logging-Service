import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from app.routers.activity_logs import router as activity_logs_router
from app.services.nosql_query import NoSqlQuery

load_dotenv()

query_service: NoSqlQuery


@asynccontextmanager
async def lifespan(app: FastAPI):
    global query_service
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db_name = os.getenv("MONGO_DB_NAME", "user_activity_logs")

    query_service = NoSqlQuery(
        mongo_uri=mongo_uri,
        db_name=mongo_db_name,
    )
    await query_service.connect()
    logging.info("Query service connected")
    yield
    await query_service.disconnect()
    logging.info("Query service disconnected")


app = FastAPI(
    title="User Activity Logging API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(activity_logs_router)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}