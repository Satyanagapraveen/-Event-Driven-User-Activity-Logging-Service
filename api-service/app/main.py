from fastapi import FastAPI

app = FastAPI(title="User Activity Logging API", version="0.1.0")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/api/v1/activity-logs")
async def get_activity_logs():
    return []