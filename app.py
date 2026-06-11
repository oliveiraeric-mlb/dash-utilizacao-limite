import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from bq_client import get_dashboard_data

app = FastAPI(title="Dashboard Utilização Limite TC")


@app.get("/api/data")
async def data():
    try:
        return JSONResponse(content=get_dashboard_data())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# Serve the dashboard HTML at /
app.mount("/", StaticFiles(directory="static", html=True), name="static")
