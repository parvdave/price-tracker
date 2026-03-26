"""
FastAPI dashboard backend.

Endpoints:
  GET /anomalies  — last 100 anomaly records from TimescaleDB
  GET /health     — service + database status
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://crypto:crypto_pass@timescaledb:5432/crypto_db",
)

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class AnomalyRecord(BaseModel):
    id: int
    symbol: str
    price: float
    mean: float
    stddev: float
    zscore: float
    detected_at: datetime


class HealthResponse(BaseModel):
    status: str
    database: str
    anomaly_count: int


# ---------------------------------------------------------------------------
# Database connection pool — managed via lifespan
# ---------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=10)
    yield
    if _pool:
        await _pool.close()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Crypto Anomaly API",
    description="Real-time crypto price anomaly detection dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/anomalies", response_model=List[AnomalyRecord])
async def get_anomalies():
    """Return the 100 most recent anomaly events."""
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not ready")

    rows = await _pool.fetch(
        """
        SELECT id, symbol, price, mean, stddev, zscore, detected_at
        FROM anomalies
        ORDER BY detected_at DESC
        LIMIT 100
        """,
    )
    return [AnomalyRecord(**dict(row)) for row in rows]


@app.get("/health", response_model=HealthResponse)
async def health():
    """Return service and database health."""
    if _pool is None:
        return HealthResponse(status="degraded", database="unavailable", anomaly_count=0)

    try:
        count: int = await _pool.fetchval("SELECT COUNT(*) FROM anomalies")
        return HealthResponse(
            status="ok",
            database="connected",
            anomaly_count=count,
        )
    except Exception as exc:  # pylint: disable=broad-except
        return HealthResponse(
            status="degraded",
            database=f"error: {exc}",
            anomaly_count=0,
        )
