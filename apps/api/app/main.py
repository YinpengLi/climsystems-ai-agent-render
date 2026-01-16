import os
import json
import time
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="ClimSystems AI Agent API", version="0.1.0")

DATABASE_URL = os.getenv("DATABASE_URL")


def _db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def _init_db():
    """Create minimal tables if they don't exist (starter-friendly)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS assets (
      tenant_id TEXT NOT NULL DEFAULT 'default',
      external_id TEXT NOT NULL,
      name TEXT,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (tenant_id, external_id)
    );

    CREATE TABLE IF NOT EXISTS analysis_runs (
      run_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      status TEXT NOT NULL,
      parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS jobs (
      job_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      run_id TEXT,
      type TEXT NOT NULL,
      status TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      attempts INT NOT NULL DEFAULT 0,
      max_attempts INT NOT NULL DEFAULT 3,
      run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
      locked_by TEXT,
      locked_at TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS evidence_items (
      evidence_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL DEFAULT 'default',
      run_id TEXT,
      type TEXT NOT NULL,
      content JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


@app.on_event("startup")
def on_startup():
    _init_db()


@app.get("/health")
def health():
    # Also checks DB connectivity
    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB not ready: {e}")


class AssetIn(BaseModel):
    external_id: str
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class BulkUpsertReq(BaseModel):
    tenant_id: str = "default"
    assets: List[AssetIn]


@app.post("/v1/assets:bulk_upsert")
def bulk_upsert(req: BulkUpsertReq):
    if not req.assets:
        return {"upserted": 0}

    with _db() as conn:
        with conn.cursor() as cur:
            for a in req.assets:
                cur.execute(
                    """
                    INSERT INTO assets (tenant_id, external_id, name, lat, lon, meta)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (tenant_id, external_id)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                      lat = EXCLUDED.lat,
                      lon = EXCLUDED.lon,
                      meta = EXCLUDED.meta,
                      updated_at = now();
                    """,
                    (req.tenant_id, a.external_id, a.name, a.lat, a.lon, json.dumps(a.meta)),
                )
        conn.commit()

    return {"upserted": len(req.assets)}


class CreateRunReq(BaseModel):
    tenant_id: str = "default"
    name: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


@app.post("/v1/runs")
def create_run(req: CreateRunReq):
    run_id = f"run_{int(time.time()*1000)}"
    job_id = f"job_{int(time.time()*1000)}"

    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO analysis_runs (run_id, tenant_id, status, parameters) VALUES (%s,%s,%s,%s::jsonb)",
                (run_id, req.tenant_id, "queued", json.dumps({"name": req.name, **req.parameters})),
            )
            cur.execute(
                """
                INSERT INTO jobs (job_id, tenant_id, run_id, type, status, payload)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (job_id, req.tenant_id, run_id, "RUN_ANALYSIS", "queued", json.dumps({"demo": True})),
            )
        conn.commit()

    return {"run_id": run_id, "status": "queued"}


@app.get("/v1/runs/{run_id}")
def get_run(run_id: str, tenant_id: str = "default"):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id, status, parameters, created_at, updated_at, error FROM analysis_runs WHERE tenant_id=%s AND run_id=%s",
                (tenant_id, run_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="run not found")
            return {
                "run_id": row[0],
                "status": row[1],
                "parameters": row[2],
                "created_at": row[3].isoformat(),
                "updated_at": row[4].isoformat(),
                "error": row[5],
            }


@app.get("/v1/evidence")
def list_evidence(run_id: Optional[str] = None, tenant_id: str = "default"):
    with _db() as conn:
        with conn.cursor() as cur:
            if run_id:
                cur.execute(
                    "SELECT evidence_id, type, created_at, content FROM evidence_items WHERE tenant_id=%s AND run_id=%s ORDER BY created_at DESC",
                    (tenant_id, run_id),
                )
            else:
                cur.execute(
                    "SELECT evidence_id, type, created_at, content FROM evidence_items WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 50",
                    (tenant_id,),
                )
            rows = cur.fetchall()

    return [
        {
            "evidence_id": r[0],
            "type": r[1],
            "created_at": r[2].isoformat(),
            "content": r[3],
        }
        for r in rows
    ]
