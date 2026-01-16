import os
import json
import time
import socket
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
WORKER_ID = socket.gethostname()


def _db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def claim_job():
    q = """
    WITH cte AS (
      SELECT job_id
      FROM jobs
      WHERE status='queued'
        AND run_after <= now()
      ORDER BY created_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE jobs
    SET status='running', locked_by=%s, locked_at=now(), updated_at=now()
    WHERE job_id IN (SELECT job_id FROM cte)
    RETURNING job_id, tenant_id, run_id, type, payload, attempts, max_attempts;
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (WORKER_ID,))
            row = cur.fetchone()
        conn.commit()
    return row


def mark_run_status(tenant_id: str, run_id: str, status: str, error: str | None = None):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE analysis_runs SET status=%s, updated_at=now(), error=%s WHERE tenant_id=%s AND run_id=%s",
                (status, error, tenant_id, run_id),
            )
        conn.commit()


def add_evidence(tenant_id: str, run_id: str, evidence_id: str, ev_type: str, content: dict):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO evidence_items (evidence_id, tenant_id, run_id, type, content) VALUES (%s,%s,%s,%s,%s::jsonb)",
                (evidence_id, tenant_id, run_id, ev_type, json.dumps(content)),
            )
        conn.commit()


def mark_job_done(job_id: str):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET status='done', updated_at=now() WHERE job_id=%s", (job_id,))
        conn.commit()


def mark_job_failed(job_id: str, attempts: int, max_attempts: int, err: str):
    backoff = [5, 20, 60]
    delay = backoff[min(attempts, len(backoff)-1)]
    status = 'queued' if attempts + 1 < max_attempts else 'failed'
    run_after_expr = f"now() + interval '{delay} seconds'" if status == 'queued' else 'now()'

    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE jobs
                SET status=%s,
                    attempts=%s,
                    last_error=%s,
                    locked_by=NULL,
                    locked_at=NULL,
                    run_after={run_after_expr},
                    updated_at=now()
                WHERE job_id=%s
                """,
                (status, attempts + 1, err[:2000], job_id),
            )
        conn.commit()


def run_demo_analysis(tenant_id: str, run_id: str):
    # Demo: create a tiny evidence payload (replace with real ClimSystems API calls)
    ev_id = f"evi_demo_{int(time.time()*1000)}"
    content = {
        "dataset_version": "demo_v1",
        "scenario": ["ssp245", "ssp585"],
        "time_slices": ["baseline", "2030s", "2050s"],
        "percentile": 50,
        "note": "Demo evidence produced by worker. Replace with ClimSystems API results.",
    }
    add_evidence(tenant_id, run_id, ev_id, "generated", content)


def main():
    print(f"Worker starting: {WORKER_ID}")
    while True:
        job = None
        try:
            job = claim_job()
            if not job:
                time.sleep(1.0)
                continue

            job_id, tenant_id, run_id, jtype, payload, attempts, max_attempts = job
            print(f"Claimed job {job_id} type={jtype} run_id={run_id}")

            if run_id:
                mark_run_status(tenant_id, run_id, 'running')

            if jtype == 'RUN_ANALYSIS':
                run_demo_analysis(tenant_id, run_id)

            if run_id:
                mark_run_status(tenant_id, run_id, 'done')

            mark_job_done(job_id)
        except Exception as e:
            err = repr(e)
            print(f"Job error: {err}")
            if job:
                job_id, tenant_id, run_id, _, _, attempts, max_attempts = job
                if run_id:
                    mark_run_status(tenant_id, run_id, 'failed', error=err)
                mark_job_failed(job_id, attempts, max_attempts, err)
            else:
                time.sleep(2.0)


if __name__ == '__main__':
    main()
