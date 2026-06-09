# ============================================================
# app/database.py — PostgreSQL access for the portal
# ============================================================
# psycopg2-based (matches the existing routers' import
# `from app.database import get_db_connection`) plus small,
# defensive query helpers used by the dashboard router.
# ============================================================
import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse

from app.config import settings

logger = logging.getLogger("portal.db")


def _dsn_from_url(url: str) -> dict:
    """Parse postgresql://user:pass@host:port/db into psycopg2 kwargs."""
    p = urlparse(url)
    return {
        "host": p.hostname or "steel-postgres",
        "port": p.port or 5432,
        "dbname": (p.path or "/steel_db").lstrip("/") or "steel_db",
        "user": p.username or "steel_admin",
        "password": p.password or "",
    }


_CONN_KWARGS = _dsn_from_url(settings.DATABASE_URL)


def get_db_connection(retries: int = 5, backoff: float = 1.5):
    """
    Open a new connection. Retries with backoff so the portal survives
    Postgres still warming up inside the compose network.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(connect_timeout=5, **_CONN_KWARGS)
        except Exception as e:
            last_err = e
            logger.warning(f"DB connect attempt {attempt}/{retries} failed: {e}")
            time.sleep(backoff * attempt)
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_err}")


def fetch_all(sql: str, params: tuple = ()):
    """Run a parameterised SELECT and return a list of dicts. Never raises
    to the caller for routine query problems — returns [] and logs."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"fetch_all error: {e} | sql={sql[:120]}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_one(sql: str, params: tuple = ()):
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: tuple = ()):
    """Run a parameterised INSERT/UPDATE/DELETE. Returns rows-affected count."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql, params)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        return affected
    except Exception as e:
        logger.error(f"execute error: {e} | sql={sql[:120]}")
        return 0
    finally:
        if conn:
            conn.close()


def healthcheck() -> bool:
    try:
        conn = get_db_connection(retries=1)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False
