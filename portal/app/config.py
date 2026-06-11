# ============================================================
# app/config.py — central, env-driven configuration
# ============================================================
# Everything that varies by environment is read from env vars with
# safe defaults, so the same image runs unchanged inside the
# steel-network compose stack or on a developer laptop.
# ============================================================
import os


def _build_database_url() -> str:
    """
    Prefer an explicit DATABASE_URL, otherwise assemble one from parts.
    NOTE: defaults align with the Spark ETL credentials (steel_admin /
    steel_pass_2024). The previous compose used 'steel_pass' which did
    NOT match the DB — that mismatch is fixed by the override compose.
    """
    explicit = os.getenv("DATABASE_URL", "").strip()
    if explicit:
        return explicit
    host = os.getenv("PG_HOST", "steel-postgres")
    port = os.getenv("PG_PORT", "5432")
    db   = os.getenv("PG_DB", "steel_db")
    user = os.getenv("PG_USER", os.getenv("POSTGRES_USER", "steel_admin"))
    pwd  = os.getenv("PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "steel_pass_2024"))
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


_WEAK_KEYS = {"change-me-in-prod-please-32+chars", "change-me-in-production", ""}
_WEAK_AIRFLOW_PASSES = {"admin123", ""}


class Settings:
    DATABASE_URL: str = _build_database_url()

    # Auth / token signing
    SECRET_KEY: str = os.getenv("PORTAL_SECRET_KEY", "change-me-in-prod-please-32+chars")
    TOKEN_TTL_SECONDS: int = int(os.getenv("PORTAL_TOKEN_TTL", "28800"))  # 8h

    # Fallback demo admin (used only if tenants.users row is absent)
    ADMIN_USER: str = os.getenv("PORTAL_USER", "admin")
    ADMIN_PASS: str = os.getenv("PORTAL_PASS", "steel2024")

    # Airflow REST API (auto-trigger ETL after an upload / factory add)
    AIRFLOW_BASE_URL: str = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080/api/v1")
    AIRFLOW_USER: str = os.getenv("AIRFLOW_USER", "admin")
    # No default — must be set explicitly; startup aborts below if missing/weak
    AIRFLOW_PASS: str = os.getenv("AIRFLOW_PASSWORD", "")
    ETL_DAG_ID: str = os.getenv("ETL_DAG_ID", "steel_production_etl")

    # n8n webhook for portal-side events (e.g. new factory onboarded)
    N8N_WEBHOOK_URL: str = os.getenv("N8N_WEBHOOK_URL", "").strip()

    # Kafka (for the public live-demo open stream, if wired)
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


settings = Settings()

# Fail fast on insecure defaults so bad config is caught at container start
if settings.SECRET_KEY in _WEAK_KEYS or len(settings.SECRET_KEY) < 32:
    raise RuntimeError(
        "PORTAL_SECRET_KEY is missing, too short (<32 chars), or still set to the default. "
        "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
    )

if settings.AIRFLOW_PASS in _WEAK_AIRFLOW_PASSES:
    raise RuntimeError(
        "AIRFLOW_PASSWORD is not set or is still 'admin123'. "
        "Set a strong password in your .env file."
    )
