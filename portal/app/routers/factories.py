# ============================================================
# app/routers/factories.py — register / list factory units (Mode B)
# ============================================================
# Lets an authenticated tenant admin register new factory units for
# THEIR OWN company. The company_id is taken from the verified token,
# never from the request body, so an admin cannot create a factory
# under someone else's company. Inserts use the required idempotent
# ON CONFLICT (factory_id) DO UPDATE pattern.
# ============================================================
import re
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, constr

from app.database import get_db_connection, fetch_all
from app.security import current_tenant, TenantContext
from app.config import settings

router = APIRouter()
logger = logging.getLogger("portal.factories")

# Egyptian governorates accepted for a factory unit
VALID_GOVERNORATES = {
    "Cairo", "Giza", "Qalyubia", "Alexandria", "Beheira", "Matrouh",
    "Dakahlia", "Sharqia", "Gharbia", "Monufia", "Kafr_El_Sheikh", "Damietta",
    "Suez", "Ismailia", "Port_Said", "Fayoum", "Beni_Suef", "Minya",
    "Assiut", "Sohag", "Qena", "Luxor", "Aswan", "Red_Sea", "South_Sinai",
    "North_Sinai", "New_Valley", "Multiple",
}


class FactoryCreate(BaseModel):
    factory_name: constr(strip_whitespace=True, min_length=2, max_length=120)
    governorate: constr(strip_whitespace=True, min_length=2, max_length=40)
    # optional explicit suffix; otherwise derived from the name
    factory_code: constr(strip_whitespace=True, min_length=0, max_length=20) = ""


def _slug(text: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", text.upper()).strip("_")
    return s[:20] or "UNIT"


@router.get("")
async def list_my_factories(tenant: TenantContext = Depends(current_tenant)):
    """All factory units belonging to the caller's company."""
    rows = fetch_all(
        "SELECT factory_id, factory_name, governorate "
        "FROM tenants.factories WHERE company_id = %s ORDER BY factory_name",
        (tenant.company_id,))
    return {"company_id": tenant.company_id, "factories": rows}


@router.post("")
async def create_factory(body: FactoryCreate,
                         tenant: TenantContext = Depends(current_tenant)):
    """Register a new factory unit under the caller's company (admin/manager)."""
    if tenant.role not in ("admin", "manager"):
        raise HTTPException(status_code=403, detail="Admin or manager role required")

    # ---- explicit validation ----
    if body.governorate not in VALID_GOVERNORATES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid governorate '{body.governorate}'. "
                   f"Expected one of the Egyptian governorates.")

    suffix = _slug(body.factory_code) if body.factory_code else _slug(body.factory_name)
    factory_id = f"{tenant.company_id}_{suffix}"

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # idempotent upsert — REQUIRED ON CONFLICT pattern
        cur.execute("""
            INSERT INTO tenants.factories (factory_id, company_id, factory_name, governorate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (factory_id) DO UPDATE
              SET factory_name = EXCLUDED.factory_name,
                  governorate  = EXCLUDED.governorate
            RETURNING factory_id, company_id, factory_name, governorate
        """, (factory_id, tenant.company_id, body.factory_name, body.governorate))
        row = cur.fetchone()
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"create_factory error: {e}")
        raise HTTPException(status_code=500, detail="Could not register factory unit.")
    finally:
        if conn:
            conn.close()

    # optional: notify n8n that a new factory was onboarded
    _notify_factory_onboarded(tenant.company_id, factory_id, body)

    return {"status": "success",
            "message": f"Factory unit '{body.factory_name}' registered.",
            "factory": {"factory_id": row[0], "company_id": row[1],
                        "factory_name": row[2], "governorate": row[3]}}


def _notify_factory_onboarded(company_id, factory_id, body):
    if not settings.N8N_WEBHOOK_URL:
        return
    try:
        import requests
        requests.post(settings.N8N_WEBHOOK_URL, timeout=6, json={
            "event_type": "factory_onboarded",
            "payload": {"company_id": company_id, "factory_id": factory_id,
                        "factory_name": body.factory_name,
                        "governorate": body.governorate}})
    except Exception as e:
        logger.warning(f"n8n factory_onboarded notify failed: {e}")
