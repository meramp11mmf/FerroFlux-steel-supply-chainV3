# ============================================================
# app/routers/auth.py — login / register / demo (REFACTORED)
# ============================================================
# Issues HMAC-signed tokens that carry the tenant scope. The token
# is the only thing the dashboard trusts for company/factory scope.
# ============================================================
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, constr

from app.database import get_db_connection, fetch_one
from app.config import settings
from app.security import hash_password, verify_password, issue_token

router = APIRouter()
logger = logging.getLogger("portal.auth")


class LoginRequest(BaseModel):
    username: constr(strip_whitespace=True, min_length=1, max_length=60)
    password: constr(min_length=1, max_length=200)


class RegisterRequest(BaseModel):
    factory_name: constr(strip_whitespace=True, min_length=2, max_length=120)
    contact_name: constr(strip_whitespace=True, min_length=2, max_length=120)
    email: EmailStr
    phone: constr(strip_whitespace=True, min_length=3, max_length=40)
    city: constr(strip_whitespace=True, min_length=2, max_length=40)


def _lookup_user(username: str):
    return fetch_one("""
        SELECT u.user_id, u.username, u.password_hash, u.company_id,
               u.factory_id, u.role, c.company_name, f.factory_name
        FROM tenants.users u
        JOIN tenants.companies c ON c.company_id = u.company_id
        LEFT JOIN tenants.factories f ON f.factory_id = u.factory_id
        WHERE u.username = %s
    """, (username,))


def _ensure_admin_hash():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE tenants.users SET password_hash = %s
            WHERE username = 'admin' AND password_hash = 'PLACEHOLDER_SET_BY_PORTAL'
        """, (hash_password(settings.ADMIN_PASS),))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"_ensure_admin_hash skipped: {e}")


def _token_for(user: dict) -> str:
    return issue_token({
        "username": user["username"],
        "role": user.get("role", "viewer"),
        "company_id": user["company_id"],
        "factory_id": user.get("factory_id"),
        "company_name": user.get("company_name"),
        "factory_name": user.get("factory_name") or "All Factories (Company View)",
    })


@router.post("/login")
async def login(req: LoginRequest):
    user = _lookup_user(req.username)
    if user:
        if verify_password(req.password, user["password_hash"]):
            if user["password_hash"] == "PLACEHOLDER_SET_BY_PORTAL":
                _ensure_admin_hash()
            return {"status": "success", "token": _token_for(user),
                    "user": {k: user.get(k) for k in
                             ("username", "role", "company_id", "company_name",
                              "factory_id", "factory_name")},
                    "message": "Login successful"}
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # legacy fallback demo admin
    if req.username == settings.ADMIN_USER and req.password == settings.ADMIN_PASS:
        demo = {"username": settings.ADMIN_USER, "role": "admin",
                "company_id": "EZZ", "company_name": "EZZ Steel Group",
                "factory_id": "EZZ_DEMO", "factory_name": "EZZ Demo (All Simulation Data)"}
        return {"status": "success", "token": _token_for(demo),
                "user": demo, "message": "Login successful (demo)"}

    raise HTTPException(status_code=401, detail="Invalid credentials")


@router.post("/register")
async def register(req: RegisterRequest):
    """
    Self-service tenant onboarding. Creates company + factory + manager
    user with idempotent ON CONFLICT upserts, scoped to a derived
    company_id so the new tenant's data is isolated from day one.
    """
    base = "".join(ch for ch in req.factory_name.upper() if ch.isalnum())[:12] or "FACTORY"
    company_id = base
    factory_id = f"{base}_MAIN"
    temp_password = "changeme123"

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tenants.companies (company_id, company_name)
            VALUES (%s, %s) ON CONFLICT (company_id) DO NOTHING
        """, (company_id, req.factory_name))
        cur.execute("""
            INSERT INTO tenants.factories (factory_id, company_id, factory_name, governorate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (factory_id) DO UPDATE
              SET factory_name = EXCLUDED.factory_name,
                  governorate  = EXCLUDED.governorate
        """, (factory_id, company_id, req.factory_name, req.city))
        cur.execute("""
            INSERT INTO tenants.users (username, password_hash, company_id, factory_id, role)
            VALUES (%s, %s, %s, %s, 'manager') ON CONFLICT (username) DO NOTHING
        """, (req.email, hash_password(temp_password), company_id, factory_id))
        conn.commit()
        cur.close()
        return {"status": "success",
                "message": f"Workspace created for {req.factory_name}. Log in with your "
                           f"email and the temporary password to upload your data.",
                "company_id": company_id, "factory_id": factory_id,
                "login_username": req.email, "temp_password": temp_password}
    except Exception as e:
        logger.error(f"register error: {e}")
        raise HTTPException(status_code=500, detail="Registration failed — please try again.")
    finally:
        if conn:
            conn.close()


@router.get("/demo")
async def demo_access():
    demo = {"username": "demo", "role": "viewer",
            "company_id": "EZZ", "company_name": "EZZ Steel Group",
            "factory_id": "EZZ_DEMO", "factory_name": "EZZ Demo (All Simulation Data)"}
    return {"status": "success", "token": _token_for(demo), "user": demo,
            "message": "Demo access granted"}
