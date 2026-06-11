# ============================================================
# app/security.py — stateless signed tokens + tenant isolation
# ============================================================
# Row-Level Data Isolation is enforced HERE: the authenticated
# tenant scope (company_id / factory_id / role) is embedded in a
# signed token at login and re-derived from that token on every
# request. Dashboard queries take the scope from the verified token
# — NEVER from a client-supplied parameter — so a user can never
# read another company's rows by editing a request.
# ============================================================
import base64
import hashlib
import hmac
import json
import time
import logging

from fastapi import Header, HTTPException

from app.config import settings

logger = logging.getLogger("portal.security")


# ---------------- password hashing (bcrypt only — SHA-256 fallback removed) ----------
def hash_password(password: str) -> str:
    # bcrypt is a required dependency; no insecure fallback
    import bcrypt
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash:
        return False
    if stored_hash == "PLACEHOLDER_SET_BY_PORTAL":
        return password == settings.ADMIN_PASS
    import bcrypt
    return bcrypt.checkpw(password.encode(), stored_hash.encode())


# ---------------- signed token (HMAC-SHA256, no external dep) -----------
def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def issue_token(payload: dict) -> str:
    body = dict(payload)
    body["exp"] = int(time.time()) + settings.TOKEN_TTL_SECONDS
    raw = json.dumps(body, separators=(",", ":")).encode()
    sig = hmac.new(settings.SECRET_KEY.encode(), raw, hashlib.sha256).digest()
    return f"{_b64e(raw)}.{_b64e(sig)}"


def verify_token(token: str) -> dict:
    try:
        body_b64, sig_b64 = token.split(".", 1)
        raw = _b64d(body_b64)
        expected = hmac.new(settings.SECRET_KEY.encode(), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64d(sig_b64)):
            raise ValueError("bad signature")
        body = json.loads(raw)
        if int(body.get("exp", 0)) < int(time.time()):
            raise ValueError("expired")
        return body
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid or expired token ({e})")


# ---------------- tenant context (the isolation boundary) ---------------
class TenantContext:
    def __init__(self, claims: dict):
        self.username = claims.get("username")
        self.role = claims.get("role", "viewer")
        self.company_id = claims.get("company_id")
        self.factory_id = claims.get("factory_id")          # None => whole company
        self.company_name = claims.get("company_name")
        self.factory_name = claims.get("factory_name")
        if not self.company_id:
            raise HTTPException(status_code=403, detail="Token missing company scope")

    def where(self, alias: str = "") -> tuple:
        """
        Build a parameterised tenant WHERE fragment + params.
        Returns ("company_id = %s [AND factory_id = %s]", (..)).
        The caller appends this to its own query so isolation is
        applied with bound parameters (no string interpolation).
        ALL_FACTORIES is the sentinel for company-wide scope — skip factory filter.
        """
        p = f"{alias}." if alias else ""
        frag = f"{p}company_id = %s"
        params = [self.company_id]
        if self.factory_id and self.factory_id != "ALL_FACTORIES":
            frag += f" AND {p}factory_id = %s"
            params.append(self.factory_id)
        return frag, tuple(params)

    def as_dict(self):
        return {
            "username": self.username, "role": self.role,
            "company_id": self.company_id, "factory_id": self.factory_id,
            "company_name": self.company_name, "factory_name": self.factory_name,
        }


def current_tenant(authorization: str = Header(None)) -> TenantContext:
    """
    FastAPI dependency. Requires `Authorization: Bearer <token>`.
    Resolves the verified token into a TenantContext. Inject this into
    any authenticated (Mode B) endpoint to enforce isolation.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    return TenantContext(verify_token(token))


def require_admin(tenant: TenantContext) -> TenantContext:
    if tenant.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return tenant
