# ============================================================
# app/main.py — FerroFlux steel-portal (FastAPI)
# ============================================================
# Two routing modes:
#   Mode A (public)  : /            portfolio + architecture
#                      /demo        live demo dashboard (mock/open stream)
#                      /api/public/* unauthenticated mock metrics
#   Mode B (secure)  : /login, /app  tenant SPA shell
#                      /api/auth/*    login / register / demo token
#                      /api/dashboard/* tenant-isolated metrics
#                      /api/factories   register factory units
#                      /api/upload/*    tenant-scoped data upload
# ============================================================
import math
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

from app.config import settings
from app.database import healthcheck
from app.routers import auth, dashboard, factories, upload, public_dashboard

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("portal")

app = FastAPI(title="FerroFlux — Steel Supply-Chain Platform", version="3.0.0")

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR    = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ---- API routers ----
app.include_router(auth.router,             prefix="/api/auth",             tags=["auth"])
app.include_router(dashboard.router,        prefix="/api/dashboard",        tags=["dashboard"])
app.include_router(public_dashboard.router, prefix="/api/public/dashboard", tags=["public"])
app.include_router(factories.router,        prefix="/api/factories",        tags=["factories"])
app.include_router(upload.router,           prefix="/api/upload",           tags=["upload"])


# ============================================================
# MODE A — PUBLIC PORTFOLIO + DEMO
# ============================================================
@app.get("/", response_class=HTMLResponse)
async def portfolio(request: Request):
    return templates.TemplateResponse("portfolio.html", {"request": request})


@app.get("/demo", response_class=HTMLResponse)
async def demo_dashboard(request: Request):
    return templates.TemplateResponse("demo.html", {"request": request})


@app.get("/api/public/metrics")
async def public_metrics():
    """
    Open, unauthenticated metrics for the public live-demo dashboard.
    Synthetic but realistic — no signup roadblock, no tenant data leak.
    """
    now = datetime.utcnow()
    days = 60
    base_price = 38000
    series = []
    price = base_price
    for i in range(days):
        d = now - timedelta(days=days - i)
        price += random.uniform(-450, 480) + 120 * math.sin(i / 7)
        throughput = 1800 + 350 * math.sin(i / 5) + random.uniform(-120, 120)
        latency = 3.2 + 0.6 * math.sin(i / 9) + random.uniform(-0.3, 0.3)
        series.append({
            "date": d.strftime("%Y-%m-%d"),
            "steel_price_egp": round(price, 1),
            "throughput_tons": round(max(throughput, 0), 1),
            "supply_chain_latency_days": round(max(latency, 0.5), 2),
        })
    spike = abs(series[-1]["steel_price_egp"] - series[-2]["steel_price_egp"]) > 400
    return {"generated_at": now.isoformat() + "Z", "series": series,
            "headline": {"latest_price": series[-1]["steel_price_egp"],
                         "latest_throughput": series[-1]["throughput_tons"],
                         "price_spike": spike}}


# ============================================================
# MODE B — SECURE TENANT SHELL
# ============================================================
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/app", response_class=HTMLResponse)
async def tenant_app(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


# ---- ops ----
@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "db": "up" if healthcheck() else "down",
                         "time": datetime.utcnow().isoformat() + "Z"})
