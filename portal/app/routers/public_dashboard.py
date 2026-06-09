# ============================================================
# app/routers/public_dashboard.py — unauthenticated EZZ demo data
# ============================================================
# These endpoints serve the same shape as /api/dashboard/* but are
# scoped to the EZZ demo company (company_id='EZZ'), require no token,
# and are used exclusively by the public /demo page (Mode A).
# The isolation guarantee is: reads are bounded to WHERE company_id='EZZ'.
# No writes are exposed here.
# ============================================================
import logging
from fastapi import APIRouter, Query

from app.database import fetch_all, fetch_one

router = APIRouter()
logger = logging.getLogger("portal.public_dashboard")

_DEMO_COMPANY = "EZZ"


def _where():
    """Return (fragment, params) scoping to the demo company."""
    return "company_id = %s", (_DEMO_COMPANY,)


@router.get("/summary")
async def public_summary():
    """Headline KPIs for the EZZ demo company."""
    frag, params = _where()
    row = fetch_one(f"""
        SELECT
            COALESCE(SUM(total_production_tons), 0) AS total_production_tons,
            COALESCE(SUM(total_revenue_egp),     0) AS total_revenue_egp,
            COALESCE(SUM(total_orders),          0) AS total_orders,
            COALESCE(AVG(avg_efficiency),        0) AS avg_efficiency,
            COALESCE(SUM(total_co2_kg),          0) AS total_co2_kg,
            COALESCE(SUM(profit_estimate_egp),   0) AS profit_estimate_egp
        FROM analytics.daily_kpis
        WHERE {frag}
    """, params) or {}
    return {"company": _DEMO_COMPANY, "kpis": row}


@router.get("/daily")
async def public_daily(limit: int = Query(90, ge=1, le=730)):
    """Daily KPI time-series (most recent `limit` days) for the demo company."""
    frag, params = _where()
    rows = fetch_all(f"""
        SELECT date, total_production_tons, total_revenue_egp,
               avg_efficiency, steel_price_egp, total_co2_kg, profit_estimate_egp
        FROM analytics.daily_kpis
        WHERE {frag}
        ORDER BY date DESC
        LIMIT %s
    """, params + (limit,))
    rows.reverse()
    return {"count": len(rows), "series": rows}


@router.get("/production-lines")
async def public_production_lines():
    frag, params = _where()
    rows = fetch_all(f"""
        SELECT facility, production_line, line_type, total_output_tons,
               avg_efficiency, downtime_pct, best_shift, worst_shift
        FROM analytics.production_efficiency
        WHERE {frag}
        ORDER BY avg_efficiency ASC
    """, params)
    return {"count": len(rows), "lines": rows}


@router.get("/regional-demand")
async def public_regional_demand():
    frag, params = _where()
    rows = fetch_all(f"""
        SELECT governorate, region, total_orders, total_quantity_tons,
               total_revenue_egp, avg_delivery_days, delay_pct, top_product
        FROM analytics.regional_demand
        WHERE {frag}
        ORDER BY total_revenue_egp DESC
    """, params)
    return {"count": len(rows), "regions": rows}


@router.get("/suppliers")
async def public_suppliers():
    frag, params = _where()
    rows = fetch_all(f"""
        SELECT supplier_name, origin_country, material_type, total_purchases,
               avg_price_per_ton_usd, avg_lead_time_days, on_time_pct, risk_score
        FROM analytics.supplier_scorecard
        WHERE {frag}
        ORDER BY risk_score DESC
    """, params)
    return {"count": len(rows), "suppliers": rows}


@router.get("/price-alerts")
async def public_price_alerts(limit: int = Query(30, ge=1, le=200)):
    """Recent price-spike rows from the shared market data."""
    frag, params = _where()
    rows = fetch_all(f"""
        SELECT date, steel_price_egypt_egp, price_change_pct, is_price_spike
        FROM processed_data.market_clean
        WHERE {frag} AND is_price_spike = 1
        ORDER BY date DESC
        LIMIT %s
    """, params + (limit,))
    return {"count": len(rows), "alerts": rows}
