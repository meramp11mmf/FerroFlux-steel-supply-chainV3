# ============================================================
# app/routers/dashboard.py — authenticated, tenant-isolated metrics
# ============================================================
# Every query derives its company/factory scope from the verified
# token via `current_tenant`, applied as BOUND parameters. There is
# no code path where a client can widen its own scope.
# ============================================================
import logging
from fastapi import APIRouter, Depends, Query

from app.database import fetch_all, fetch_one
from app.security import current_tenant, TenantContext

router = APIRouter()
logger = logging.getLogger("portal.dashboard")


@router.get("/summary")
async def summary(tenant: TenantContext = Depends(current_tenant)):
    """Headline KPIs for the authenticated tenant only."""
    frag, params = tenant.where()
    row = fetch_one(f"""
        SELECT
            COALESCE(SUM(total_production_tons), 0)  AS total_production_tons,
            COALESCE(SUM(total_revenue_egp), 0)      AS total_revenue_egp,
            COALESCE(SUM(total_orders), 0)           AS total_orders,
            COALESCE(AVG(avg_efficiency), 0)         AS avg_efficiency,
            COALESCE(SUM(total_co2_kg), 0)           AS total_co2_kg,
            COALESCE(SUM(profit_estimate_egp), 0)    AS profit_estimate_egp
        FROM analytics.daily_kpis
        WHERE {frag}
    """, params) or {}
    return {"tenant": tenant.as_dict(), "kpis": row}


@router.get("/daily")
async def daily(tenant: TenantContext = Depends(current_tenant),
                limit: int = Query(90, ge=1, le=730)):
    """Daily KPI time-series for charts (most recent `limit` days)."""
    frag, params = tenant.where()
    rows = fetch_all(f"""
        SELECT date, total_production_tons, total_revenue_egp,
               avg_efficiency, steel_price_egp, total_co2_kg, profit_estimate_egp
        FROM analytics.daily_kpis
        WHERE {frag}
        ORDER BY date DESC
        LIMIT %s
    """, params + (limit,))
    rows.reverse()  # chronological for plotting
    return {"count": len(rows), "series": rows}


@router.get("/production-lines")
async def production_lines(tenant: TenantContext = Depends(current_tenant)):
    frag, params = tenant.where()
    rows = fetch_all(f"""
        SELECT facility, production_line, line_type, total_output_tons,
               avg_efficiency, downtime_pct, best_shift, worst_shift
        FROM analytics.production_efficiency
        WHERE {frag}
        ORDER BY avg_efficiency ASC
    """, params)
    return {"count": len(rows), "lines": rows}


@router.get("/regional-demand")
async def regional_demand(tenant: TenantContext = Depends(current_tenant)):
    frag, params = tenant.where()
    rows = fetch_all(f"""
        SELECT governorate, region, total_orders, total_quantity_tons,
               total_revenue_egp, avg_delivery_days, delay_pct, top_product
        FROM analytics.regional_demand
        WHERE {frag}
        ORDER BY total_revenue_egp DESC
    """, params)
    return {"count": len(rows), "regions": rows}


@router.get("/suppliers")
async def suppliers(tenant: TenantContext = Depends(current_tenant)):
    frag, params = tenant.where()
    rows = fetch_all(f"""
        SELECT supplier_name, origin_country, material_type, total_purchases,
               avg_price_per_ton_usd, avg_lead_time_days, on_time_pct, risk_score
        FROM analytics.supplier_scorecard
        WHERE {frag}
        ORDER BY risk_score DESC
    """, params)
    return {"count": len(rows), "suppliers": rows}


@router.get("/price-alerts")
async def price_alerts(tenant: TenantContext = Depends(current_tenant),
                       limit: int = Query(20, ge=1, le=200)):
    """Recent price-spike rows (drives the alert badge)."""
    frag, params = tenant.where()
    rows = fetch_all(f"""
        SELECT date, steel_price_egypt_egp, price_change_pct
        FROM processed_data.market_clean
        WHERE {frag} AND is_price_spike = 1
        ORDER BY date DESC
        LIMIT %s
    """, params + (limit,))
    return {"count": len(rows), "alerts": rows}
