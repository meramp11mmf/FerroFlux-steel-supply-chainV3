# ============================================================
# FERROFLUX — TENANT CONTEXT HELPER (dashboard)
# ============================================================
# Drop this file next to app.py (scripts/dashboards/tenant.py).
#
# It provides:
#   - the sidebar tenant selector (company + factory)
#   - tfilter(): builds a WHERE fragment for the chosen tenant
#   - twrap(): injects the tenant filter into an existing query
#
# DESIGN: 100% backward compatible.
#   - If the viewer chooses "All Factories (Company View)" we filter
#     by company_id only  -> aggregates across that company's factories.
#   - If they choose a specific factory we filter by both.
#   - The demo data is company EZZ / factory EZZ_DEMO, so the default
#     view shows exactly what the dashboard showed before.
# ============================================================
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text


@st.cache_resource
def get_engine():
    # Connection URL built from env vars — no hardcoded credentials
    import os as _os
    _u = _os.getenv("PG_USER", _os.getenv("POSTGRES_USER", "steel_admin"))
    _p = _os.getenv("PG_PASSWORD", _os.getenv("POSTGRES_PASSWORD", ""))
    _h = _os.getenv("PG_HOST", "steel-postgres")
    _d = _os.getenv("PG_DB", "steel_db")
    _port = _os.getenv("PG_PORT", "5432")
    return create_engine(
        f"postgresql+psycopg2://{_u}:{_p}@{_h}:{_port}/{_d}",
        pool_size=10, max_overflow=20
    )


def load_companies():
    """Return list of (company_id, company_name)."""
    try:
        df = pd.read_sql("SELECT company_id, company_name FROM tenants.companies ORDER BY company_name", get_engine())
        return list(df.itertuples(index=False, name=None))
    except Exception:
        # tenants schema not migrated yet -> behave like single-tenant
        return [("EZZ", "EZZ Steel Group")]


def load_factories(company_id):
    """Return list of (factory_id, factory_name) for a company."""
    try:
        df = pd.read_sql(
            text(
                "SELECT factory_id, factory_name FROM tenants.factories "
                "WHERE company_id = :c AND factory_id != 'ALL_FACTORIES' "
                "ORDER BY factory_name"
            ),
            get_engine(), params={"c": company_id})
        return list(df.itertuples(index=False, name=None))
    except Exception:
        return [("EZZ_DEMO", "EZZ Demo (All Simulation Data)")]


def tenant_selector():
    """
    Render the company/factory selector in the sidebar.
    Returns a dict describing the current tenant scope.
    """
    st.markdown("### \U0001f3e2 Tenant View")

    companies = load_companies()
    comp_labels = [name for _, name in companies]
    comp_ids = [cid for cid, _ in companies]
    ci = st.selectbox("Company", comp_labels, index=0, key="tenant_company")
    company_id = comp_ids[comp_labels.index(ci)]

    factories = load_factories(company_id)
    # "All Factories" first → company-level rollup
    fac_labels = ["\U0001f310 All Factories (Company View)"] + [name for _, name in factories]
    fac_ids = [None] + [fid for fid, _ in factories]
    fi = st.selectbox("Factory / Branch", fac_labels, index=0, key="tenant_factory")
    factory_id = fac_ids[fac_labels.index(fi)]

    return {
        "company_id": company_id,
        "factory_id": factory_id,                 # None => all factories of the company
        "company_name": ci,
        "factory_name": fi,
        "is_company_view": factory_id is None,
    }


def tfilter(tenant, alias=""):
    """
    Build a WHERE fragment (always starts with ' AND ...') for the tenant.
    `alias` lets you prefix the column (e.g. 'k' -> 'k.company_id').
    """
    p = f"{alias}." if alias else ""
    frag = f" AND {p}company_id = '{tenant['company_id']}'"
    if tenant["factory_id"] is not None:
        frag += f" AND {p}factory_id = '{tenant['factory_id']}'"
    return frag


def twrap(query, tenant, alias=""):
    """
    Inject the tenant filter into an existing query.

    Rules:
      - If the query contains 'WHERE 1=1' we append the filter right after it.
      - Else if it has a 'GROUP BY' we insert a WHERE before it.
      - Else if it has 'ORDER BY' we insert a WHERE before it.
      - Else we append a WHERE at the end.
    This keeps every existing query working unchanged.
    """
    filt = tfilter(tenant, alias).strip()  # 'AND company_id = ...'
    where_clause = filt[4:]                 # strip leading 'AND '

    q = query
    upper = q.upper()

    if "WHERE 1=1" in upper:
        idx = upper.index("WHERE 1=1") + len("WHERE 1=1")
        return q[:idx] + " " + filt + " " + q[idx:]

    if " WHERE " in upper:
        # already has a WHERE -> just AND our filter in right after it
        idx = upper.index(" WHERE ") + len(" WHERE ")
        return q[:idx] + "(" + where_clause + ") AND " + q[idx:]

    # no WHERE: insert before GROUP BY / ORDER BY / end
    for kw in (" GROUP BY ", " ORDER BY "):
        if kw in upper:
            idx = upper.index(kw)
            return q[:idx] + " WHERE " + where_clause + " " + q[idx:]

    return q + " WHERE " + where_clause


def tenant_banner(tenant):
    """A small caption describing the active view, for the page header."""
    if tenant["is_company_view"]:
        return f"Company View · {tenant['company_name']} · all factories combined"
    return f"Factory View · {tenant['factory_name']}"
