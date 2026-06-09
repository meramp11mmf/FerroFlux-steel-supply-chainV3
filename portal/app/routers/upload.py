# ============================================================
# app/routers/upload.py — tenant-scoped data upload (REFACTORED)
# ============================================================
# The company is taken from the verified token (not the form), and
# the chosen factory must belong to that company, so an upload can
# never inject rows into another tenant's data. Rows are tagged with
# company_id + factory_id and the ETL DAG is auto-triggered.
# ============================================================
import io
import logging
from datetime import datetime

import pandas as pd
import requests
from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Depends
from fastapi.responses import JSONResponse, StreamingResponse

from app.database import get_db_connection
from app.security import current_tenant, TenantContext
from app.config import settings

router = APIRouter()
logger = logging.getLogger("portal.upload")

ALLOWED_TYPES = {
    "market": {"required_cols": ["date", "steel_price_egypt_egp", "iron_ore_price_usd",
               "scrap_price_usd", "usd_egp_rate", "natural_gas_price_usd",
               "brent_oil_usd", "electricity_price_egp_kwh"],
               "table": "raw_data.market_prices", "description": "Market Prices"},
    "production": {"required_cols": ["batch_id", "date", "facility", "production_line",
                   "shift", "planned_tons", "actual_tons", "efficiency_pct"],
                   "table": "raw_data.production", "description": "Production Data"},
    "orders": {"required_cols": ["order_id", "order_date", "customer_id", "product_type",
               "quantity_tons", "price_per_ton_egp", "delivery_governorate"],
               "table": "raw_data.orders", "description": "Orders Data"},
    "shipments": {"required_cols": ["shipment_id", "order_id", "origin", "destination",
                  "transport_mode", "weight_tons", "transport_cost_egp"],
                  "table": "raw_data.shipments", "description": "Shipments Data"},
    "raw_materials": {"required_cols": ["purchase_id", "material_type", "supplier_name",
                      "quantity_tons", "price_per_ton_usd", "purchase_date"],
                      "table": "raw_data.raw_materials", "description": "Raw Materials Data"},
}


def _resolve_factory(conn, company_id, factory_id):
    cur = conn.cursor()
    cur.execute("SELECT company_id FROM tenants.factories WHERE factory_id = %s", (factory_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise HTTPException(status_code=400, detail=f"Unknown factory_id: {factory_id}")
    if row[0] != company_id:
        raise HTTPException(status_code=403,
                            detail=f"Factory {factory_id} does not belong to your company")


def _trigger_etl(company_id, factory_id):
    try:
        run_id = f"upload__{factory_id}__{datetime.now().strftime('%Y%m%dT%H%M%S')}"
        resp = requests.post(
            f"{settings.AIRFLOW_BASE_URL}/dags/{settings.ETL_DAG_ID}/dagRuns",
            auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
            json={"dag_run_id": run_id,
                  "conf": {"company_id": company_id, "factory_id": factory_id,
                           "triggered_by": "portal_upload"}},
            timeout=10)
        if resp.status_code in (200, 201):
            return {"triggered": True, "dag_run_id": run_id}
        return {"triggered": False, "reason": f"Airflow HTTP {resp.status_code}"}
    except Exception as e:
        logger.warning(f"Airflow trigger failed: {e}")
        return {"triggered": False, "reason": str(e)}


@router.post("/excel/{data_type}")
async def upload_excel(data_type: str,
                       file: UploadFile = File(...),
                       factory_id: str = Form(...),
                       auto_process: bool = Form(True),
                       tenant: TenantContext = Depends(current_tenant)):
    if data_type not in ALLOWED_TYPES:
        raise HTTPException(status_code=400,
                            detail=f"Invalid data type. Choose from: {list(ALLOWED_TYPES.keys())}")
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls or .csv files allowed")

    company_id = tenant.company_id  # from token, NOT client input

    conn = None
    try:
        contents = await file.read()
        try:
            df = (pd.read_csv(io.BytesIO(contents)) if file.filename.lower().endswith(".csv")
                  else pd.read_excel(io.BytesIO(contents)))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

        config = ALLOWED_TYPES[data_type]
        missing = [c for c in config["required_cols"] if c not in df.columns]
        if missing:
            return JSONResponse(status_code=422, content={
                "status": "error", "message": f"Missing required columns: {missing}",
                "your_columns": list(df.columns), "required_columns": config["required_cols"]})
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="File is empty")

        # never trust tenant columns from the file
        for reserved in ("company_id", "factory_id"):
            if reserved in df.columns:
                df = df.drop(columns=[reserved])

        conn = get_db_connection()
        _resolve_factory(conn, company_id, factory_id)
        cur = conn.cursor()

        cols = list(df.columns) + ["company_id", "factory_id"]
        placeholders = ",".join(["%s"] * len(cols))
        col_names = ",".join(cols)

        inserted, errors = 0, 0
        for _, row in df.iterrows():
            try:
                values = [None if pd.isna(v) else v for v in row.values] + [company_id, factory_id]
                cur.execute(
                    f"INSERT INTO {config['table']} ({col_names}) VALUES ({placeholders}) "
                    f"ON CONFLICT DO NOTHING", values)
                inserted += 1
            except Exception as row_err:
                errors += 1
                logger.warning(f"Row error: {row_err}")
        conn.commit()
        cur.close()

        trigger = (_trigger_etl(company_id, factory_id) if auto_process
                   else {"triggered": False, "reason": "auto_process=false"})
        return {"status": "success",
                "message": f"{config['description']} uploaded for {factory_id}",
                "company_id": company_id, "factory_id": factory_id,
                "rows_processed": len(df), "rows_inserted": inserted, "rows_skipped": errors,
                "etl_trigger": trigger}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
    finally:
        if conn:
            conn.close()


@router.get("/templates/{data_type}")
async def download_template(data_type: str):
    if data_type not in ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail="Invalid data type")
    config = ALLOWED_TYPES[data_type]
    sample = {col: [f"sample_{col}_1", f"sample_{col}_2"] for col in config["required_cols"]}
    df = pd.DataFrame(sample)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=data_type)
    output.seek(0)
    return StreamingResponse(
        output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={data_type}_template.xlsx"})
