# FerroFlux — Big Data Analytics Platform for the Egyptian Steel Supply Chain

An end-to-end data engineering and machine learning platform built around EZZ Steel Group, Egypt's largest steel manufacturer. FerroFlux processes over 86,000 rows of historical data through a three-layer ETL pipeline, trains three ML models, and surfaces insights through an interactive web portal and dashboard.

---

## What is FerroFlux?

FerroFlux is a multi-tenant Big Data platform designed for Egyptian steel companies. It simulates a real production environment with data flowing from raw CSV files all the way to machine learning predictions, real-time Kafka streaming, and automated anomaly alerts.

**Simulated company:** EZZ Steel Group  
**3 factories:** Alexandria (ALEX), Suez (SUEZ), Sadat City (SADAT)  
**24 governorates** covered | **12 suppliers** (domestic and international)

---

## Requirements

| Tool | Version |
|------|---------|
| Docker | 24+ |
| Docker Compose | 2.x |
| Python | 3.8+ |
| WSL2 (Windows users) | Enabled |

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/nourhan-shalaby/FerroFlux-steel-supply-chainV3.git
cd FerroFlux-steel-supply-chainV3
```

### 2. Start all services

```bash
docker compose up -d --build
```

Wait about 2 minutes for everything to initialize, then verify all containers are running:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### 3. Open the control panel

```bash
python manager.py
```

---

## Services & Ports

| Service | URL | Description |
|---------|-----|-------------|
| Portal (FastAPI) | http://localhost:8000 | Main web portal and dashboard |
| Streamlit Dashboard | http://localhost:8501 | Interactive charts and analytics |
| Airflow | http://localhost:8089 | Pipeline scheduling and monitoring |
| Spark Master UI | http://localhost:8081 | Spark job monitoring |
| Kafka UI | http://localhost:8086 | Kafka topic monitoring |
| PgAdmin | http://localhost:5050 | Database management |
| PostgreSQL | localhost:5432 | Main database |

**Default credentials:**
- Airflow: `admin` / `admin123`
- PgAdmin: `admin@ferroflux.com` / `admin123`
- Portal: use the "Demo Login" button on the `/login` page

---

## Control Panel (manager.py)

```
[1] Docker Lifecycle    — Start / Stop / Rebuild containers
[2] Status Check        — View status of all running services
[3] ETL Pipelines       — Run Bronze / Silver / Gold data layers
[4] AI/ML Models        — Run price prediction, demand forecast, supplier risk
[5] Spark Streaming     — Start real-time data processing from Kafka
[6] Kafka Operations    — Run producers and verify topics
[7] Dashboard           — Launch Streamlit dashboard
[8] System Utilities    — Maintenance and repair tools
```

---

## Data Pipeline (Medallion Architecture)

```
Raw CSV Files (86,044 rows)
         ↓
    Bronze Layer     ←  Ingests raw data as Parquet into raw_data schema
         ↓
    Silver Layer     ←  Cleans, enriches, detects anomalies
         ↓
    Gold Layer       ←  Builds 6 aggregated tables for analytics & ML
         ↓
    ML Models        ←  3 trained models with predictions saved to DB
```

### Bronze ETL
Reads five CSV files from `data/raw/` and loads them into PostgreSQL without modification, saving a Parquet copy alongside.

### Silver ETL
Adds computed columns like price change percentage, production efficiency, and shipment delay flags. When it detects anomalies (sudden price spikes or low efficiency), it fires automatic alerts.

### Gold ETL
Builds six aggregated tables used by the dashboard and ML models:

| Table | Description |
|-------|-------------|
| `daily_kpis` | Daily performance indicators |
| `monthly_summary` | Monthly revenue and volume summary |
| `supplier_scorecard` | Supplier performance scores |
| `regional_demand` | Demand breakdown by governorate |
| `production_efficiency` | Efficiency per line and shift |
| `price_features` | Feature-engineered table for price ML model |

---

## Machine Learning Models

### Model 1 — Steel Price Prediction
Predicts future steel prices based on historical market and production data.  
**Algorithm:** Random Forest  
**Results:** MAPE: 5.29% | RMSE: 3,049 EGP

### Model 2 — Weekly Demand Forecasting
Predicts weekly order volume to support production planning.  
**Algorithm:** Gradient Boosted Trees  
**Results:** R²: 0.71 | MAPE: 43.28%

### Model 3 — Supplier Risk Assessment
Classifies each supplier as low / medium / high risk and assigns a numeric risk score.  
**Algorithm:** Random Forest Classifier + Regressor  
**Results:** Accuracy: 94.78% | F1 Score: 0.9468

> **Note:** Gold ETL must run before ML models since Model 1 reads from `analytics.price_features`.

---

## Alert & Notification System

When the Silver ETL detects anomalies in the data:
1. It attempts to send an HTML email via Gmail SMTP to the address registered for the tenant
2. Regardless of whether the email succeeds, the alert is saved to the `etl_alerts` database table
3. Alerts appear on the **Alerts page** inside the Portal, with a badge showing the unread count

This two-layer approach means notifications always reach the portal inbox, even when SMTP is unavailable.

---

## Real-Time Streaming (Kafka)

Four Kafka producers simulate live data feeds:

```
steel_market_prices   →  new price every  5 seconds
steel_orders          →  new order  every 10 seconds
steel_production      →  production every 15 seconds
steel_shipments       →  shipment   every 20 seconds
```

---

## Project Structure

```
FerroFlux-steel-supply-chainV3/
├── docker-compose.yml
├── Dockerfile.spark
├── init_db.sql
├── 01_multitenant_migration.sql
├── manager.py
├── data/
│   ├── raw/                  ← Original CSV files
│   └── processed/            ← Parquet output (Bronze / Silver / Gold)
├── scripts/
│   ├── spark_jobs/
│   │   ├── etl_common.py     ← Shared helpers (save_pg_tenant, notifications)
│   │   ├── bronze_etl.py
│   │   ├── silver_etl.py
│   │   ├── gold_etl.py
│   │   └── streaming_etl.py
│   ├── ml_jobs/
│   │   ├── price_prediction.py
│   │   ├── demand_forecasting.py
│   │   └── supplier_risk.py
│   ├── kafka_producers/
│   └── dashboards/
│       └── app.py            ← Streamlit application
├── portal/
│   └── app/
│       ├── main.py
│       ├── database.py
│       ├── routers/
│       └── templates/
│           ├── dashboard.html
│           ├── login.html
│           └── portfolio.html
└── dags/                     ← Airflow DAGs
```

---

## Troubleshooting

**Streamlit not loading:**
```bash
docker compose exec -d spark-master bash -c \
  "streamlit run /opt/spark/scripts/scripts/dashboards/app.py \
   --server.port 8501 --server.address 0.0.0.0 --server.headless true"
```

**Airflow UI not responding:**  
Wait 2–3 minutes after `docker compose up`, then check logs:
```bash
docker logs airflow-webserver --tail 50
```

**PostgreSQL connection refused:**
```bash
docker logs steel-postgres --tail 20
```

**Stop all services:**
```bash
# Stop and keep all data
docker compose down

# Full reset — stops and deletes all data
docker compose down -v
```

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Compute | Apache Spark 3.4.4 |
| Streaming | Apache Kafka + Zookeeper |
| Orchestration | Apache Airflow 2.8 |
| Storage | PostgreSQL 13 + Parquet |
| Backend API | FastAPI (Python 3.11) |
| Dashboard | Streamlit |
| Infrastructure | Docker Compose (13 containers) |

---

## Author

**Nourhan Saber** — Data Engineer & Project Lead  
Graduation Project — Big Data Analytics

---

*FerroFlux — Built with Apache Spark · Apache Kafka · Apache Airflow · FastAPI · PostgreSQL · Streamlit*
