# 🏭 Steel Supply Chain — Data Engineering Platform

An end-to-end data engineering platform for steel supply chain management, featuring real-time Kafka Streaming, Spark ETL Pipelines, ML Models, Airflow Orchestration, and an interactive Streamlit Dashboard.

---

## ⚡ Requirements

| Tool | Version | Download |
|------|---------|----------|
| Docker | 24+ | https://docs.docker.com/get-docker/ |
| Docker Compose | 2.x | Included with Docker Desktop |
| Python | 3.8+ | https://python.org (for manager.py only) |
| Git | Any | https://git-scm.com |

> **Windows users:** Make sure WSL 2 is enabled before installing Docker Desktop.

---

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/steel-supply-chain.git
cd steel-supply-chain
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

### 3. Generate a Fernet key (required for Airflow)

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output and paste it into `.env` as the value for `AIRFLOW_FERNET_KEY`.

### 4. Edit credentials (optional for local testing)

Open `.env` and update passwords as needed. The default values work for local development.

> ⚠️ **Never commit your `.env` file to Git — it contains sensitive credentials.**

### 5. Build and start all services

```bash
docker compose up -d --build
```

Wait about 2 minutes for all services to initialize, then verify:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

All containers should show `Up`.

### 6. Launch the control panel

```bash
python3 manager.py
```

---

## 🗂️ Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Spark Master UI | http://localhost:8081 | Monitor Spark jobs |
| Airflow UI | http://localhost:8089 | Manage DAGs and pipelines |
| Streamlit Dashboard | http://localhost:8501 | Interactive data dashboard |
| PostgreSQL | localhost:5432 | Main database |
| Kafka | localhost:9092 | Message broker |

---

## 🎮 Control Panel (manager.py)

```
[1] Docker Lifecycle    — Start / Stop / Rebuild containers
[2] Status Check        — View status of all running services
[3] ETL Pipelines       — Run Bronze / Silver / Gold data layers
[4] AI/ML Models        — Run price prediction, demand forecast, supplier risk models
[5] Spark Streaming     — Start real-time data processing from Kafka
[6] Kafka Operations    — Run producers and verify topics
[7] Dashboard           — Launch Streamlit dashboard
[8] System Utilities    — Maintenance and repair tools
[9] AI Agent (Airflow)  — Trigger and monitor DAGs via REST API
```

---

## 🏗️ Project Structure

```
steel-supply-chain/
├── docker-compose.yml          # All service definitions
├── Dockerfile.spark            # Custom Spark image
├── manager.py                  # Main control panel
├── .env.example                # Environment variable template
├── .env                        # ← Never commit this file
├── scripts/
│   ├── spark_jobs/             # ETL and streaming jobs
│   │   ├── bronze_etl.py
│   │   ├── silver_etl.py
│   │   ├── gold_etl.py
│   │   └── streaming_etl.py
│   ├── ml_jobs/                # Machine learning models
│   │   ├── price_prediction.py
│   │   ├── demand_forecasting.py
│   │   └── supplier_risk.py
│   ├── kafka_producers/        # Data producers
│   │   ├── market_producer.py
│   │   ├── orders_producer.py
│   │   ├── production_producer.py
│   │   └── shipments_producer.py
│   └── dashboards/
│       └── app.py              # Streamlit application
└── dags/                       # Airflow DAGs
```

---

## 🐛 Troubleshooting

### `ByteArraySerializer` error in Spark Streaming
**Cause:** kafka-clients JAR missing from the driver classpath.  
**Fix:** Already resolved in the Dockerfile. Rebuild the image:
```bash
docker compose up -d --build spark-master
```

### Airflow UI not responding
**Fix:** Wait 2–3 minutes after `docker compose up`, then check logs:
```bash
docker logs airflow-webserver --tail 50
```

### PostgreSQL connection refused
**Fix:** Make sure `.env` values match `docker-compose.yml`:
```bash
docker logs postgres --tail 20
```

### Port already in use
**Fix:** Find and stop the process using the conflicting port:
```bash
# Example for port 5432
sudo lsof -i :5432
sudo kill -9 <PID>
```

### On Windows: `docker compose` command not found
**Fix:** Use Docker Desktop and make sure it is running before executing any commands.

---

## 🔄 Shutdown

```bash
# Stop services and keep all data
docker compose down

# Stop services and delete all data (full reset)
docker compose down -v
```

---

## 👥 Contributors

| Name | Role |
|------|------|
| Nourhan | Data Engineer & Project Lead |

---

*Built with Apache Spark 3.4.4 • Apache Kafka • Apache Airflow • PostgreSQL • Streamlit*
