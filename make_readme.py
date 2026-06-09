readme = r"""# EZZ STEEL - Smart Supply Chain Analytics Platform

**Big Data Analytics Platform for Egyptian Steel Supply Chain | Graduation Project 2026**

---

## Overview

A comprehensive Big Data Analytics Platform designed for the Egyptian steel supply chain industry.
The platform processes 86,044 records across 2 years of data, providing real-time insights through
streaming analytics, machine learning predictions, and interactive dashboards.

| Metric | Value |
|--------|-------|
| Total Records | 86,044 |
| Data Period | 2 Years (2023-2024) |
| Docker Containers | 12 |
| Kafka Topics | 4 (Real-time) |
| Database Tables | 23 |
| ML Models | 3 |
| Dashboard Pages | 5 |

---

## Architecture
BATCH: CSV Files --> Spark ETL (Bronze/Silver/Gold) --> PostgreSQL --> ML Models
STREAM: Kafka Producers --> Kafka Topics --> Spark Streaming --> PostgreSQL --> Dashboard

text

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Containerization | Docker Compose | 12 microservices orchestration |
| Message Broker | Apache Kafka 7.4 | Real-time data streaming (4 topics) |
| Processing | Apache Spark 3.4 | Batch ETL + Stream processing |
| Storage | PostgreSQL 13 | 6 schemas, 23 tables |
| Orchestration | Apache Airflow 2.8 | Daily pipeline automation |
| ML/AI | PySpark MLlib | 3 predictive models |
| Dashboard | Streamlit + Plotly | 5-page interactive analytics |
| Language | Python 3.11 | Primary development language |

---

## Features

### Data Pipeline (Batch)
- Bronze Layer: Raw CSV ingestion to Parquet + PostgreSQL
- Silver Layer: Data cleaning, transformations, joins, feature engineering
- Gold Layer: Aggregated KPIs, scorecards, ML feature tables

### Real-time Streaming
- 4 Kafka Topics: Market prices, Orders, Production, Shipments
- Spark Structured Streaming: Live aggregation and alerting
- Price Alerts: Automatic detection of critical price changes

### Machine Learning Models

| Model | Algorithm | Key Metric |
|-------|----------|------------|
| Steel Price Prediction | RandomForest Regressor | MAPE: 5.34% |
| Demand Forecasting | GBT Regressor | R2: 0.74 |
| Supplier Risk Scoring | RandomForest Classifier | Accuracy: 94.78% |

### Interactive Dashboard
1. Executive Command Center - KPIs, trends, AI predictions
2. Market Intelligence - Price analytics, correlations, volatility
3. Production Analytics - Efficiency, energy consumption, facility comparison
4. Orders and Demand - Regional analysis, AI forecasting, customer segmentation
5. Logistics and Procurement - Carrier performance, supplier risk, carbon tracking

---

## Dashboard Screenshots

### Executive Dashboard
![Executive Dashboard](docs/screenshots/executive.png)

### Market Intelligence
![Market Intelligence](docs/screenshots/market.png)

### Production Analytics
![Production Analytics](docs/screenshots/production.png)

### Orders and Demand
![Orders and Demand](docs/screenshots/orders.png)

### Logistics and Procurement
![Logistics](docs/screenshots/logistics.png)

---

## Setup and Installation

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### Quick Start

1. Clone the repository
git clone https://github.com/YOUR_USERNAME/steel-supply-chain.git
cd steel-supply-chain

text

2. Start Docker containers
docker-compose up -d

text

3. Run Batch ETL
docker exec spark-master /opt/spark/bin/spark-submit --master local[] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/spark_jobs/bronze_etl.py
docker exec spark-master /opt/spark/bin/spark-submit --master local[] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/spark_jobs/silver_etl.py
docker exec spark-master /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/spark_jobs/gold_etl.py

text

4. Run ML Models
docker exec spark-master /opt/spark/bin/spark-submit --master local[] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/ml_jobs/price_prediction.py
docker exec spark-master /opt/spark/bin/spark-submit --master local[] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/ml_jobs/demand_forecasting.py
docker exec spark-master /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g --jars /opt/spark/jars/postgresql-42.7.1.jar /opt/spark/data/ml_jobs/supplier_risk.py

text

5. Install dashboard dependencies
pip install streamlit plotly pandas psycopg2-binary sqlalchemy

text

6. Launch Dashboard
streamlit run dashboards/app.py --server.port 8501

text

### Access Points

| Service | URL |
|---------|-----|
| Dashboard | http://localhost:8501 |
| Spark Master UI | http://localhost:9090 |
| Kafka UI | http://localhost:8083 |
| Airflow UI | http://localhost:8082 (admin/admin123) |

---

## Project Structure
steel-supply-chain/
|-- docker-compose.yml # 12 container orchestration
|-- Dockerfile # Custom Airflow image
|-- init_db.sql # Database schema (6 schemas, 23 tables)
|-- requirements.txt # Python dependencies
|-- README.md # Project documentation
|-- .gitignore # Git ignore rules
|
|-- data/raw/ # Source CSV files (86,044 rows)
|-- kafka_producers/ # 4 Kafka producers + config
|-- spark_jobs/ # Batch + Streaming ETL
|-- ml_jobs/ # 3 ML models
|-- dags/ # Airflow DAG
|-- dashboards/ # Streamlit Dashboard
|-- docs/ # Documentation + Screenshots

text

---

## Database Schema

| Schema | Tables | Description |
|--------|--------|-------------|
| raw_data | 5 tables (86,044 rows) | Bronze layer - raw ingested data |
| processed_data | 5 tables (86,044 rows) | Silver layer - cleaned and enriched |
| analytics | 6 tables | Gold layer - business KPIs |
| streaming | 4 tables | Real-time streaming data |
| ml_models | 3 tables | ML predictions and scores |

---

## Key Business Insights

- 530 price spikes detected (>2% daily change) in 730 days
- Morning shift consistently outperforms across all 13 production lines
- Greater Cairo dominates demand with 8,842 orders
- Rail transport: 83% less CO2 than trucks (1.7 vs 10.3 kg/ton)
- Gross margin: 88-93% monthly
- Raw material on-time delivery: only 41.2%
- BHP = safest supplier (risk score: 32.57)
- Steel price and USD/EGP: strong negative correlation (-0.8)

---

## Team Members

| Name | Role | ID |
|------|------|----|
| Nourhan Shalaby | Team Lead and Data Engineer | - |
| | | |
| | | |
| | | |

---

## License

This project is developed as a graduation project for academic purposes.

---

Built with love for the Egyptian Steel Industry
Powered by Apache Spark - Apache Kafka - PostgreSQL - PySpark MLlib - Streamlit
"""

with open('README.md', 'w', encoding='utf-8') as f:
    f.write(readme)
print("README.md created!")
