# Distributed Ecommerce Lakehouse

A production-grade data lakehouse pipeline built with Apache Airflow, dbt, Trino, Apache Iceberg, and MinIO. The pipeline ingests raw ecommerce data, transforms it through a bronze → silver → gold medallion architecture, and exposes business-ready analytics tables queryable via Trino.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│     Customer Events │ Inventory │ Payments │ Support Tickets    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Airflow                              │
│              Orchestrates the full pipeline                     │
│         (CeleryExecutor + Redis + PostgreSQL)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                          dbt Core                               │
│           Bronze → Silver → Gold transformations                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────┐    ┌──────────────────────┐
│   Trino (1 coord     │    │   Iceberg REST        │
│   + 3 workers)       │◄──►│   Catalog             │
│   Query Engine       │    │   (tabulario)         │
└──────────────────────┘    └──────────┬───────────┘
                                       │
                                       ▼
                            ┌──────────────────────┐
                            │       MinIO           │
                            │   Object Storage      │
                            │  (S3-compatible)      │
                            └──────────────────────┘
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 3.x (CeleryExecutor) |
| Transformation | dbt Core + dbt-trino | 1.10.19 / 1.9.3 |
| Query Engine | Trino | 480 |
| Table Format | Apache Iceberg | 1.10.1 |
| Catalog | tabulario/iceberg-rest | latest |
| Object Storage | MinIO | latest |
| Message Broker | Redis | 7.2 |
| Metadata DB | PostgreSQL | 16 |
| Containerization | Docker + Docker Compose | - |

---

## Project Structure

```
Distributed_Lakehouse/
├── dags/
│   ├── dag_pipeline.py              # Main Airflow DAG
│   ├── operators/
│   │   └── dbt_operator.py          # Custom dbt operator
│   └── ecommerce_dbt/               # dbt project
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── seeds/                   # Raw CSV seed data
│       │   ├── raw_customer_events.csv
│       │   ├── raw_inventory_snapshots.csv
│       │   ├── raw_payment_transactions.csv
│       │   └── raw_support_tickets.csv
│       └── models/
│           ├── bronze/              # Raw ingestion layer
│           │   ├── bronze_customer_events.sql
│           │   ├── bronze_inventory_snapshots.sql
│           │   ├── bronze_payment_transactions.sql
│           │   └── bronze_support_tickets.sql
│           ├── silver/              # Cleaned & enriched layer
│           │   ├── silver_customer_events.sql
│           │   ├── silver_inventory_snapshots.sql
│           │   ├── silver_payment_transactions.sql
│           │   └── silver_support_tickets.sql
│           └── gold/                # Business-ready aggregates
│               ├── gold_customer_360.sql
│               ├── gold_daily_revenue.sql
│               ├── gold_inventory_health.sql
│               └── gold_support_team_performance.sql
├── coordinator/
│   └── config.properties            # Trino coordinator config
├── worker/
│   └── config.properties            # Trino worker config
├── catalog/
│   └── iceberg.properties           # Iceberg catalog config
├── docker-compose.yml
├── Dockerfile                       # Custom Airflow image
└── .env                             # Environment variables
```

---

## Medallion Architecture

### Bronze Layer — Raw Ingestion
Ingests raw data from source systems with minimal transformation. Handles type casting, null handling, and adds ingestion metadata (`ingested_at`, `source_system`).

| Model | Source | Description |
|---|---|---|
| `bronze_customer_events` | `raw_customer_events` | Web/app event tracking data |
| `bronze_inventory_snapshots` | `raw_inventory_snapshots` | Daily warehouse stock snapshots |
| `bronze_payment_transactions` | `raw_payment_transactions` | Payment processing records |
| `bronze_support_tickets` | `raw_support_tickets` | Customer support interactions |

### Silver Layer — Cleaned & Enriched
Applies business logic, normalizes categoricals, masks PII, derives calculated fields, and adds data quality flags.

| Model | Key Additions |
|---|---|
| `silver_customer_events` | PII masked (IP → MD5 hex), device category, product event flag, time dimensions |
| `silver_inventory_snapshots` | Stock status, reorder flag, inventory value, net available quantity |
| `silver_payment_transactions` | Status booleans, risk tier, net amount, normalized payment method |
| `silver_support_tickets` | SLA breach flags, response/resolution minutes, satisfaction category |

### Gold Layer — Business Aggregates
One-row-per-entity aggregates optimized for BI tools and dashboards.

| Model | Grain | Business Use |
|---|---|---|
| `gold_customer_360` | 1 row per customer | CRM, churn analysis, customer health score |
| `gold_daily_revenue` | Day × currency × method × country | Finance reporting, payment ops |
| `gold_inventory_health` | Latest snapshot per product × warehouse | Supply chain, merchandising |
| `gold_support_team_performance` | Agent × day | Support management, SLA monitoring |

---

## Pipeline DAG

The Airflow DAG (`ecommerce_dag_pipeline`) runs every 6 hours and executes the following tasks in order:

```
start_pipeline
      │
      ▼
seed_bronze          ← loads CSVs into raw Iceberg tables
      │
      ▼
transform_bronze     ← dbt run --select tag:bronze
      │
      ▼
validated_bronze     ← data quality checks
      │
      ▼
transform_silver     ← dbt run --select tag:silver
      │
      ▼
validated_silver     ← business rule checks
      │
      ▼
transform_gold       ← dbt run --select tag:gold
      │
      ▼
validated_gold       ← KPI accuracy checks
      │
      ▼
generate_docs        ← dbt docs generate
      │
      ▼
end_pipeline
```

---

## Getting Started

### Prerequisites

- Docker Desktop (4GB+ RAM allocated)
- Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/your-username/Distributed_Lakehouse.git
cd Distributed_Lakehouse
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env and set FERNET_KEY and AIRFLOW_UID
echo "FERNET_KEY=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" >> .env
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Start the stack

```bash
docker compose up -d
```

### 4. Create the MinIO bucket

Open http://localhost:9001, login with `minio / minio12345`, and create a bucket named `lakehouse`.

Or via CLI:
```bash
docker exec -it minio mc alias set local http://localhost:9000 minio minio12345 --api S3v4
docker exec -it minio mc mb local/lakehouse
```

### 5. Install dbt dependencies in Airflow

```bash
docker exec -it distributed_lakehouse-airflow-worker-1 bash -c \
  "pip install 'dbt-core==1.10.19' 'dbt-trino==1.9.3' --no-cache-dir"
docker exec -it distributed_lakehouse-airflow-scheduler-1 bash -c \
  "pip install 'dbt-core==1.10.19' 'dbt-trino==1.9.3' --no-cache-dir"
```

### 6. Wait for services to be healthy

```bash
# Check all services
docker ps --format "table {{.Names}}\t{{.Status}}"

# Verify Trino is ready
curl http://localhost:8088/v1/info

# Verify Iceberg catalog
curl http://localhost:8181/v1/config
```

### 7. Trigger the pipeline

Open Airflow at http://localhost:8080 (login: `airflow / airflow`), find `ecommerce_dag_pipeline`, and trigger it manually.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Trino UI | http://localhost:8088 | any username, no password |
| MinIO Console | http://localhost:9001 | minio / minio12345 |
| Iceberg REST | http://localhost:8181/v1/config | — |

---

## Querying the Data

Connect any SQL client to Trino:

```
Host:     localhost
Port:     8088
Database: iceberg
User:     any (no password required)
```

Example queries:

```sql
-- Customer health scores
SELECT customer_id, customer_health_score, value_segment, is_churn_risk
FROM iceberg.bronze_raw_gold.gold_customer_360
ORDER BY customer_health_score DESC
LIMIT 20;

-- Daily revenue summary
SELECT transaction_date, currency, gross_revenue, net_revenue, refund_rate_pct
FROM iceberg.bronze_raw_gold.gold_daily_revenue
ORDER BY transaction_date DESC;

-- Low stock alerts
SELECT product_id, warehouse_id, quantity_on_hand, stock_status, needs_reorder
FROM iceberg.bronze_raw_gold.gold_inventory_health
WHERE needs_reorder = TRUE;

-- Agent performance
SELECT agent_id, created_date, resolution_rate_pct, csat_pct, performance_tier
FROM iceberg.bronze_raw_gold.gold_support_team_performance
ORDER BY created_date DESC;
```

---

## Connecting to Your Ecommerce Platform

Replace the static CSV seeds with live data from your ecommerce platform:

**Option 1 — Scheduled export:** Configure your ecommerce database to dump CSVs to MinIO daily/hourly. Airflow picks them up automatically on the next run.

**Option 2 — Change Data Capture:** Use Debezium to stream database changes from your operational PostgreSQL/MySQL directly into MinIO in real time.

**Option 3 — Direct API events:** Instrument your ecommerce app to write events directly to MinIO as they happen (page views, purchases, cart adds).

---

## Known Limitations

- Nessie catalog replaced with `tabulario/iceberg-rest` for simpler credential management in Docker. For production, use Nessie with a PostgreSQL backend and proper secret management.
- dbt versions must be manually installed in Airflow containers after each rebuild. Add them to the Dockerfile for persistence.
- Trino workers use `IN_MEMORY` state — data is lost on container restart without a persistent catalog backend.

---

## License

MIT
