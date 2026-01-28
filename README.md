# RedWatch — Streaming Warehouse Observability (Capstone)

RedWatch is a **streaming analytics system** that ingests query-level warehouse events via Kafka, cleans and enriches them in Postgres using dbt, and produces **accurate, time-aware KPIs** for cost, concurrency, and workload behavior.

This project is fully **Dockerized**—no local installation of Python, dbt, Kafka, or Postgres is required.

---

## 🏗 Architecture Overview


1. **Ingestion**: Kafka → Postgres (`public.redset_events`)
2. **Transformation**: dbt (Incremental models → `analytics.clean_table`)
3. **Analytics**: dbt KPI Models (Minute / 15m aggregates)
4. **Visualization**: Streamlit (Optional)

### Core Principles
* **Single Source of Truth**: All KPIs derive from the enriched `clean_table`.
* **Time-Consistent**: KPIs lag ingestion to prevent partial-minute data errors.
* **Efficiency**: Incremental dbt models ensure no redundant recomputation.

---

## 🚀 Getting Started

### Prerequisites
* **Docker** & **Docker Compose**
* *Note: Do NOT install Python, dbt, or Kafka manually.*

### Configuration
Review the `.env` file before launching. 
* **Change**: `POSTGRES_PASSWORD` for security.
* **Do Not Change**: `POSTGRES_HOST`, `DBT_SCHEMA`, or `PG_DSN` (internal Docker networking).

### Operation Commands
| Action | Command |
| :--- | :--- |
| **Start System** | `docker compose up --build -d` |
| **Check Status** | `docker compose ps` |
| **Stop (Keep Data)** | `docker compose down` |
| **Full Reset (Delete Data)** | `docker compose down -v` |

---

## 📊 Data Flow & KPIs

### The Clean Table
An incremental, enriched table where each row represents one query, including:
* **Durations** and **Workload Class**
* **Heavy Units** and **Cluster Size**

### Implemented KPIs
| KPI Model | Description |
| :--- | :--- |
| `kpi_minute_cluster_workload` | Cost & workload intensity per minute. |
| `kpi_minute_concurrency` | Active/Started/Ended query counts. |
| `kpi_leaderboard_15m` | Top users/fingerprints by cost. |
| `kpi_resource_predator_15m` | Cost share % per user. |
| `kpi_offpeak_flags_minute` | Recommendations for workload shifting. |

---

## 🔍 Validation & Debugging
Run these inside your Postgres instance to verify health:

```sql
-- Check ingestion volume
SELECT COUNT(*) FROM public.redset_events;

-- Check KPI freshness
SELECT MAX(minute_ts) FROM analytics.kpi_minute_cluster_workload;

-- Verify 'Heavy Unit' consistency
SELECT minute_ts, instance_id, SUM(heavy_unit) 
FROM analytics.clean_table 
GROUP BY 1, 2;