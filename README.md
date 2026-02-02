# 🔴 RedWatch — Real-Time Cost & Query Observability for Amazon Redshift

> **“Most teams see the cloud bill *after* the damage is done.  
RedWatch shows who burned the money, why it spiked, and how to fix it — in real time.”**

---

## 🚀 Project Vision

Modern cloud data warehouses like **Amazon Redshift** run **hundreds of queries per minute** — across ETL jobs, dashboards, analysts, and ad-hoc workloads.  

Yet most teams operate **blind**:
- ❌ No real-time visibility into *who* is expensive  
- ❌ No clarity on *why* costs spike  
- ❌ No way to detect inefficient workloads before the bill arrives  

**RedWatch** is built to solve exactly this problem.

It is a **Database Observability & Cost Intelligence Platform** that:
- Replays historical Redshift query workloads as a **live data stream**
- Calculates **shadow cost ($/min)** in real time
- Identifies **resource predators**, waste, and contention
- Recommends **actionable optimizations**

This project was built as a **Data Engineering Capstone**, but designed with **production-grade architecture principles**.

---

## 🧠 What Makes RedWatch Different?

RedWatch does **not** analyze business data (sales, users, revenue).

Instead, it analyzes **query behavior itself**.

Think of it as:
> 🕹️ **Mission Control for your Data Warehouse**

### 🔥 Core Innovation: Shadow Costing
We translate query execution behavior into:
- **Current Spend Rate ($/min)**
- **Daily Cost Trends**
- **Cost Spikes during Peak Load**

This allows teams to *see the bill forming in real time*, not weeks later.

---

## 📊 Key Features

### 1️⃣ Shadow Cost Panel
- Real-time **$ / minute burn rate**
- Cost spikes correlated with workload patterns
- Peak vs off-peak cost behavior

---

### 2️⃣ Leaderboard Rankings
Ranked views of:
- 💸 Most expensive queries
- 👤 Most expensive users
- 🧠 Most expensive query types (SELECT, ANALYZE, ETL, etc.)

Each ranking clearly explains **what metric the rank is based on**.

---

### 3️⃣ Query Efficiency Panel
Identify waste using:
- Execution time vs scanned bytes
- Waste percentage
- CPU pressure
- Queue wait time
- Concurrent query pressure
- Scan-heavy vs output-light queries

---

### 4️⃣ Cluster Heat Visualizer
- Each cluster visualized as a **polygon heatmap**
- Color-coded load intensity
- Instantly spot under-utilized vs overloaded clusters

---

### 5️⃣ Resource Predator Tracker
Groups queries by:
- Query fingerprint
- Workload type
- Resource usage
- Cost impact

Shows which **patterns**, not just users, are burning money.
---

## 🏗️ System Architecture

RedWatch is built as a **fully dockerized streaming data pipeline**.

<img width="1024" height="683" alt="image" src="https://github.com/user-attachments/assets/6b5a059b-2a18-4ffd-aca6-f37a0156c728" />



### Why this architecture?
- **Kafka** decouples producers and consumers
- **Time-Warp Replay** simulates real-time behavior from static data
- **Postgres** handles transformations & feature engineering
- **Streamlit** enables rapid, live visualization

This mirrors **real production streaming architectures** used in industry.

---

## 🧪 Dataset

- **Source:** Redset (Amazon Redshift query traces)
- **Type:** Metadata / Observability data
- **Contains:**
  - Query timings
  - Resource usage
  - Scan statistics
  - Concurrency behavior
  - Query fingerprints

No business or sensitive data is used.

---

## 🛠️ Tech Stack

| Layer | Technology |
|-----|-----------|
| Data Replay | Python |
| Streaming | Apache Kafka |
| Storage & Processing | PostgreSQL |
| Orchestration | Docker & Docker Compose |
| Dashboard | Streamlit |
| Data Format | Parquet / CSV |

---

## 🎯 Why This Project Matters

This project demonstrates:
- ✅ Real-time data streaming
- ✅ Event-driven architecture
- ✅ Cost modeling from system metrics
- ✅ Observability-first thinking
- ✅ End-to-end data engineering ownership

It is designed to reflect how **modern data platform teams actually work**.

---

## 🧭 Final Thought

> **Data warehouses don’t fail because of scale —  
they fail because teams can’t see what’s happening inside.**

**RedWatch turns the warehouse from a black box into a control panel.**

---


