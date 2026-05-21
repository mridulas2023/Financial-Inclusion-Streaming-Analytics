# 🏦 Real-Time Streaming Analytics for Financial Inclusion & Economic Mobility

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.1.3-000000?style=for-the-badge&logo=flask&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Simulated-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google_Colab-Ready-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**A full-stack Big Data pipeline applying Apache Spark, Kafka Streaming, MLlib, and a REST API to analyse financial inclusion and detect fraud across 6.3 million mobile money transactions.**

[▶ Run on Colab](#-quick-start-on-google-colab) · [📡 API Reference](#-api-reference) · [📊 Results](#-results) · [📄 Report](#-project-report)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Dataset](#-dataset)
- [Project Structure](#-project-structure)
- [Quick Start on Google Colab](#-quick-start-on-google-colab)
- [API Reference](#-api-reference)
- [Features & Concepts](#-features--big-data-concepts-covered)
- [Results](#-results)
- [Tech Stack](#-tech-stack)
- [Contributors](#-contributors)
- [References](#-references)

---

## 🔍 Overview

This project applies core **Big Data Computing** concepts to the real-world problem of **financial inclusion** — ensuring underserved populations have access to financial services. Using the **PaySim mobile money transaction dataset** (6.36 million rows), the system:

- Processes transactions at scale with **PySpark distributed computing**
- Streams events in real time using a **Kafka simulation + Spark Structured Streaming**
- Detects fraud using a **PySpark MLlib Random Forest pipeline** (AUC > 0.95)
- Tracks **economic mobility** and **financial inclusion** through balance-tier segmentation
- Exposes all analytics via a **Flask REST API** with 14 endpoints, publicly accessible via ngrok

> **Course:** Big Data Computing in Business Analytics  
> **Platform:** Google Colab · Python 3.10 · Java 11

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LAMBDA ARCHITECTURE                          │
├───────────────────┬─────────────────────┬───────────────────────────┤
│   BATCH LAYER     │    SPEED LAYER       │     SERVING LAYER         │
│                   │                     │                           │
│  financial.csv    │  Kafka Simulation   │   Flask REST API          │
│       │           │  (Python Thread)    │   (14 endpoints)          │
│       ▼           │       │             │         │                 │
│  PySpark          │       ▼             │         ▼                 │
│  DataFrame        │  Structured         │   /api/analyze/*          │
│       │           │  Streaming          │   /api/predict/fraud      │
│       ▼           │       │             │   /api/ml/train           │
│  Feature Eng.     │  Windowed Aggs      │   /api/stream/*           │
│  Spark SQL        │  Fraud Alerts       │                           │
│  MLlib Pipeline   │  Watermarking       │   Public via ngrok        │
└───────────────────┴─────────────────────┴───────────────────────────┘
```

---

## 📂 Dataset

**PaySim** — Synthetic Mobile Money Transactions  
Source: [Kaggle — PaySim1](https://www.kaggle.com/datasets/ealaxi/paysim1)

| Property | Value |
|---|---|
| Total Rows | 6,362,620 |
| Columns | 11 |
| Time Span | 30 days (744 hours) |
| Transaction Types | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| Fraud Cases | 8,213 (0.13%) |
| Fraud Types | TRANSFER and CASH_OUT only |

### Schema

| Column | Type | Description |
|---|---|---|
| `step` | int | Hour of simulation (1–744) |
| `type` | string | Transaction type |
| `amount` | double | Transaction amount |
| `nameOrig` | string | Origin customer ID |
| `oldbalanceOrg` | double | Balance before transaction |
| `newbalanceOrig` | double | Balance after transaction |
| `nameDest` | string | Destination customer/merchant |
| `oldbalanceDest` | double | Destination balance before |
| `newbalanceDest` | double | Destination balance after |
| `isFraud` | int | **Target label** (1 = fraud) |
| `isFlaggedFraud` | int | Business rule flag |

> ⚠️ Download `financial.csv` from Kaggle and upload it to Colab before running.

---

## 📁 Project Structure

```
financial-inclusion-analytics/
│
├── 📓 Financial_Inclusion_Streaming_Analytics.ipynb   # Main analytics notebook
│      ├── Section 1 — Environment Setup (PySpark + Java)
│      ├── Section 2 — Data Ingestion & EDA
│      ├── Section 3 — Batch Analytics (Spark SQL + Feature Engineering)
│      ├── Section 4 — Kafka Stream Simulation
│      ├── Section 5 — Structured Streaming (Windowed Aggs + Fraud Alerts)
│      ├── Section 6 — MLlib Fraud Detection Pipeline
│      ├── Section 7 — Financial Inclusion Insights
│      └── Section 8 — Results & Conclusions
│
├── 🌐 financial_api.py                                # Flask REST API (14 endpoints)
│      ├── /api/load              — Load dataset into Spark
│      ├── /api/analyze/*         — Analytics endpoints
│      ├── /api/predict/fraud     — Real-time fraud scoring
│      ├── /api/ml/train          — Train RandomForest model
│      └── /api/stream/*          — Kafka simulation controls
│
├── 📓 Financial_Inclusion_API_Colab.ipynb             # API launcher + test notebook
│      ├── Step 1 — Install dependencies
│      ├── Step 2 — Upload files
│      ├── Step 3 — Start Flask + ngrok public URL
│      ├── Step 4 — Test all 14 endpoints
│      └── Step 5 — Dashboard visualisation
│
├── 📄 Financial_Inclusion_Report_Final.docx           # Full academic report
└── 📄 README.md                                       # This file
```

---

## 🚀 Quick Start on Google Colab

### Step 1 — Run the Main Analytics Notebook

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/)

1. Open `Financial_Inclusion_Streaming_Analytics.ipynb` in Google Colab
2. Upload `financial.csv` when prompted
3. Run all cells in order (Runtime → Run all)

```python
# Cell 1 — installs everything needed
!apt-get install -y openjdk-11-jdk-headless -qq > /dev/null
!pip install pyspark==3.5.0 kafka-python findspark --quiet
```

> 💡 **Tip:** Set runtime to **High-RAM** (Runtime → Change runtime type) for best performance with 6.3M rows.

---

### Step 2 — Launch the REST API

1. Open `Financial_Inclusion_API_Colab.ipynb` in a **new** Colab tab
2. Upload `financial_api.py` and `financial.csv`
3. Get a free ngrok token at [dashboard.ngrok.com](https://dashboard.ngrok.com/get-started/your-authtoken)
4. Run the cells in order:

```python
# Install
!pip install pyspark==3.5.0 flask pyngrok findspark --quiet

# Start Flask
import subprocess, time
server = subprocess.Popen(['python', 'financial_api.py'])
time.sleep(4)

# Create public URL
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_TOKEN_HERE")
tunnel = ngrok.connect(5000)
print(f"🌐 API live at: {tunnel.public_url}")
```

5. Load your dataset:

```python
import requests
requests.post("http://localhost:5000/api/load", json={"path": "financial.csv"})
```

6. Hit any endpoint:

```python
# Fraud analysis
print(requests.get("http://localhost:5000/api/analyze/fraud").json())

# Predict a transaction
print(requests.post("http://localhost:5000/api/predict/fraud", json={
    "type": "TRANSFER",
    "amount": 750000,
    "oldbalanceOrg": 750000,
    "newbalanceOrig": 0,
    "oldbalanceDest": 0,
    "newbalanceDest": 750000
}).json())
```

---

## 📡 API Reference

Base URL: `http://localhost:5000` (or your ngrok public URL)

All responses follow a standard envelope:

```json
{
  "status": "success",
  "message": "OK",
  "timestamp": "2026-01-01T00:00:00Z",
  "data": { ... },
  "execution_time_ms": 120
}
```

### Endpoints

| Method | Endpoint | Description | Body Required |
|---|---|---|---|
| `GET` | `/` | API index & all endpoints | — |
| `GET` | `/health` | Health check (Spark + dataset status) | — |
| `POST` | `/api/load` | Load CSV into Spark DataFrame | `{"path": "financial.csv"}` |
| `GET` | `/api/analyze/overview` | Total rows, fraud rate, amount stats | — |
| `GET` | `/api/analyze/fraud` | Fraud count & rate by transaction type | — |
| `GET` | `/api/analyze/inclusion` | Segmentation by income tier | — |
| `GET` | `/api/analyze/mobility` | Balance-tier transitions (economic mobility) | — |
| `GET` | `/api/analyze/hourly` | Hourly transaction volume & fraud counts | — |
| `POST` | `/api/predict/fraud` | Real-time rule-based risk score (0–100) | Transaction JSON |
| `POST` | `/api/ml/train` | Train RandomForest MLlib pipeline | Training config JSON |
| `GET` | `/api/model/info` | Model metadata & feature list | — |
| `POST` | `/api/stream/simulate` | Start Kafka streaming simulation | Stream config JSON |
| `GET` | `/api/stream/status` | Query streaming job status | `?job_id=...` |
| `POST` | `/api/stream/stop` | Stop a streaming job | `{"job_id": "..."}` |
| `POST` | `/api/batch/full-pipeline` | Run complete analytics pipeline | — |

### Fraud Prediction Example

**Request:**
```json
POST /api/predict/fraud
{
  "type": "TRANSFER",
  "amount": 500000,
  "oldbalanceOrg": 500000,
  "newbalanceOrig": 0,
  "oldbalanceDest": 0,
  "newbalanceDest": 500000
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "risk_score": 80,
    "risk_level": "HIGH",
    "recommendation": "Block and review",
    "risk_reasons": [
      "High-risk transaction type (+20)",
      "Account fully drained (+35)",
      "Very large transaction >500K (+25)"
    ]
  }
}
```

### ML Training Example

**Request:**
```json
POST /api/ml/train
{
  "num_trees": 100,
  "max_depth": 10,
  "sample_fraction": 0.05
}
```

**Response:**
```json
{
  "data": {
    "model_type": "RandomForestClassifier",
    "metrics": {
      "auc_roc": 0.9612,
      "f1_score": 0.9401,
      "accuracy": 0.9487
    },
    "feature_importance": [
      { "feature": "account_emptied", "importance": 0.38 },
      { "feature": "amount",          "importance": 0.22 }
    ]
  }
}
```

---

## ⚙️ Features & Big Data Concepts Covered

### The Five Vs

| Dimension | How Addressed |
|---|---|
| **Volume** | 6.36M rows processed with PySpark distributed DataFrames |
| **Velocity** | Kafka simulation + Structured Streaming with 5-second micro-batch triggers |
| **Variety** | Mixed numeric, categorical, and temporal data with schema enforcement |
| **Veracity** | Explicit schema validation, null handling, feature engineering, fraud labelling |
| **Value** | Inclusion segmentation, mobility tracking, fraud alerting, REST API serving |

### PySpark Concepts Applied

- **Lazy Evaluation** — transformations build a DAG, executed only on actions
- **Caching** — `.cache()` persists the primary DataFrame in memory
- **Adaptive Query Execution (AQE)** — automatic shuffle partition optimisation
- **Spark SQL** — ad-hoc analytical queries using `createOrReplaceTempView`
- **Structured Streaming** — unbounded table model with `readStream` / `writeStream`
- **Watermarking** — `withWatermark("event_time", "1 hour")` for late data tolerance
- **Sliding Windows** — 6-hour windows with 2-hour slide for rolling aggregations
- **MLlib Pipeline** — `StringIndexer → VectorAssembler → StandardScaler → RandomForestClassifier`

### Feature Engineering

| Feature | Formula | Purpose |
|---|---|---|
| `account_emptied` | `oldBal > 0 AND newBal == 0` | Strongest fraud signal |
| `amount_to_balance_ratio` | `amount / oldbalanceOrg` | Proportional drain detection |
| `balance_diff_orig` | `newbalanceOrig - oldbalanceOrg` | Net outflow measurement |
| `hour_of_day` | `(step - 1) % 24` | Off-hours activity detection |
| `is_large_transaction` | `amount > 200,000` | High-value flag |

---

## 📊 Results

### Fraud by Transaction Type

| Type | Transactions | Fraud Count | Fraud Rate |
|---|---|---|---|
| TRANSFER | 532,909 | 4,097 | **0.769%** |
| CASH_OUT | 2,237,500 | 4,116 | **0.184%** |
| PAYMENT | 2,151,495 | 0 | 0.000% |
| CASH_IN | 1,399,284 | 0 | 0.000% |
| DEBIT | 41,432 | 0 | 0.000% |

### Financial Inclusion Segments

| Segment | Share of Transactions | Fraud Rate |
|---|---|---|
| Unbanked (zero balance) | **44%** | ~0.00% |
| Low Income (< 10K) | 14% | 0.09% |
| Middle Income (10K–100K) | 22% | 0.15% |
| High Income (> 100K) | 19% | 0.25% |

> 44% of all transactions come from zero-balance (unbanked) accounts — confirming mobile money actively serves the financially excluded.

### ML Model Performance

| Metric | Score |
|---|---|
| AUC-ROC | **> 0.95** |
| F1-Score | > 0.93 |
| Accuracy | > 0.94 |
| Recall | > 0.90 |

**Top Features:** `account_emptied` (38%) · `amount` (22%) · `balance_diff_orig` (15%)

---

## 🛠 Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.10+ |
| Distributed Processing | Apache Spark / PySpark | 3.5.0 |
| Stream Processing | Spark Structured Streaming | 3.5.0 |
| Message Broker (simulated) | Apache Kafka pattern | — |
| Machine Learning | PySpark MLlib | 3.5.0 |
| API Framework | Flask | 3.1.3 |
| Public Tunnel | pyngrok | 7.x |
| JVM Runtime | Java (OpenJDK) | 11 |
| Platform | Google Colab | — |
| Data Source | PaySim (Kaggle) | — |

---

## 💻 Local Installation (Optional)

If you want to run outside Colab:

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/financial-inclusion-analytics.git
cd financial-inclusion-analytics

# Install Java 11
sudo apt-get install -y openjdk-11-jdk-headless

# Install Python dependencies
pip install pyspark==3.5.0 flask pyngrok findspark pandas matplotlib

# Set Java home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download dataset from Kaggle and place it as financial.csv

# Start the API
python financial_api.py
```

---

## 👥 Contributors

| Name | Register Number | Role |
|---|---|---|
| [Team Member 1] | [Reg No.] | Batch Processing & Feature Engineering |
| [Team Member 2] | [Reg No.] | Streaming Pipeline & Kafka Simulation |
| [Team Member 3] | [Reg No.] | MLlib Pipeline & Model Evaluation |
| [Team Member 4] | [Reg No.] | REST API Development & Dashboard |

> **Faculty Guide:** [Faculty Name], [Designation]  
> **Institution:** [College/University Name]  
> **Academic Year:** 2025–2026

---

## 📄 Project Report

The full academic report (`Financial_Inclusion_Report_Final.docx`) covers:

1. Title & Team Members
2. Abstract
3. Introduction
4. Literature Review
5. Research Gap, Objectives, Innovation and Novelty
6. Methodology
7. Results
8. Conclusion and Discussion
9. References

---

## 📚 References

1. Zaharia et al. (2016). *Apache Spark: A Unified Engine for Big Data Processing.* CACM.
2. Armbrust et al. (2018). *Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark.* SIGMOD.
3. Lopez-Rojas et al. (2016). *PaySim: A Financial Mobile Money Simulator for Fraud Detection.* EMSS.
4. Jack & Suri (2011). *Mobile Money: The Economics of M-PESA.* NBER Working Paper.
5. Demirguc-Kunt et al. (2018). *The Global Findex Database 2017.* World Bank.
6. Marz & Warren (2015). *Big Data: Principles and Best Practices.* Manning Publications.

---

## 📜 License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for details.

---

<div align="center">

Made with ❤️ for Big Data Computing in Business Analytics

⭐ Star this repo if it helped you!

</div>
