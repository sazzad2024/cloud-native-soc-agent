# Aegis: Cloud-Native AI SOC Agent 🛡️🤖

**A Federated Security Analyst that bridges Snowflake (SIEM) and Databricks (Data Lake) using AI.**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Architecture](https://img.shields.io/badge/Architecture-Cloud--Native-green)
![Status](https://img.shields.io/badge/Status-POC%20Complete-orange)

---

## 🚨 The Problem
Modern Security Operations Centers (SOCs) face "Data Gravity" issues:
1.  **SIEMs (Snowflake/Splunk)** are fast but expensive. Storing Petabytes of raw PCAP/Netflow logs here is bankrupting.
2.  **Data Lakes (Databricks)** are cheap and vast, but hard to query for real-time alerts.
3.  **Analysts** are stuck in the middle, manually copying IPs from the SIEM and searching them in the Lake.

## 💡 The Solution: Aegis
Aegis is an **Agentic AI** that automates this workflow using a **Federated Architecture**:
1.  **Listen**: Detects High-Fidelity Alerts in **Snowflake**.
2.  **Investigate**: Automatically queries **Databricks** to fetch the heavy forensic evidence (Volume, Packet Counts) for that specific specific IP.
3.  **Enrich**: Uses **RAG (Retrieval Augmented Generation)** to map alert terms to **MITRE ATT&CK** Techniques.
4.  **Triage**: Generates a JSON Triage Package containing Context + Evidence + Threat Intel.

---

## 🏗️ Project Structure

```text
socagent/
├── app/                    # 🧠 Core Agent Logic
│   ├── main_agent.py       # LangGraph Orchestrator
│   ├── connectors.py       # Snowflake/Databricks APIs
│   ├── rag_engine.py       # Semantic RAG Engine
│   └── guardrails.py       # SQL Injection Protection
├── pipelines/              # 🌊 Data Engineering
│   ├── streaming_pipeline.py# PySpark Stream
│   └── simulate_stream.py  # Traffic Generator
├── scripts/                # 🛠️ Utilities
│   ├── check_snowflake.py  # Connection Audit
│   └── list_models.py      # Gemini Model Check
├── sql/                    # 🏗️ Database Schemas
│   └── snowflake_setup.sql
└── tests/                  # 🧪 Verification
    └── verify_run2.py      # Agent Test Scenarios
```

## 🛠️ Tech Stack
*   **Orchestrator**: LangGraph (Stateful Python Agent)
*   **LLM**: Google Gemini 2.0 Flash
*   **SIEM / Identity**: Snowflake (`FACT_ALERTS`)
*   **Data Lake**: Databricks SQL (`Delta Lake`)
*   **AI / RAG**: `sentence-transformers` + `FAISS`
*   **Security**: Custom `SQLGuardrail` & LLM Output Verification.

## 🚀 How to Run

### 1. Prerequisites
*   Python 3.10+
*   Snowflake Account (Standard/Trial)
*   Databricks Account (Standard/Trial)

### 2. Installation
```bash
git clone https://github.com/sazzad2024/cloud-native-soc-agent.git
cd cloud-native-soc-agent
pip install -r requirements.txt
```

### 3. Configuration
Copy the example environment file and fill in your Cloud credentials:
```bash
cp .env.example .env
# Edit .env with your Keys
```

### 4. Setup Data (Simulation)
```bash
# 1. Setup Snowflake (Tables & Alerts)
python scripts/seed_snowflake_real.py

# 2. Setup Databricks (Gold Tables)
python scripts/seed_databricks.py
```

### 5. Run the Agent
```bash
python app/main_agent.py
```

## 📊 Sample Output

### 1. Triage Package (JSON)
```json
{
  "status": "Auto-Triaged",
  "alert": {
    "id": 1003,
    "src_ip": "18.218.229.235",
    "attack_cat": "Exploits",
    "severity": "High"
  },
  "evidence": {
    "traffic_volume": "368.3 MB",
    "packet_count": 58000
  },
  "mitre_mapping": ["Exploits (T1588.005)"]
}
```

### 2. AI Narrative Summary (Gemini 2.0 Flash)
> **EXECUTIVE SUMMARY:** Investigated a High-Severity Exploit alert originating from 18.218.229.235. The incident has been triaged as a critical threat due to abnormal traffic volume and direct mapping to MITRE ATT&CK T1588.005.
>
> **FINDINGS:**
> - **Attack Type:** High-Confidence Exploit attempt with potential Exfiltration signal.
> - **Anomalous Outbound Volume:** 368.3 MB identified via Databricks (Forensic Layer).
> - **Adversary TTPs:** Signature matches MITRE ATT&CK T1588.005 (Resource Development).
>
> **RECOMMENDATION:** Block source IP immediately and initiate credential rotation for the affected sub-system.

---
**Created by [A K M Sazzadul Alam]** - *Security Engineer (AI Specialization)*
