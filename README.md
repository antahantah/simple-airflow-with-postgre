# Capstone Project 3: Data Pipeline with Apache Airflow & BigQuery

**Author:** Aditya Putra Ferza  
**Program:** Purwadhika Data Engineering Bootcamp  
**Date:** October 2025  

---

## Project Overview

Automated Data pipeline using Apache Airflow to:
- **DAG 1:** Generate and insert random student score data into PostgreSQL (hourly)
- **DAG 2:** Extract yesterday's data (H-1) from PostgreSQL and load into Google BigQuery (daily) for partitioning purpose

---

## Technology Stack

- **Infrastructure:** Docker, Docker Compose
- **Orchestration:** Apache Airflow 2.7+
- **Database:** PostgreSQL 12
- **Data Warehouse:** Google BigQuery
- **Language:** Python 3.7+
- **Other Tools:** DBeaver

---

## Database Schema

### Tables

**siswa (Students)**
- `id` INTEGER PRIMARY KEY
- `nama_siswa` VARCHAR(50)
- `created_at` TIMESTAMP

**mapel (Subjects)**
- `id` INTEGER PRIMARY KEY
- `nama_mapel` VARCHAR(50)
- `created_at` TIMESTAMP

**nilai (Scores)**
- `id` INTEGER PRIMARY KEY
- `id_siswa` INTEGER FOREIGN KEY → siswa.id
- `id_mapel` INTEGER FOREIGN KEY → mapel.id
- `nilai_siswa` INTEGER (0-100)
- `created_at` TIMESTAMP

For Dummy Datasets can be accessed [here](https://docs.google.com/spreadsheets/d/1CJtZS2MyMwPUvOz8dXU3MWE2i3oBRQQN/edit?usp=sharing&ouid=109442793021875848350&rtpof=true&sd=true) 

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose installed
- Google Cloud Platform account with BigQuery enabled
- Service account key with BigQuery permissions

### Setup

1. **Clone repository**
```bash
   git clone 
   cd capstone-project-3
```

2. **Create .env file** (see `.env` examples)
```bash
   cp .env .env
   # Edit .env with your values
```

3. **Add GCP credentials**
```bash
   # Place your service account key (your own gcloud auth application-default key)
   cp /path/to/your-key.json gcp-key.json
```

4. **Start services**
```bash
   docker-compose up -d
```

5. **Access Airflow UI**
   - URL: http://localhost:8082
   - Username: airflow
   - Password: airflow

6. **Configure Connections**
   - PostgreSQL: `my_simple_postgres`
   - BigQuery: Add GCP credentials path

7. **Enable and trigger DAGs**

---
