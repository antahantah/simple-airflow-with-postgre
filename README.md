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

## Quick Start

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


## 📂 Project Structure
```
simple-airflow-with-postgre/
├── docker-compose.yaml        # Docker services configuration
├── Dockerfile.airflow         # Custom Airflow image
├── .env                       # Environment variables
├── .gitignore                 # Git ignore rules
├── docs/
│   ├── documentations/        # Project documentation (WIP)
│   └── diagrams/              # Architecture diagrams (WIP)
├── airflow/
│   ├── dags/
│   │   ├── my_postgre_dags.py    # DAG 1: Insert to PostgreSQL
│   │   └── postgres_to_bq.py     # DAG 2: ETL to BigQuery
│   ├── gcp-key.json           # Google Service Account key
│   └── logs/                  # Airflow logs (git ignored)
└── README.md                  # **This file**
```

---

### BigQuery Setup

1. Create service account in GCP
2. Grant roles:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Admin
3. Create and Download JSON key
4. Place at root: `gcp-key.json` (or copy from your local gcloud application default credentials)

## Documentations (WIP)

See `/docs/documentations/` folder for:
- Airflow UI with successful DAG runs
- DBeaver showing PostgreSQL data
- BigQuery Console showing tables and data
- Partition verification query results

---

## Video Demonstration

[https://youtu.be/s21dGEkzlgY] - 20-minute walkthrough of the Capstone project

---

## Author

**Aditya Putra Ferza**
- GitHub: [@antahantah](https://github.com/antahantah)
- Email: adit.ferza@gmail.com
This is a capstone project for educational purposes, created as assignment for Purwadhika Data Engineering Bootcamp - Capstone Project 3.

---

## Acknowledgements

- Purwadhika Digital Technology School for the comprehensive curriculum and learning resources
- The lecturers and cohort peers for their supports and guidances

Special thanks to Google Cloud Platform and everyone who contributed to the open-source libraries used in this project.

---
