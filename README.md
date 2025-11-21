# Pipeline_Airflow_Transport  
### End-to-End ETL Pipeline & Dashboard for Mexico’s Public Transportation Data

This project implements a complete **Extract → Transform → Load (ETL)** data pipeline using **Apache Airflow**, fully containerized with **Docker Compose**. The pipeline processes the national transportation dataset *etup_mensual_tr_cifra_1986_2025* (1986–2025), producing a cleaned dataset stored in PostgreSQL and Parquet format.  
A **Streamlit dashboard** is included for visualization and analysis.

---

## Why This Dataset Matters

Public transportation is essential for **economic mobility**, **sustainable development**, and **social equity**. Understanding usage patterns across Mexico’s states and transport modes helps identify where services are growing, where infrastructure is lacking, and how mobility needs evolve with time.  
The insights generated benefit **cities**, **local governments**, **environmental planners**, and **communities**, promoting data-driven decisions for better transportation systems.

---

## Project Structure

Pipeline_Airflow_Transport/
│
├── dags/
│ └── transport_etl.py
│
├── data/
│ ├── etup_mensual_tr_cifra_1986_2025.csv
│ └── tc_entidad.csv
│
├── dashboard/
│ ├── dashboard.py
│ └── Dockerfile
│
├── docker-compose.yml
├── requirements.txt
└── README.md


---

## 1. Installation & Setup

### Clone the Repository

```bash
git clone https://github.com/JoaquinMO17/Pipeline_Airflow_Transport.git
cd Pipeline_Airflow_Transport
```

## Start Airflow + Postgres + Dashboard

Ensure Docker Desktop is running, then launch the stack:

```bash
docker compose up -d
```

This starts:

- PostgreSQL

- Airflow Scheduler

- Airflow Webserver

- Airflow Init

- Streamlit Dashboard

## 2. Access the Airflow UI

Visit:

http://localhost:8080

Login credentials:

username: airflow
password: airflow

Locate the DAG:

transport_etl

Turn it on and run it manually, or let it run using its schedule.

## 3. ETL Outputs

After the DAG runs successfully, the following files are created:

Cleaned CSV
```bash
data/transport_clean.csv
```

Parquet File (optimized storage)
```bash
data/transport_clean.parquet
```

PostgreSQL Table

You can inspect the table from inside the Postgres container:
```bash
docker exec -it postgres psql -U airflow -d airflow
SELECT * FROM transport_clean LIMIT 10;
```

## 4. Dashboard
The Streamlit dashboard runs automatically with Docker.

Open:

http://localhost:8501

It includes:

- KPI: Total national transport volume

- Time-Series Chart: Growth by transport mode

- Bar Chart: Top states by usage

- Most Used Transport per State

All data comes directly from the cleaned ETL output stored in Postgres.

## Technologies Used

- Python 3.10

- Apache Airflow

- Docker Compose

- PostgreSQL

- Pandas & PyArrow

- Streamlit

- Parquet
