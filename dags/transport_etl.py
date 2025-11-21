from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
from sqlalchemy import create_engine

# --------------------------
# CONFIGURATION
# --------------------------
DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/etup_mensual_tr_cifra_1986_2025.csv"
CLEAN_FILE = f"{DATA_DIR}/cleaned_transport.parquet"

POSTGRES_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"


# --------------------------
# EXTRACT
# --------------------------
def extract_dataset(**context):
    """Checks that the real dataset exists and pushes path via XCom."""
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"Dataset not found at {RAW_FILE}")

    logging.info(f"Dataset found at {RAW_FILE}")
    context["ti"].xcom_push(key="raw_path", value=RAW_FILE)


# --------------------------
# TRANSFORM
# --------------------------
def transform_dataset(**context):
    """Chunk-based cleaning and preprocessing."""
    raw_path = context["ti"].xcom_pull(key="raw_path")
    transformed_chunks = []

    try:
        for chunk in pd.read_csv(raw_path, chunksize=5000, encoding="latin-1"):

            # Remove duplicates
            chunk.drop_duplicates(inplace=True)

            # Drop empty rows
            chunk.dropna(inplace=True)

            # Type casting
            chunk["ANIO"] = chunk["ANIO"].astype(int)
            chunk["ID_MES"] = chunk["ID_MES"].astype(int)
            chunk["VALOR"] = chunk["VALOR"].astype(float)

            # Feature creation: Normalized value
            max_val = chunk["VALOR"].max()
            chunk["VALOR_NORMALIZED"] = chunk["VALOR"] / max_val if max_val != 0 else 0

            transformed_chunks.append(chunk)

        df_clean = pd.concat(transformed_chunks, ignore_index=True)

        # Save as Parquet (efficient format)
        df_clean.to_parquet(CLEAN_FILE, index=False)
        logging.info(f"Cleaned dataset saved to {CLEAN_FILE}")

        context["ti"].xcom_push(key="clean_path", value=CLEAN_FILE)

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise


# --------------------------
# LOAD
# --------------------------
def load_to_postgres(**context):
    """Loads the cleaned parquet dataset into Postgres."""
    clean_path = context["ti"].xcom_pull(key="clean_path")
    df = pd.read_parquet(clean_path)

    try:
        engine = create_engine(POSTGRES_CONN)
        df.to_sql("transport_clean", engine, if_exists="replace", index=False)

        logging.info(
            f"Loaded cleaned dataset into Postgres table 'transport_clean' ({df.shape[0]} rows)"
        )

    except Exception as e:
        logging.error(f"Failed to write to Postgres: {e}")
        raise


# --------------------------
# DAG DEFINITION
# --------------------------
default_args = {
    "owner": "joaquin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="transport_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="ETL pipeline for Mexican public transport dataset",
) as dag:

    extract = PythonOperator(
        task_id="extract_dataset",
        python_callable=extract_dataset,
    )

    transform = PythonOperator(
        task_id="transform_dataset",
        python_callable=transform_dataset,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract >> transform >> load
