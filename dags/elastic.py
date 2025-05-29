from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from elasticsearch import Elasticsearch
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

dag = DAG(
    'elasticdag',
    default_args=default_args,
    description='Dag para enviar dados do PostgreSQL para o Elasticsearch',
)

# Task 01: Obter os dados

def queryPostgresql(ti):
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    conn = pg_hook.get_conn()
    try:
        df = pd.read_sql("select title, price from books_ml", conn)
        df.to_csv('postgresql.csv', index=False)
        print(">>Debug: Data saved")
    except Exception as e:
        print(f"Erro ao realizar query postgres: {e}")
        raise
    finally:
        if conn:
            conn.close()

getDataTask = PythonOperator(
    task_id='getData',
    python_callable=queryPostgresql,
    dag=dag,
)

# Task 02: Indexar no elasticsearch

def insertElasticSearch(ti):
    conn = BaseHook.get_connection('elasticsearch_books_conn')
    es = Elasticsearch(
        hosts=[{
            'host': conn.host,
            'port': conn.port,
            'scheme': conn.schema
        }],
        http_auth=(conn.login, conn.password) if conn.login else None,
    )
    try:
        if not es.ping():
            raise ValueError("Não foi possível conectar com o Elasticsearch")
        df = pd.read_csv('postgresql.csv')
        for i,r in df.iterrows():
            doc=r.to_json()
            res=es.index(index="frompostgresql", document=doc)
            print(res)
    except Exception as e:
        print(f"Erro durante indexação: {e}")
        raise

insertDataTask = PythonOperator(
    task_id='insertData',
    python_callable=insertElasticSearch,
    dag=dag,
)