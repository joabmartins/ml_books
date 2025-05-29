from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


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
    description='Dag para enviar dados do PostgreSQL para o Elasticsearch'
)

# Task 01: Obter dados
def queryPostgresql(ti):
        pg_hook = PostgresHook(postgres_conn_id='books_connection')
        conn = pg_hook.get_conn()
        try:
                df = pd.read_sql("select title, price from books_ml", conn)
                df.to_csv('postgresqldata.csv', index=False)
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