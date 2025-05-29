import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# PARAMETROS
default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2025, 5, 20), # data de hoje
     'retries': 1,
     'retry_delay': timedelta(minutes=5),
     'schedule_interval': '@daily'
}

# DAGS
# -> 03 (Observa no servidor)
dag = DAG(
     'elasticdag',
     default_args=default_args,
     description='Dag para mandar os dados do postgres para o elasticsearch',
)

# TASKS:
     # FUNÇÕES PYTHON
def queryPostgresql(ti):
     print("iniciando task 01")
     pg_hook = PostgresHook(postgres_conn_id='books_connection') # <-- Use o ID da sua conexão
     conn = pg_hook.get_conn() # <-- Obtém o objeto de conexão psycopg2
     try:
          df = pd.read_sql("select title, price from books_ml",conn)
          df.to_csv('postgresqldata.csv', index=False)
          print("-------Data Saved------")
     except Exception as e:
          print(f"Error during PostgreSQL query: {e}")
          raise # Re-levanta o erro para que o Airflow marque a tarefa como falha
     finally:
          if conn:
               conn.close() # <-- É importante fechar a conexão

def insertElasticsearch(ti):
     print("iniciando task 02")
     conn = BaseHook.get_connection('elasticsearch_books_conn')
     es = Elasticsearch(
        hosts=[{
            'host': conn.host,
            'port': conn.port,
            'scheme': conn.schema # Isso pegará 'http' ou 'https' da sua conexão do Airflow
        }],
        http_auth=(conn.login, conn.password) if conn.login else None,
     )
     try:
          if not es.ping():
               raise ValueError("Não foi possível conectar ao Elasticsearch!")
          df=pd.read_csv('postgresqldata.csv')
          for i,r in df.iterrows():
               doc=r.to_json()
               res=es.index(index="frompostgresql", document=doc)
               print(res)
     except Exception as e:
          print(f"Error during Elasticsearch indexing: {e}")
          raise # Re-levanta o erro para que o Airflow marque a tarefa como falha

     # OPERATORS
getDataTask = PythonOperator(
    task_id='getData',
    python_callable=queryPostgresql,
    dag=dag,
)

insertDataTask = PythonOperator(
    task_id='insertData',
    python_callable=insertElasticsearch,
    dag=dag,
)

# DEPENDÊNCIAS
getDataTask >> insertDataTask