import logging
from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

## Define variables
conn_mysql = Variable.get("mysql_default")
file = "https://github.com/marcellocaron/desafio_pagseguro/raw/main/bs140513_032310.csv"

## Define methods
def _get_mysql_connection(conn_uri):
  return create_engine(conn_uri, echo=False)

def _csv_to_dataframe(ti, file):
  logging.info(f"get data started")
  raw_df = pd.read_csv(file)
  json_raw = raw_df.to_dict()
  ti.xcom_push(key="raw_data", value=json_raw)
  logging.info(f"get data finished")

def _clean_data(ti):
  logging.info(f"clean data started")
  json_raw= ti.xcom_pull(key="raw_data", task_ids=["get_data"])
  df = pd.DataFrame.from_dict(json_raw[0], orient="columns")
  print(df.head(5))

  # remove as aspas simples dos campos que não são numéricos
  for column in df.columns:
    if df[column].dtype == object: df[column] = df[column].map(lambda x: x.replace("'",""))

  # mantem nome das colunas em snake case
  df.columns = df.columns.str.lower()

  #cria uma nova coluna com o valor id incremental
  df = df.assign(id=[0 + i for i in range(len(df))])[["id"] + df.columns.tolist()]

  # substitui os valores não utilizados
  df["age"] = df["age"].replace(["U"], None)
  df["age"] = df["age"].astype(int)
  df["gender"] = df["gender"].replace(["U", "E"], np.nan)
  df["amount"].replace(0, np.nan, inplace = True)

  # cria informações das datas
  df["year"] = "2021"
  df.loc[(df["step"] <= 30), "month"] = "01"
  df.loc[(df["step"] > 30) & (df["step"] <= 60), "month"] = "02"
  df.loc[(df["step"] > 60) & (df["step"] <= 90), "month"] = "03"
  df.loc[(df["step"] > 90) & (df["step"] <= 120), "month"] = "04"
  df.loc[(df["step"] > 120) & (df["step"] <= 150), "month"] = "05"
  df.loc[(df["step"] > 150) & (df["step"] <= 180), "month"] = "06"
  df["day"] = "01"

  json_cleaned = df.to_dict()
  ti.xcom_push(key="cleaned_data", value=json_cleaned)
  logging.info(f"clean data finished")


def _df_to_sql(ti,table_name):
  logging.info(f"upload data started")
  json_cleaned = ti.xcom_pull(key="cleaned_data", task_ids=["clean_data"])
  df = pd.DataFrame.from_dict(json_cleaned[0], orient="columns")
  print(df.head(5))

  engine = _get_mysql_connection(conn_mysql)
  logging.info(f"engine connection success")
  df.to_sql(table_name, engine, index=False)
  logging.info(f"upload data finished")


## Define the DAG object
with DAG(dag_id="ft_challenge",
         start_date=datetime(2021, 8, 1),
         description="Load and transform data using Pandas and MySQL",
         schedule_interval="@daily", catchup=False) as dag:

  start_operator = DummyOperator(task_id='begin-execution', dag=dag)

  get_data = PythonOperator(
    task_id="get_data",
    python_callable=_csv_to_dataframe,
    op_kwargs=dict(file="https://github.com/marcellocaron/desafio_pagseguro/raw/main/bs140513_032310.csv")
  )

  clean_data = PythonOperator(
      task_id="clean_data",
      python_callable=_clean_data
  )

  upload_data_mysql = PythonOperator(
      task_id="upload_data_mysql",
      python_callable=_df_to_sql,
      op_kwargs=dict(table_name="transactions")
  )

  create_tables = MySqlOperator(
      task_id="create_tables",
      mysql_conn_id = "mysql_default",
      sql = "sql_scripts/create_tables.sql"
  )

  insert_data = MySqlOperator(
      task_id="insert_data",
      mysql_conn_id = "mysql_default",
      sql = "sql_scripts/insert_data.sql"
  )

  start_operator >> get_data >> clean_data >> upload_data_mysql >> create_tables >> insert_data

