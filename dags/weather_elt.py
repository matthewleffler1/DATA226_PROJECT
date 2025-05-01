"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator
Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/project_dbt"

conn = BaseHook.get_connection('snowflake_conn')
with DAG(
    "build_weather_elt",
    start_date=datetime(2025, 3, 19),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule='30 * * * *',
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"/home/airflow/.local/bin/dbt build --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_build