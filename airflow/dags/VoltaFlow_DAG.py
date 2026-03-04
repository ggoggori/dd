from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
import time

def pause_for_10_minuets():
    time.sleep(600)

script_dir = r"./workspace/scripts"

with DAG(
    "voltaflow_dag",
    default_args={
        "cwd":script_dir,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    description="data pipeline for voltaflow",
    schedule="0 0 * * *",
    start_date=datetime(2025, 8, 21),
    catchup=False,
    max_active_runs=15,
    max_active_tasks=15,
    tags=["example"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="copy_db",
        bash_command="python ./db_copy.py",
    )

    t2 = BashOperator(
        task_id="tos_data_update",
        bash_command="python ./tos_update.py",
    )

    pause_task = PythonOperator(
        task_id="pause",
        python_callable=pause_for_10_minuets
        
    )
    t3 = BashOperator(
        task_id="tos_download_and_upload_to_minio",
        bash_command="python ./tos_download_to_minio.py",
    )

    t4 = BashOperator(
        task_id="minio_to_db",
        bash_command="python ./minio_to_db.py",
    )

    t5 = BashOperator(
        task_id="refresh_pipeline_queue_tb",
        bash_command="python ./refresh_pipeline_queue_tb.py",
    )

    t6 = BashOperator(
        task_id="send_email",
        bash_command="python ./send_email.py",
    )
    # t1 >> t2 >> t3 >> t4
    t1 >> t2 >> pause_task >> t3 >> t4 >> t5



