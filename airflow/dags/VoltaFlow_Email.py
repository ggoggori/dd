from datetime import datetime, timedelta

# Operators; we need this to operate!

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
import time
import requests
import subprocess

# script_dir = r"./workspace/scripts"

def call_api():
    requests.get("https://10.99.212.69:8443/api/email-png-files-sh/", verify=False, timeout=3000)

# def call_api():

# def run_send_email_batch():
#     cmd = [
#         "docker", "exec", "django_web",
#         "bash", "-lc", "bash /app/scripts/send_email_batch.sh"
#     ]

#     p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace', bufsize=1)

#     assert p.stdout is not None

#     for line in p.stdout:
#         print(line, end="")
    
#     rc=p.wait()
#     p.stdout.close()

#     if rc!= 0:
#         raise RuntimeError(f"batch faield: rc={rc}")


with DAG(
    "mainling",
    default_args={
        # "cwd":script_dir,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    description="data pipeline for voltaflow",
    schedule="0 2 * * 5", # every fri
    start_date=datetime(2025, 8, 21),
    catchup=False,
    max_active_runs=15,
    max_active_tasks=15,
    tags=["example"],
) as dag:
 
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="send_email",
        python_callable=call_api
        
    )

    # t1 >> t2 >> t3 >> t4
    # t1

t1



