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
    requests.get("https://10.99.212.69:8443/api/email-png-files-scheduled/", verify=False, timeout=3000)

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

def trigger_scheduled_emails(**context):
    resp = requests.get("https://10.99.212.69:8443/api/email-png-files-scheduled/", verify=False, timeout=3000)
    resp.raise_for_status()

    result = resp.json()
    summary = result.get("summary", {})
    print(f"발송: {summary.get('sent', 0)}건")
    print(f"스킵: {summary.get('skip', 0)}건")
    print(f"오류: {summary.get('errors', 0)}건")

    if summary.get("errors", 0) > 0:
        details = result.get("details", [])
        errors = [d for d in details if d["status"] == "error"]
        print(f"오류 발생: {errors}")
        raise Exception(f"발송 오류 {summary['erros']} 건")

with DAG(
    "mailing_custom",
    default_args = {
        # "cwd":script_dir,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    description="15분 간격 메일링 트리거(사용자 별 설정 시각/주기 반영)",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 2, 27),
    catchup=False,
    max_active_runs=15,
    max_active_tasks=15,
    tags=["example"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="send_email",
        python_callable=trigger_scheduled_emails
        
    )

    # t1 >> t2 >> t3 >> t4
    # t1

t1



