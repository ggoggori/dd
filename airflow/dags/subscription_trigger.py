from datetime import datetime, timedelta
# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
from email.header import Header
from email.utils import formataddr
from django.core.mail import send_mail
from django.conf import settings

if not settings.configured:
    settings.configure(
        # 필수 설정: 어떤 이메일 백엔드를 사용할 것인지 지정
        # 이메일 전송 테스트 중이라면 'django.core.mail.backends.console.EmailBackend' 사용
        EMAIL_BACKEND='django.core.mail.backends.smtp.EmailBackend', 
        
        # SMTP 서버 설정 (예: GMail, AWS SES 등)
        EMAIL_HOST="spam.lgensol.com",             # 환경 변수에서 가져옵니다.
        EMAIL_PORT=25,  # 포트 번호 (일반적으로 587 또는 465)
        EMAIL_USE_TLS=False,                            # TLS 사용 여부 (일반적으로 True)
    )
    
def send_email(user_email, subject, message):
    display_name = Header('수명모델링팀', 'utf-8').encode()
    from_email = formataddr((display_name, 'blmt2024@lgensol.com'))
    
    send_mail(
        subject = subject,
        message = message,
        from_email = from_email,
        recipient_list=[user_email],
        fail_silently=False,
    )

def report_final_failure(context):
    title = context['dag_run'].conf.get('title')
    user_email = context['dag_run'].conf.get('user_email')
    
    message = f'요청하신 Test "{title}"이 업로드에 실패하였습니다. \n관리자에게 문의하세요.'
    subject = 'DAG 오류 알림'
    send_email('hyukjun_jo@lgensol.com', subject, message)
    send_email(user_email, subject, message)

def report_final_success(context):
    title = context['dag_run'].conf.get('title')
    user_email = context['dag_run'].conf.get('user_email')
    print('성공')
    message = f'요청하신 Test "{title}"이 업로드되었습니다. \n10.99.212.69:8443 에서 확인하세요.'
    subject = 'Test 업로드 알림'
    send_email(user_email, subject, message)        
    
def get_cell_id_from_conf(**context):
    cell_id = context['dag_run'].conf.get('cell_id')
    title = context['dag_run'].conf.get('title')
    user_email = context['dag_run'].conf.get('user_email')
    exp_id = context['dag_run'].conf.get('exp_id')

    if cell_id and title:
        context['ti'].xcom_push(key='cell_id', value=cell_id)
        context['ti'].xcom_push(key='title', value=title)
        context['ti'].xcom_push(key='user_email', value=user_email)
        context['ti'].xcom_push(key='exp_id', value=exp_id)

        print(f"Cell ID:{cell_id} / title:{title} / mail:{user_email} / EXP ID:{exp_id} 을 Xcom에 성공적으로 저장.")
        return cell_id
    else:
        raise ValueError("Error: 파라미터가 conf에 전달되지 않았습니다.")

script_dir = r"./workspace/scripts"

with DAG(
    "subscription_trigger",
    default_args={
        "cwd":script_dir,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="data pipeline for voltaflow",
    # schedule="0 0 * * *",
    start_date=datetime(2025, 8, 21),
    catchup=False,
    tags=["example"],
) as dag:
    
    extract_cell_id_task = PythonOperator(
        task_id="get_cell_id_from_conf",
        python_callable=get_cell_id_from_conf,
    )
    #extract_cell_id_task

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    process_data_task = BashOperator(
        task_id="data_gathering",
        on_failure_callback=report_final_failure,
        on_success_callback=report_final_success,
        bash_command="""
        CELL_ID="{{task_instance.xcom_pull(task_ids='get_cell_id_from_conf', key='cell_id')}}"
        TITLE="{{task_instance.xcom_pull(task_ids='get_cell_id_from_conf', key='title')}}"
        USER_EMAIL="{{task_instance.xcom_pull(task_ids='get_cell_id_from_conf', key='user_email')}}"
        EXP_ID="{{task_instance.xcom_pull(task_ids='get_cell_id_from_conf', key='exp_id')}}"

        python ./subscription_trigger.py --cell_id="$CELL_ID" --title="$TITLE" --user_email="$USER_EMAIL" --exp_id="$EXP_ID"
        """,
    )

    extract_cell_id_task >> process_data_task