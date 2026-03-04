import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

from src.voltaflow.connectors.db import DbConnector
from psycopg2 import Error
from utils.utils import send_email
import os

host = os.getenv("DB_HOST")
database_mart = "mart_db"
database_req = "req_sub_db"
database_account = "accounts_db"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")

def get_email_data():
    """
    Fetches data by joining subscription_data with account_data, and then with pipelinequeue_tb.
    """
    try:
        # Connect to req_sub_db for subscription_data
        req_db_conn = DbConnector(host, database_req, user, password, port)
        req_db_conn.connect()
        subscription_query = "SELECT cell_id, data_requester, subscription, exp_id FROM subscription_data;"
        subscription_data = req_db_conn.fetch_data(subscription_query, as_dataframe=True)
        subscription_data = subscription_data.rename(columns={"data_requester": "name"})
        # Connect to accounts_db for account_data
        account_db_conn = DbConnector(host, database_account, user, password, port)
        account_db_conn.connect()
        account_query = "SELECT name, email FROM account_data;"
        account_data = account_db_conn.fetch_data(account_query, as_dataframe=True)
        
        # Merge subscription_data and account_data
        merged_data = subscription_data.merge(account_data, on="name", how="left")
        
        # Connect to mart_db for pipelinequeue_tb
        mart_db_conn = DbConnector(host, database_mart, user, password, port)
        mart_db_conn.connect()
        pipeline_query = """
        SELECT exp_id, test_title, download_required_yn, last_download_datetime
        FROM pipeline_queue_tb;
        """
        pipeline_data = mart_db_conn.fetch_data(pipeline_query, as_dataframe=True)
        
        # Merge with pipelinequeue_tb data
        email_data = merged_data.merge(pipeline_data, on="exp_id", how="left")
        
    except (Exception, Error) as error:
        print(f"데이터베이스 작업 중 오류 발생: {error}")
        if req_db_conn:
            req_db_conn.rollback()
        if account_db_conn:
            account_db_conn.rollback()
        if mart_db_conn:
            mart_db_conn.rollback()
    finally:
        if req_db_conn:
            req_db_conn.disconnect()
        if account_db_conn:
            account_db_conn.disconnect()
        if mart_db_conn:
            mart_db_conn.disconnect()
    
    return email_data

def main():
    df = get_email_data()
    df = df.drop_duplicates(subset=['cell_id', 'exp_id','email'])
    
    for email in df['email'].unique():
        temp = df[(df['email'] == email) & (df['subscription'] =='Y')]
        if temp.empty:
            continue
        name = temp['name'].values[0]
        row_num_total = len(temp)
        row_num_required = len(temp[temp['download_required_yn'] == True])
        
        if row_num_required == 0 or name == 'admin':
            continue
                # List of experiments with download_required_yn = True
        
        required_experiments = temp[temp['download_required_yn'] == True]['test_title'].tolist()
        experiments_list = "\n".join([f"-. {test_title}" for test_title in required_experiments])
        message = message = f"""
==================================================
{name} 님의 데이터 파이프라인 업데이트 현황
==================================================

[요약 정보]
--------------------------------------------------
* 총 구독 실험 수: {row_num_total}개
* 종료된 실험 수: {row_num_total - row_num_required}개
* 업데이트 완료 실험 수: {row_num_required}개
--------------------------------------------------

[업데이트 완료 실험 목록]
--------------------------------------------------
{experiments_list}
--------------------------------------------------

실험 데이터가 시스템에 성공적으로 반영되었습니다.
확인 주소: http://10.99.212.69:8443
"""
        subject = "Pipeline Update Notification"
        send_email(email, subject, message)

if __name__ == "__main__":
    main()