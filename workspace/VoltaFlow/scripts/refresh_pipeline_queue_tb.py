import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

from src.voltaflow.connectors.db import DbConnector
from psycopg2 import Error
import requests
import os
from pathlib import Path
from src.voltaflow.parsers.Factory import FactoryProcessor
from assets import config
from src.voltaflow.connectors.minio import MinioConnector
import botocore
import io
import time
import re
    
def refresh_table():
    try:
        db_conn = DbConnector(host, database, user, password, port)
        db_conn.connect()
        
        DESTINATION_TABLE_NAME = "pipeline_queue_tb"
        
        query = f"""
        UPDATE {DESTINATION_TABLE_NAME} 
        SET download_required_yn = false
        WHERE (test_state = '완료' and 
        test_file_upload_request_state = 'COMPLETE' and
        last_download_datetime IS NOT NULL and
        last_minio_upload_datetime IS NOT NULL and
        last_db_insert_datetime IS NOT NULL) 
        OR (
        file_full_path_cts IS NULL and
        file_full_path_cyc IS NULL
            );
        """
        result = db_conn._execute_query(query)
        if result:
            db_conn.commit()
        
    except (Exception, Error) as error:
        print(f"데이터베이스 작업 중 오류 발생: {error}")
        if db_conn:
            db_conn.rollback() # 오류 발생 시 롤백
    
    finally:
        db_conn.disconnect()
        
if __name__ == "__main__":
    host = os.getenv("DB_HOST")
    database = "mart_db"
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT")
    
    refresh_table()