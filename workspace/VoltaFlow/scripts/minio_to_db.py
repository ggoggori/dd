import sys
sys.path.append("..")
import os
import io
import pandas as pd
from psycopg2 import Error
from src.voltaflow.connectors.minio import MinioConnector
from src.voltaflow.connectors.db import DbConnector

# 1. DB 설정 공통화
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": "mart_db",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

def get_db_connector():
    return DbConnector(**DB_CONFIG)

def get_required_exp_ids():
    db_conn = get_db_connector()
    try:
        db_conn.connect()
        # TRUE는 SQL 표준이며, 소문자로 써도 무방합니다.
        query = "SELECT exp_id FROM pipeline_queue_tb WHERE download_required_yn = TRUE;"
        df = db_conn.fetch_data(query, as_dataframe=True)
        return set(df['exp_id'].astype(str).tolist()) if not df.empty else set()
    finally:
        db_conn.disconnect()

def scan_minio(minio_connector, required_exp_ids):
    rows_to_insert = []
    paginator = minio_connector.client.get_paginator('list_objects_v2') 
    pages = paginator.paginate(Bucket="tos-stepend")
    
    for page in pages:
        if 'Contents' in page:
            for row in page['Contents']:
                key = row['Key']
                path_parts = key.split("/")
                if len(path_parts) >= 2:
                    exp_id = path_parts[1]
                    if exp_id in required_exp_ids:
                        rows_to_insert.append(key)
    return rows_to_insert

def parquet_to_df(minio_connector, minio_path, cell_id, exp_id):
    res = minio_connector.client.get_object(Bucket="tos-stepend", Key=minio_path)
    df = pd.read_parquet(io.BytesIO(res['Body'].read()))
    # 처음부터 소문자로 할당하면 더 깔끔합니다.
    df["cell_id"] = cell_id
    df["exp_id"] = exp_id
    df.columns = [col.lower() for col in df.columns]
    return df

def insert_to_db(df, cell_id, exp_id):
    db_conn = get_db_connector()
    try:
        db_conn.connect()
        DESTINATION_TABLE_NAME = "exp_fact_tb"
        
        # 1. 중복 체크
        query = f"SELECT * FROM {DESTINATION_TABLE_NAME} WHERE exp_id = %s and cell_id = %s;"
        temp_db = db_conn.fetch_data(query, params=(exp_id, cell_id), as_dataframe=True)
        temp_db = temp_db.drop("serial_id", axis=1, errors='ignore')
        
        if not temp_db.empty and temp_db.shape[0] == df.shape[0]:
            print(f"{cell_id}/{exp_id} 최신 데이터가 이미 존재합니다. 건너뜁니다.")
            return
        
        # 2. 삭제 후 재삽입
        delete_query = f"DELETE FROM {DESTINATION_TABLE_NAME} WHERE exp_id = %s and cell_id = %s;"
        db_conn.cursor.execute(delete_query, (exp_id, cell_id))
        
        cols = temp_db.columns
        insert_cols = ', '.join(cols)
        placeholders = ', '.join(['%s'] * len(cols))
        insert_query = f"INSERT INTO {DESTINATION_TABLE_NAME} ({insert_cols}) VALUES ({placeholders})"
        
        # 데이터 정렬 및 삽입
        target_data = df[cols].values.tolist()
        db_conn.cursor.executemany(insert_query, target_data)
        
        db_conn.commit()
        print(f"{exp_id} 데이터 삽입 완료")
        
        db_conn.update_timestamp_in_db(cell_id, exp_id, "last_db_insert_datetime")
    
    except (Exception, Error) as error:
        print(f"DB 작업 중 오류 발생: {error}")
        db_conn.rollback()
        raise # 상위 호출자(main)에게 오류 전달
    finally:
        db_conn.disconnect()

def main(**kwargs):
    failure_exp_ids = []
    minio_connector = MinioConnector()
    # 괄호 추가!
    required_exp_ids = get_required_exp_ids()
    
    if not required_exp_ids:
        print("처리할 exp_id가 없습니다.")
        return

    rows_to_insert = scan_minio(minio_connector, required_exp_ids)
    print(f"처리 대상 파일 수: {len(rows_to_insert)}")
    
    for minio_path in rows_to_insert:
        try:
            print(f"Processing: {minio_path}")
            parts = minio_path.split("/")
            cell_id, exp_id = parts[0], parts[1]
            
            df = parquet_to_df(minio_connector, minio_path, cell_id, exp_id)
            if df.empty:
                continue
            
            insert_to_db(df, cell_id, exp_id)
        except Exception as e:
            failure_exp_ids.append([cell_id, exp_id])
            print(f"실패: {minio_path} 에러: {e}")
            continue # 다음 파일로 진행
        
    ti = kwargs.get('ti')
    if ti:
        ti.xcom_push(key='error_summary', value=failure_exp_ids)
        
if __name__ == "__main__":
    main()