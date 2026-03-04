import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

import botocore.exceptions
from src.voltaflow.connectors.db import DbConnector
from psycopg2 import Error
import requests
import os
import json
from pathlib import Path
from src.voltaflow.parsers.Factory import FactoryProcessor
from assets import config
from src.voltaflow.connectors.minio import MinioConnector
import botocore
import io
import time
import re
from utils.utils import process_rpt


    
host = os.getenv("DB_HOST")
database = "mart_db"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")
    
def get_pipeline_queue_tb():
    try:
        db_conn = DbConnector(host, database, user, password, port)
        db_conn.connect()
        DESTINATION_TABLE_NAME = "pipeline_queue_tb"
        query = f"""SELECT * FROM {DESTINATION_TABLE_NAME} 
        WHERE download_required_yn IS NULL OR download_required_yn = TRUE;
        """
        pipeline_queue_tb = db_conn.fetch_data(query ,as_dataframe=True)
        
    except (Exception, Error) as error:
        print(f"데이터베이스 작업 중 오류 발생: {error}")
        if db_conn:
            db_conn.rollback() # 오류 발생 시 롤백
    
    finally:
        db_conn.disconnect()
    
    return pipeline_queue_tb

def download_file_from_db(url, output_path, cell_id=None, exp_id=None):
    """
    Downloads a file from the database based on the index and saves it to the target path.
    """
    start_time = time.time()
    with requests.get(url) as res:
        if res.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(res.content)
            print(f"파일 다운로드 성공 :{url}")
            
            try:
                db_conn = DbConnector(host, database, user, password, port)
                db_conn.connect()
                db_conn.update_timestamp_in_db(cell_id, exp_id, "last_download_datetime")
            except (Exception, Error) as error:
                print(f"데이터베이스 작업 중 오류 발생: {error}")
            finally:
                db_conn.disconnect()
        else:
            print(f"파일 다운로드 실패: {res.status_code}")

    print(f"Download time: {time.time() - start_time} seconds")
    return res.status_code
            
def upload_to_minio(file_path, bucket_name, object_path, cell_id=None, exp_id=None, cell_type=None):
    processor = FactoryProcessor.create_processor(str(file_path), config)
    try:
        stepend_df = processor.parse_binary_file(file_path)['StepEndData']
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        os.remove(file_path)
        return    
    
    stepend_df = process_rpt(stepend_df, cell_type)
    
    pq_buffer = io.BytesIO()
    stepend_df.to_parquet(pq_buffer, index=False, compression='snappy')
    pq_buffer.seek(0)
    pq_size = pq_buffer.getbuffer().nbytes
    
    try:    
        minio_connector = MinioConnector()
        res = minio_connector.client.put_object(Bucket=bucket_name,
                                                Key=object_path,
                                                Body=pq_buffer,
                                                ContentLength=pq_size,
                                                ContentType='application/x-parquet'
                                                )
        # Update timestamp in the database
        db_conn = DbConnector(host, database, user, password, port)
        db_conn.connect()
        db_conn.update_timestamp_in_db(cell_id, exp_id, "last_minio_upload_datetime")
            
    except botocore.exceptions.ConnectTimeoutError as e:
        print(f"ConnectTimeoutError: {e}")
        raise 
    except botocore.exceptions.ClientError as e:
        print("######################################")
        print(f"CliendError: {e}")
        print("######################################")

    else:
        os.remove(file_path)
        print(f"Uploaded to MinIO: {bucket_name}/{object_path}")
    
    finally:
        if 'db_conn' in locals():
            db_conn.disconnect()
    
def main():
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    temp_folder = cur_dir.parent / "tmp"
    
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        
    pipeline_queue_tb = get_pipeline_queue_tb()
    
    for idx in range(pipeline_queue_tb.shape[0]):
        download_url = pipeline_queue_tb["file_full_path_cts"][idx]
        cell_id = pipeline_queue_tb["cell_id"][idx]
        exp_id = pipeline_queue_tb["exp_id"][idx]
        cell_type = pipeline_queue_tb["cell_type"][idx]
        
        if download_url is None:
            continue
        file_name = os.path.basename(download_url)
        output_path = os.path.join(temp_folder, file_name)

        try:
            res_code = download_file_from_db(download_url, output_path, cell_id=cell_id, exp_id=exp_id)
            if res_code != 200:
                print(f"Failed to download file from {download_url}. Skipping...")
                continue
        except requests.exceptions.ConnectTimeout as e:
            print(f"ConnectTimeoutError: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            continue
        
        minio_path = pipeline_queue_tb['cell_id'][idx] + "/" + \
                    pipeline_queue_tb['exp_id'][idx] + "/" + \
                    re.sub(r'cts|dat', 'parquet', file_name)
        upload_to_minio(output_path, "tos-stepend", minio_path, cell_id=cell_id, exp_id=exp_id, cell_type=cell_type)

        time.sleep(1)
        
if __name__ == "__main__":    
    main()