import boto3
from dotenv import load_dotenv
import os
from botocore.config import Config

load_dotenv(verbose=True)

class MinioConnector():
    def __init__(self, bucket_name=None):
        try:     
            custom_config = Config(
                connect_timeout=10, 
                read_timeout=1800, 
                retries={'max_attempts': 3},
                # 여기에 signature_version을 추가합니다.
                signature_version='s3v4' 
            )

            self.client = boto3.client(
                's3',
                endpoint_url=os.getenv("MINIO_ENDPOINT"),
                aws_access_key_id=os.getenv("access_key"),
                aws_secret_access_key=os.getenv("secret_key"),
                # 통합된 Config 객체를 한 번만 전달합니다.
                config=custom_config 
            )
            print("Minio connection established successfully")
            
        except Exception as e:
            raise Exception("Failed to connect to MinIO", print(e))
        
        self.bucket_name = bucket_name
        
    def upload_file(self, file_name, object_name=None):
        try:
            self.client.upload_file(file_name, self.bucket_name, object_name)
        except:
            raise Exception("Failed to upload file to MinIO")
            
