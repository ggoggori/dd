import psycopg2
from psycopg2 import Error
import pandas as pd # fetch_data에서 DataFrame 반환을 위해 필요합니다.
import os 
from dotenv import load_dotenv
import time
load_dotenv() 
    
class DbConnector():
    """
    PostgreSQL 데이터베이스 연결 및 삽입/조회 작업을 위한 클래스입니다.
    """
    def __init__(self, host, database, user, password, port):
        """
        DbConnector 객체를 초기화합니다.
        데이터베이스 연결 정보를 설정합니다.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        
    def connect(self):
        """
        데이터베이스에 연결합니다.
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print(f"데이터베이스 '{self.database}'에 성공적으로 연결되었습니다.")
            return True
        except Error as e:
            print(f"데이터베이스 연결 오류 발생: {e}")
            self.conn = None
            self.cursor = None
            return False

    def disconnect(self):
        """
        데이터베이스 연결을 종료합니다.
        """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            print("커서가 닫혔습니다.")
        if self.conn:
            self.conn.close()
            self.conn = None
            print("데이터베이스 연결이 종료되었습니다.")

    def _execute_query(self, query, params=None):
        """
        SQL 쿼리를 내부적으로 실행합니다. (INSERT, UPDATE, DELETE 등)
        결과를 반환하지 않으며, 성공/실패 여부를 True/False로 반환합니다.
        """
        if not self.conn:
            print("데이터베이스에 연결되어 있지 않습니다. 먼저 connect()를 호출하세요.")
            return False

        try:
            self.cursor.execute(query, params)
            return True
        except Error as e:
            print(f"쿼리 실행 오류 발생: {e}")
            self.conn.rollback() # 오류 발생 시 롤백
            return False

    def commit(self):
        """
        현재 트랜잭션을 커밋합니다.
        """
        if self.conn:
            self.conn.commit()
        else:
            print("데이터베이스에 연결되어 있지 않습니다.")

    def rollback(self):
        """
        현재 트랜잭션을 롤백합니다.
        """
        if self.conn:
            self.conn.rollback()
            print("트랜잭션이 롤백되었습니다.")
        else:
            print("데이터베이스에 연결되어 있지 않습니다.")

    def insert_data(self, table_name, data_dict):
        """
        단일 행 데이터를 테이블에 삽입합니다.
        data_dict: {컬럼명: 값, ...} 형태의 딕셔너리
        """
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s'] * len(data_dict))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        values = list(data_dict.values())
        
        return self._execute_query(query, values)

    def fetch_data(self, query, params=None, as_dataframe=False):
        """
        쿼리를 실행하여 데이터를 가져옵니다.
        as_dataframe=True로 설정하면 Pandas DataFrame으로 반환합니다.
        """
        if not self.conn:
            print("데이터베이스에 연결되어 있지 않습니다. 먼저 connect()를 호출하세요.")
            return None
        
        try:
            self.cursor.execute(query, params)
            if as_dataframe:
                column_names = [desc[0] for desc in self.cursor.description]
                data = self.cursor.fetchall()
                return pd.DataFrame(data, columns=column_names)
            else:
                return self.cursor.fetchall()
        except Error as e:
            print(f"데이터 가져오기 오류 발생: {e}")
            return None
        
    def update_timestamp_in_db(self, cell_id: str, exp_id: str, column_name: str):
        """
        지정된 cell_id와 exp_id 행의 특정 컬럼에 현재 시간을 기록합니다.
        """
        DESTINATION_TABLE_NAME = "pipeline_queue_tb"
        
        # 1. 현재 시간을 SQL 형식으로 준비
        current_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        
        try:
            # 쿼리에 컬럼 이름과 시간을 삽입 (컬럼 이름은 f-string, 시간은 따옴표 처리)
            query = f"""
            UPDATE {DESTINATION_TABLE_NAME} 
            SET {column_name} = '{current_timestamp}'
            WHERE cell_id = '{cell_id}' and exp_id = '{exp_id}';"""
            
            result = self._execute_query(query)
            if result:
                self.commit()
            
        except (Exception, Error) as error:
            print(f"데이터베이스 작업 중 오류 발생: {error}")
            print(f"DB 업데이트 시간 기록 실패: {column_name} / {cell_id} / {exp_id}")
            if self.conn:
                self.rollback() # 오류 발생 시 롤백
                
        finally:
            if self.conn:
                self.disconnect()

# --- 사용 예시 (이 부분은 db_connector.py 파일 외부에서 실행) ---
if __name__ == "__main__":
    # 데이터베이스 연결 시도
    if db_conn.connect():
        try:
            # 1. 데이터 삽입 예시
            print("\n--- 데이터 삽입 예시 ---")
            table_to_insert = "test_table" # 실제 테이블 이름으로 변경하세요
            
            # 테스트를 위해 테이블이 없으면 생성 (실제 사용 시에는 미리 생성되어 있어야 함)
            # db_conn._execute_query(f"""
            #     CREATE TABLE IF NOT EXISTS {table_to_insert} (
            #         id SERIAL PRIMARY KEY,
            #         name VARCHAR(100),
            #         value INTEGER
            #     );
            # """)
            # db_conn.commit()

            data_to_insert_1 = {'name': 'Alice', 'value': 100}
            data_to_insert_2 = {'name': 'Bob', 'value': 200}

            if db_conn.insert_data(table_to_insert, data_to_insert_1):
                print(f"'{data_to_insert_1}' 데이터 삽입 성공.")
            else:
                print(f"'{data_to_insert_1}' 데이터 삽입 실패.")
            
            if db_conn.insert_data(table_to_insert, data_to_insert_2):
                print(f"'{data_to_insert_2}' 데이터 삽입 성공.")
            else:
                print(f"'{data_to_insert_2}' 데이터 삽입 실패.")
            
            db_conn.commit() # 삽입 후 변경사항 커밋

            # 2. 데이터 조회 예시
            print("\n--- 데이터 조회 예시 ---")
            query_to_fetch = f"SELECT * FROM {table_to_insert} WHERE value > %s;"
            
            # Pandas DataFrame으로 조회
            df_result = db_conn.fetch_data(query_to_fetch, params=(150,), as_dataframe=True)
            if df_result is not None:
                print("조회된 데이터 (DataFrame):")
                print(df_result)
                if not df_result.empty:
                    print(f"첫 번째 행의 name: {df_result['name'].iloc[0]}")
            else:
                print("데이터 조회 실패.")

            # 튜플 리스트 형태로 조회
            list_result = db_conn.fetch_data(f"SELECT name, value FROM {table_to_insert} LIMIT 2;")
            if list_result is not None:
                print("\n조회된 데이터 (튜플 리스트):")
                print(list_result)

        except Exception as e:
            print(f"예상치 못한 오류 발생: {e}")
            db_conn.rollback() # 오류 발생 시 롤백
        finally:
            # 연결 종료
            db_conn.disconnect()
    else:
        print("데이터베이스 연결에 실패하여 작업을 수행할 수 없습니다.")