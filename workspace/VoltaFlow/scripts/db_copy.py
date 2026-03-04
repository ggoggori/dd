import psycopg2
from psycopg2 import Error
import pandas as pd
import requests
from dotenv import load_dotenv
load_dotenv(verbose=True)
import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")
import os

header_kr = { 
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '479',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'L-VISITOR=z2qurp1uebcoa6; JSESSIONID=542798FFA6087D23582E25EBA128DA80.6134f0ddc45e87161; InitechEamUURL=http%3A%2F%2Ftoscn.lgensol.com%3A8009%2Ftos%2Fmain.do; InitechEamRTOA=1; CLP=; InitechEamCookieEnc=T; LGCHEM_NC_FLAG=TRUE; ep_mode=F; InitechEamNCFlag=F; language=ko; change=Y; InitechEamUID=1mL53LU3fyaHIWvC6qPkMg%3D%3D; InitechEamUTOA=1; InitechEamUPID=XQDvTAgJ4afUQUwvgr1wJA%3D%3D; eWLanguage=ko-KR; tempLGDPID=hyukjun_jo; InitechEamDomain=LGENSOL; SCOUTER=z2bd0rldnhe3l8; leftMenuType=list; _ga=GA1.2.521480946.1681885360; _ga_JMHFCNW1FS=GS2.1.s1747959829$o32$g1$t1747959848$j41$l0$h0$ds4wgRbOLl7W1quhcvsYoxmV5slhbgmyuhg; _ga_QMD5NW261X=GS2.1.s1747959830$o31$g1$t1747959848$j0$l0$h0; InitechEamUIP=EaIGJz9JlvRTXewdIhl5Pg%3D%3D; InitechEamULAT=1748310792; InitechEamUHMAC=0SbTDvsBWhpZP7fJjhlMJhSpJ5iVj48xJpjQygsv2b0%3D; engpuid=pps1W+dAfqoY+W6Al2Qfytulp9ocriXZHmThkwuCDXs=; LENA-UID=17224de2.638bf38482291',
    'Host': 'toskr.lgensol.com:8009',
    'Origin': 'http://toskr.lgensol.com:8009',
    'Pragma': 'no-cache',
    'Referer': 'http://toskr.lgensol.com:8009/tos/testResultDownload/retrieveTstRltDldList.do?_selectedMenuId=353',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

header_cn = { 
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '496',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'L-VISITOR=z6drvdmv45p3ji; JSESSIONID=D52068096290FD9504B5C9632C4B60B6.5814bece043f87161; InitechEamUURL=http%3A%2F%2Ftoscn.lgensol.com%3A8009%2Ftos%2Fmain.do; InitechEamRTOA=1; CLP=; InitechEamCookieEnc=T; LGCHEM_NC_FLAG=TRUE; ep_mode=F; InitechEamNCFlag=F; language=ko; change=Y; InitechEamUID=1mL53LU3fyaHIWvC6qPkMg%3D%3D; InitechEamUTOA=1; InitechEamUPID=XQDvTAgJ4afUQUwvgr1wJA%3D%3D; eWLanguage=ko-KR; tempLGDPID=hyukjun_jo; InitechEamDomain=LGENSOL; installRetryCookie=2; _ga=GA1.2.521480946.1681885360; _ga_JMHFCNW1FS=GS2.1.s1747959829$o32$g1$t1747959848$j41$l0$h0$ds4wgRbOLl7W1quhcvsYoxmV5slhbgmyuhg; _ga_QMD5NW261X=GS2.1.s1747959830$o31$g1$t1747959848$j0$l0$h0; InitechEamUIP=EaIGJz9JlvRTXewdIhl5Pg%3D%3D; InitechEamULAT=1748310792; InitechEamUHMAC=0SbTDvsBWhpZP7fJjhlMJhSpJ5iVj48xJpjQygsv2b0%3D; engpuid=pps1W+dAfqoY+W6Al2Qfytulp9ocriXZHmThkwuCDXs=; LENA-UID=c28b0916.638bed54b0739; leftMenuType=list',
    'Host': 'toscn.lgensol.com:8009',
    'Origin': 'http://toscn.lgensol.com:8009',
    'Pragma': 'no-cache',
    'Referer': 'http://toscn.lgensol.com:8009/tos/testResultDownload/retrieveTstRltDldList.do?_selectedMenuId=310',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

def request_test_results(site, sub ,payload):
    if sub == "retrieveTstRltFileList":
        base_url = "http://tos{}.lgensol.com:8009/tos/testResultDownload/{}.ajax"
    else: # 'retrieveTestResultList', 'requestTestResultData
        base_url = "http://tos{}.lgensol.com:8009/tos/test/resultCdms/{}.ajax"
    
    res = requests.post(base_url.format(site, sub),
                headers=header_kr if site == 'kr' else header_cn,
                data=payload,
                timeout=30)
    return res

def get_test_results(site, cell_id):
    payload = {"searchTestTitle": cell_id}
    res = request_test_results(site, "retrieveTestResultList", payload)
    if res.status_code == 200:
    # 200 (성공) 상태 코드인 경우에만 JSON 파싱 시도
        try:
            search_result = pd.DataFrame.from_dict(res.json()['rows'])
            return search_result
        except requests.exceptions.JSONDecodeError as e:
            # 200이지만 응답이 잘못된 JSON일 경우 (매우 드물게 발생)
            print("200 응답이지만 JSON 디코딩 실패:", e)
            print("응답 본문:", res.text)
    else:
        # 200 외의 상태 코드 (4xx, 5xx)인 경우
        print(f"API 요청 실패. 상태 코드: {res.status_code}")
        # 실패 응답 본문을 확인하여 서버 측 메시지를 파악
        print("실패 응답 본문:", res.text)   
        return pd.DataFrame([])
    
def get_file_paths(site, test_id):
    payload = {"testId": test_id}
    res = request_test_results(site, "retrieveTstRltFileList", payload)
    return res.json()['rows']

def process_file_info(file_list):
    cts_path, cyc_path = None, None
    for item in file_list:
        file_path = item["fileFullPath"]
        ext = os.path.splitext(item["fileFullPath"])[-1]
        if ext == '.cts' :
            cts_path = file_path
        elif ext == '.cyc':
            cyc_path = file_path
        elif ext == '.dat':
            cts_path = item["fileFullPath"]
            cyc_path = item["fileFullPath"]
        
    return cts_path, cyc_path
        
def copy_subscripton_tb_to_pipeline_queue_tb():
    # --- 데이터베이스 연결 설정 ---
    # django_test 데이터베이스 연결 정보
    DB_DJANGO_HOST = os.getenv("DB_HOST")
    DB_DJANGO_NAME = "req_sub_db"
    DB_DJANGO_USER = os.getenv("DB_USER")
    DB_DJANGO_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DJANGO_PORT = "5432" # 기본 PostgreSQL 포트

    # mart_db 데이터베이스 연결 정보
    DB_MART_HOST = os.getenv("DB_HOST")
    DB_MART_NAME = "mart_db"
    DB_MART_USER = os.getenv("DB_USER")
    DB_MART_PASSWORD = os.getenv("DB_PASSWORD")
    DB_MART_PORT = "5432" # 기본 PostgreSQL 포트

    # 원본 테이블 이름 (django_test DB에 있는 테이블)
    SOURCE_TABLE_NAME = "subscription_data" # 예: 'subscription_data'
    # 대상 테이블 이름 (mart_db DB에 있는 테이블)
    DESTINATION_TABLE_NAME = "pipeline_queue_tb"
    try:
        django_conn = None
        mart_conn = None
        # 1. django_test 데이터베이스에 연결
        django_conn = psycopg2.connect(
            host=DB_DJANGO_HOST,
            database=DB_DJANGO_NAME,
            user=DB_DJANGO_USER,
            password=DB_DJANGO_PASSWORD,
            port=DB_DJANGO_PORT,
        )
        django_cursor = django_conn.cursor()
        print(f"{DB_DJANGO_NAME} 데이터베이스 연결 성공.")

        # 2. mart_db 데이터베이스에 연결
        mart_conn = psycopg2.connect(
            host=DB_MART_HOST,
            database=DB_MART_NAME,
            user=DB_MART_USER,
            password=DB_MART_PASSWORD,
            port=DB_MART_PORT
        )
        mart_cursor = mart_conn.cursor()
        print(f"{DB_MART_NAME} 데이터베이스 연결 성공.")

        # 3. django_test에서 데이터 읽기
        print(f"'{SOURCE_TABLE_NAME}' 테이블에서 데이터 읽기...")
        # 원본 테이블의 모든 컬럼을 선택합니다.
        
        df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE_NAME};", django_conn)
        df = df[["cell_id", "cell_type", "test_title", "cp", "temperature", "build_type", "build_version", "data_requester"]]
        
        print(f"{len(df)}개의 Cell을 읽었습니다.")
        
        # mart_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{DESTINATION_TABLE_NAME}' ORDER BY ordinal_position;")

        dfs = []
        for _, row in df.iterrows():
            for site in ['cn', 'kr']:
                # 1-1. request_test_results 호출 (함수화)
                search_result = get_test_results(site, row['test_title'])
                if search_result.empty:
                    continue
                # 1-2. 원본 df의 행 정보를 search_result에 복사
                for col in ["cell_id", "cell_type", "cp", "temperature", "build_type", "build_version", "data_requester"]:
                    search_result[col] = row[col]
    
                # 1-3. 파일 경로 데이터 수집 및 병합
                search_result['file_paths'] = search_result['testId'].apply(lambda x: get_file_paths(site, x))
                
                # 1-4. 파일 경로 정보 추출
                search_result[['file_full_path_cts', 'file_full_path_cyc']] = search_result['file_paths'].apply(
                    lambda x: pd.Series(process_file_info(x))
                )
                
                if not "testEndDatetime" in search_result.columns:
                    search_result["testEndDatetime"] = None
                # 1-5. 최종 결과 리스트에 추가
                dfs.append(search_result)
                
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.drop_duplicates(subset=['testId'], keep='first', inplace=True) # 같은 실험을 여러 사람이 요청했을 때는 한 EXP_ID 행만 가져오도록 함.
        # 3. 필요한 컬럼만 선택하여 upsert 쿼리 준비
        # 원하는 컬럼 목록을 리스트로 정의
        required_columns = ["testId", "data_requester", "userName", "deptNm", "cell_id", "cell_type", "cp", "temperature", "build_type", "build_version", "workplaceNm", 
                            "testStateNm", "outterIp", "testTitle", "testFullTitle", "testStateCd", "testStartDatetime", "testEndDatetime", "firstChnnStartDatetime", 
                            "lastChnnEndDatetime", "testFileLastUploadDatetime", "file_full_path_cts", "file_full_path_cyc"]
     
        insert_df = final_df[required_columns] 
        insert_df = insert_df.rename(columns={'testId': 'exp_id', 
                                            'userName': 'test_requester', 
                                            'deptNm': 'test_requester_team', 
                                            'workplaceNm': 'test_site', 
                                            'testStateNm': 'test_state', 
                                            'outterIp': 'outter_ip', 
                                            'testTitle': 'test_title', 
                                            'testFullTitle': 'test_full_title', 
                                            'testStateCd': 'test_file_upload_request_state', 
                                            'testStartDatetime': 'test_start_datetime', 
                                            'testEndDatetime': 'test_end_datetime', 
                                            'firstChnnStartDatetime': 'first_chnn_start_datetime', 
                                            'lastChnnEndDatetime': 'last_chnn_end_datetime', 
                                            'testFileLastUploadDatetime': 'test_file_last_upload_datetime'})
        upsert_columns = insert_df.columns
        # UPSERT 쿼리 생성
        # 대상 테이블의 'exp_id' 컬럼에 UNIQUE 제약 조건 또는 PRIMARY KEY가 설정되어 있어야 합니다.
        # 만약 설정되어 있지 않다면, 'ALTER TABLE public.pipeline_queue_tb ADD CONSTRAINT unique_exp_id UNIQUE (exp_id);' 와 같이 추가해야 합니다.
        
        # INSERT 부분에 사용될 컬럼 목록과 플레이스홀더
        insert_cols_str = ', '.join(upsert_columns)
        placeholders = ', '.join(['%s'] * len(upsert_columns))

        # DO UPDATE SET 부분에 사용될 컬럼 목록 (exp_id 제외)
        # 이 컬럼들이 변경되었을 때만 업데이트가 일어납니다.
        update_cols = ['exp_id', 'test_state', 'test_file_upload_request_state', 'test_end_datetime', 'last_chnn_end_datetime', 'test_file_last_upload_datetime']
        # 특정 컬럼이 바뀌었을 때만 업데이트되도록 WHERE 절 추가
        # 'target'은 현재 테이블에 존재하는 행을, 'EXCLUDED'는 삽입하려는 새 행을 나타냅니다.
        # 'IS DISTINCT FROM'은 NULL 값을 안전하게 비교하여 값이 다른 경우에만 TRUE를 반환합니다.
        where_clause_parts = []
        for col in update_cols:
            where_clause_parts.append(f"{DESTINATION_TABLE_NAME}.{col} IS DISTINCT FROM EXCLUDED.{col}")

        where_clause = ""
        if where_clause_parts:
            where_clause = f" WHERE {' OR '.join(where_clause_parts)}"
        
        set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in upsert_columns])
        
        upsert_query = f"""
            INSERT INTO public.{DESTINATION_TABLE_NAME} ({insert_cols_str})
            VALUES ({placeholders})
            ON CONFLICT (exp_id) DO UPDATE SET
                {set_clause}
            {where_clause};
        """
        
        # 5. mart_db에 데이터 삽입/업데이트
        print(f"'{DESTINATION_TABLE_NAME}' 테이블에 데이터 삽입/업데이트 중...")
        # executemany를 사용하여 여러 행을 한 번에 처리합니다.
        mart_cursor.executemany(upsert_query, insert_df.values)
        mart_conn.commit() # 변경사항 커밋
        print(f"'{DESTINATION_TABLE_NAME}' 테이블에 {mart_cursor.rowcount}개의 행이 성공적으로 처리되었습니다 (삽입 또는 업데이트).")
    
    except (Exception, Error) as error:
        print(f"데이터베이스 작업 중 오류 발생: {error}")
        if mart_conn:
            mart_conn.rollback() # 오류 발생 시 롤백
            
        raise
            
    finally:
        # 6. 연결 종료
        if django_conn:
            django_cursor.close()
            django_conn.close()
            print(f"{DB_DJANGO_NAME} 데이터베이스 연결 종료.")
        if mart_conn:
            mart_cursor.close()
            mart_conn.close()
            print(f"{DB_MART_NAME} 데이터베이스 연결 종료.")

def main():
    copy_subscripton_tb_to_pipeline_queue_tb()
    
if __name__ == "__main__":
    main()


