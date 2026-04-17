import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

from src.voltaflow.connectors.db import DbConnector
import requests
import os
import time
import pandas as pd
from pathlib import Path
from src.voltaflow.parsers.Factory import FactoryProcessor
from src.voltaflow.connectors.minio import MinioConnector
from assets import config
from db_copy import *
from tos_download_to_minio import *
from minio_to_db import *
import argparse

# Cell ID 입력 받기
# Cell ID로 정보 조회
# 조회 정보 기반 실험 데이터 요청하기
# 대기
# tmp에 다운로드 후 minio에 업로드
# minio에 업로드 된 파일 DB에 반영

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

host = os.getenv("DB_HOST")
database = "mart_db"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")

def check_data_availability(cell_id, exp_id):
    """
    exp_id 기반으로 데이터 존재 여부 확인.
    """
    try:
        db_conn = DbConnector(host, database, user, password, port)
        db_conn.connect()
        DESTINATION_TABLE_NAME = "exp_fact_tb"
        query = f"""SELECT * FROM {DESTINATION_TABLE_NAME} 
        WHERE exp_id = '{exp_id}' and cell_id = '{cell_id}';
        """
        exp_fact_tb = db_conn.fetch_data(query ,as_dataframe=True)
        
    except (Exception, Error) as error:
        print(f"데이터베이스 작업 중 오류 발생: {error}")
        if db_conn:
            db_conn.rollback() # 오류 발생 시 롤백
    
    finally:
        db_conn.disconnect()
        
    if not exp_fact_tb.empty:
        return True
    else:
        return False

def get_info(title, cell_id, exp_id):
    """ cell_id 기반 TOS 검색해서 정보 추출.

    Args:
        cell_id (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    print(f"title: {title} / cell_id: {cell_id} / exp_id: {exp_id}")
    
    search_results = []
    for site in ['cn', 'kr']:
        # 1-1. request_test_results 호출 (함수화)
        search_result = get_test_results(site, title)
        print(site, "검색 결과 수:", len(search_result))
        if search_result.empty:
            continue
        search_result = search_result[search_result["testId"] == exp_id]
        search_result['file_paths'] = search_result['testId'].apply(lambda x: get_file_paths(site, x))
                        # 1-4. 파일 경로 정보 추출
        search_result[['file_full_path_cts', 'file_full_path_cyc']] = search_result['file_paths'].apply(
                        lambda x: pd.Series(process_file_info(x))
                        )
        search_result = search_result.rename(columns={'testId': 'exp_id', 
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
        search_result["cell_id"] = cell_id
    
        search_results.append(search_result)
        
    search_results = pd.concat(search_results, ignore_index=True)
    if len(search_results) >= 2: # 동일 title 여러개일 때 처리
        search_results = search_results[search_results['test_title'] == title]
        # 1-3. 파일 경로 데이터 수집 및 병합
    return search_results.reset_index(drop=True)

def request_data(pipeline_queue_tb):
    """ 데이터 받아서 한줄씩 read, 후 데이터 요청

    Args:
        pipeline_queue_tb (_type_): _description_
    """
    for idx, row in pipeline_queue_tb.iterrows():
        if row['outter_ip'] is None or row['exp_id'] is None:
            print(f"Row {idx}에서 outter_ip 또는 exp_id가 None입니다. 건너뜁니다.")
            continue
        
        payload = {"analyzerUrl": row["outter_ip"],
                    "testId":row["exp_id"]}

        if row['test_site'] == '대전' or row['test_site'] == 'EP2':
            header = header_kr
            url = "http://toskr.lgensol.com:8009/tos/test/resultCdms/requestTestResultData.ajax"
        else:
            header = header_cn 
            url = "http://toscn.lgensol.com:8009/tos/test/resultCdms/requestTestResultData.ajax"
        
        try:     
            res = requests.post(url,
                            headers=header,
                            data=payload)
        except Exception as e:
            print("데이터 요청 중 오류 발생:", e)
                
        if res.status_code == 200:
            print(row["test_title"], row["exp_id"], "데이터 요청 성공")
        else:
            print(row["test_title"], row["exp_id"], "데이터 요청 실패", res.status_code)

def download_and_upload_minio(pipeline_queue_tb):
    
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    temp_folder = cur_dir.parent / "tmp"
    minio_paths = []
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        
    for idx in range(pipeline_queue_tb.shape[0]):
        download_url = pipeline_queue_tb["file_full_path_cts"][idx]
        if download_url is None:
            continue
        download_url = download_url.replace('dat.','dat')
        
        file_name = os.path.basename(download_url)
        output_path = os.path.join(temp_folder, file_name)
        try:
            res_code = download_file_from_db(download_url, output_path)
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
        minio_paths.append(minio_path)
        upload_to_minio(output_path, "tos-stepend", minio_path)

    return minio_paths

def process_minio_to_db(minio_paths):
    minio_connector = MinioConnector()
    
    for minio_path in minio_paths:
        cell_id = minio_path.split("/")[0]
        exp_id = minio_path.split("/")[1]
        df = parquet_to_df(minio_connector, minio_path, cell_id=cell_id, exp_id=exp_id)
        if df.empty:
            print(f"Skipping empty dataframe for {minio_path}")
            continue
        # Extract cell_id and exp_id from the file pat
        
        # Insert data into the database
        insert_to_db(df, cell_id, exp_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cell_id", type=str, required=True, help="Cell ID to process")
    parser.add_argument("--title", type=str, required=True, help="title to search")
    parser.add_argument("--user_email", type=str, required=True)
    parser.add_argument("--exp_id", type=str, required=True)
    parser.add_argument("--cts_path", type=str, required=False)
    parser.add_argument("--cyc_path", type=str, required=False)
    args = parser.parse_args()
    
    cell_id = args.cell_id
    title = args.title
    user_email = args.user_email
    exp_id = args.exp_id
    cts_path = args.cts_path
    cyc_path = args.cyc_path
    
    try:
        flag = check_data_availability(cell_id,exp_id)
        if flag == False:
            search_results = get_info(title, cell_id, exp_id)
            print(search_results["file_full_path_cts"])

            if search_results["file_full_path_cts"].isnull().all() == True: # 260416 hotfix: 첫 요청으로 path 없는 경우는 요청 후 다시 get_info 호출 
                request_data(search_results)
                print("60초 대기")
                time.sleep(60)
                search_results = get_info(title, cell_id, exp_id)
            else:
                request_data(search_results)

            if (cts_path != "" or cts_path is not None) and len(cts_path) > 10:
                search_results["file_full_path_cts"] = cts_path
                search_results["file_full_path_cyc"] = cyc_path # 한 EXP_ID에 시험이 여러 개 인 경우 대비하여 받아온 path로 변경
                
            minio_paths = download_and_upload_minio(search_results)
            process_minio_to_db(minio_paths)
        else:
            print("이미 데이터가 존재합니다. 건너뜁니다.")
        
    except Exception as e:
        # 어떤 종류의 에러가 났는지, 에러 메시지는 무엇인지 출력합니다.
        print("발생한 에러 종류:", type(e).__name__) # 에러 클래스 이름 출력 (예: ZeroDivisionError)
        print("에러 상세 메시지:", e) # 에러 객체 e의 내용 출력
        sys.exit(1)
          
    else:
        print("업로드 완료")
        sys.exit(0) # 혹은 sys.exit()
        