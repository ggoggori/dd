import sys
# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

from src.voltaflow.connectors.db import DbConnector
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

if __name__ == "__main__":
    host = os.getenv("DB_HOST")
    database = "mart_db"
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT")

    try:
        db_conn = DbConnector(host, database, user, password, port)
        db_conn.connect()
        DESTINATION_TABLE_NAME = "pipeline_queue_tb"
        query = f"SELECT * FROM {DESTINATION_TABLE_NAME} WHERE download_required_yn = TRUE;"
        pipeline_queue_tb = db_conn.fetch_data(query ,as_dataframe=True)
    except:
        db_conn.rollback() # 오류 발생 시 롤백
        raise
    finally:
        db_conn.disconnect()
    
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
            
        time.sleep(2)