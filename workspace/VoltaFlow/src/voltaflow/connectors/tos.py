import requests
import json
import pandas as pd
import time
from datetime import  datetime
import os
import random

class TosConnector():
    def __init__(self):
        self.BASE_URL = "http://tos{}.lgensol.com:8009/tos/test/resultCdms/{}.ajax"

        self.header_kr = { 
                "Accept" : "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Content-Length": "23",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Cookie": "L-VISITOR=z2qurp1uebcoa6; JSESSIONID=E61974443D46EC4B8BD1F3548679E727.6134f0ddc45e87161; _ga_W4K6YD6SKQ=GS1.1.1718675776.1.1.1718675789.0.0.0; _ga_W4K6YD6SKQG-W4K6YD6SKQ=GS1.1.1718675777.1.1.1718675789.0.0.0; _fbp=fb.1.1731974383088.868934801414755891; _ga=GA1.1.521480946.1681885360; _ga_QMD5NW261X=GS1.1.1738630325.29.1.1738630376.0.0.0; _ga_JMHFCNW1FS=GS1.1.1738630325.30.1.1738630376.9.0.0; InitechEamUURL=http%3A%2F%2Ftoscn.lgensol.com%3A8009%2Ftos%2Fmain.do; InitechEamRTOA=1; CLP=; InitechEamCookieEnc=T; LGCHEM_NC_FLAG=TRUE; ep_mode=F; InitechEamNCFlag=F; language=ko; change=Y; InitechEamUID=1mL53LU3fyaHIWvC6qPkMg%3D%3D; InitechEamUIP=umxQQq46C0M6UuJdGIkf%2Fw%3D%3D; InitechEamULAT=1742965988; InitechEamUTOA=1; InitechEamUPID=XQDvTAgJ4afUQUwvgr1wJA%3D%3D; InitechEamUHMAC=q9%2BMFjtELSpwb7T7TTuROLpJGNCLwpqbvlPtr4NTt6g%3D; eWLanguage=ko-KR; engpuid=oHMNDv1VP83vQ91gJ4P1xNulp9ocriXZHmThkwuCDXs=; tempLGDPID=hyukjun_jo; InitechEamDomain=LGENSOL; LENA-UID=bcc9c6d8.6316178c7fee8; SCOUTER=z2bd0rldnhe3l8; leftMenuType=list",
                "Host": "toskr.lgensol.com:8009",
                "Origin": "http://toskr.lgensol.com:8009",
                "Referer""": "http://toskr.lgensol.com:8009/tos/test/resultCdms/retrieveTestResultFileList.pop",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
        }

        self.header_cn = { 
                "Accept" : "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Content-Length": "23",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Cookie": "L-VISITOR=z2qurp1uebcoa6; JSESSIONID=E61974443D46EC4B8BD1F3548679E727.6134f0ddc45e87161; _ga_W4K6YD6SKQ=GS1.1.1718675776.1.1.1718675789.0.0.0; _ga_W4K6YD6SKQG-W4K6YD6SKQ=GS1.1.1718675777.1.1.1718675789.0.0.0; _fbp=fb.1.1731974383088.868934801414755891; _ga=GA1.1.521480946.1681885360; _ga_QMD5NW261X=GS1.1.1738630325.29.1.1738630376.0.0.0; _ga_JMHFCNW1FS=GS1.1.1738630325.30.1.1738630376.9.0.0; InitechEamUURL=http%3A%2F%2Ftoscn.lgensol.com%3A8009%2Ftos%2Fmain.do; InitechEamRTOA=1; CLP=; InitechEamCookieEnc=T; LGCHEM_NC_FLAG=TRUE; ep_mode=F; InitechEamNCFlag=F; language=ko; change=Y; InitechEamUID=1mL53LU3fyaHIWvC6qPkMg%3D%3D; InitechEamUIP=umxQQq46C0M6UuJdGIkf%2Fw%3D%3D; InitechEamULAT=1742965988; InitechEamUTOA=1; InitechEamUPID=XQDvTAgJ4afUQUwvgr1wJA%3D%3D; InitechEamUHMAC=q9%2BMFjtELSpwb7T7TTuROLpJGNCLwpqbvlPtr4NTt6g%3D; eWLanguage=ko-KR; engpuid=oHMNDv1VP83vQ91gJ4P1xNulp9ocriXZHmThkwuCDXs=; tempLGDPID=hyukjun_jo; InitechEamDomain=LGENSOL; LENA-UID=bcc9c6d8.6316178c7fee8; SCOUTER=z2bd0rldnhe3l8; leftMenuType=list",
                "Host": "toscn.lgensol.com:8009",
                "Origin": "http://toscn.lgensol.com:8009",
                "Referer""": "http://toscn.lgensol.com:8009/tos/test/resultCdms/retrieveTestResultFileList.pop",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
        }

    def _filter_to_payload(self, filter):
        print(filter)
        return {
            'searchTestTitle': filter['test_name'],
            'searchUserName': filter['requester_name'],
            'searchWorkplaceCd' : filter['site'],
        }

    def _get_res_kr(self, payload):
        return requests.post(self.BASE_URL.format('kr', 'retrieveTestResultList'), 
                    headers=self.header_kr, data=payload, timeout=30)
    
    def _get_res_cn(self, payload):
        return requests.post(self.BASE_URL.format('cn', 'retrieveTestResultList'),
                    headers=self.header_cn, data=payload, timeout=10)
    
    def _get_res_data(self, site, payload):
        res = site(payload)
        res.raise_for_status()
        return res.json()

    def _get_test_list(self, res_site, payload):

        test_list = []

        current_page = 1

        while True:
            print("Current Page: ", current_page)
            payload['page'] = current_page
            '''
            Response from KR or CN
            '''
            res_json = self._get_res_data(res_site, payload)
            total_pages = res_json.get('total', 1)
            test_list.extend(res_json.get('rows', []))

            if current_page >= total_pages:
                break
            
            if current_page == 2:
                break

            current_page += 1

        return test_list

    def _get_test_result_path_kr(self, payload):
        return requests.post(self.BASE_URL.format('kr', 'retrieveTestResultFileList'),
                    headers=self.header_cn, data=payload, timeout=10)
    
    def _get_test_result_path_cn(self, payload):
        return requests.post(self.BASE_URL.format('cn', 'retrieveTestResultFileList'),
                    headers=self.header_cn, data=payload, timeout=10)

    def _define_valid_list(self, user, test_list):
        list_to_web = []
        for row in test_list:
            print('-------------------------------------------------------------------------------------------------------')
            print(row)
            print('-------------------------------------------------------------------------------------------------------')
            '''
            서버 요청 시 튕김 방지를 위한 Random 시간 Delay
            '''
            time.sleep(random.uniform(0.8, 1.2))
            if 'testFileUploadReqStateCd' not in row.keys():
                continue
            res = self._get_test_result_path_kr({'testId' : row['testId']})
            res.raise_for_status()
            json_file = res.json()
            for json_row in json_file['rows']:
                if json_row['fileFullPath'].endswith('cyc'): 
                    row['FileFullPath_cyc'] = json_row['fileFullPath']
                if json_row['fileFullPath'].endswith('cts'):
                    row['FileFullPath_cts'] = json_row['fileFullPath']
            '''
            analyzerUrl은 FileFullPath에 들어가있기 때문에 삭제
            '''
            
            row = {'user': user, 'testTitle': row['testTitle'], 'requester': row['userName'], 'requester_team':row['deptNm'].replace('ESS.개발.Cell.',''), 'testId': row['testId'], 'site': row['workplaceNm'], 'status': row['testStateNm'], 'testDate': row['firstChnnStartDatetime'], 'fileFullPath_cyc': row['FileFullPath_cyc'], 'fileFullPath_cts': row['FileFullPath_cts']}
            list_to_web.append(row)

        return list_to_web

    def search_tos_data(self, filter):
        '''
        서버 튕김 방지용 Delay
        '''
        time.sleep(random.uniform(0.8, 1.2))
        '''
        TOS검색용 payload 설정
        '''
        payload = self._filter_to_payload(filter)

        '''
        Filtering 된 결과 추출을 통해 DB에 추가 할 양식에 맞춰 valid list 추출
        시험을 아직 업로드하지 않은 항목의 경우 여기서 걸러짐
        toskr과 toscn각각 데이터 뽑아낸 후에 병합해서 return
        '''
        print(self._get_test_list(self._get_res_kr, payload))
        temp_list_kr = self._define_valid_list(filter['user'], self._get_test_list(self._get_res_kr, payload))
        temp_list_cn = self._define_valid_list(filter['user'], self._get_test_list(self._get_res_cn, payload))

        return temp_list_kr + temp_list_cn

    def download_example(self, json_file):
        '''
        아직 작업 전 (혁준님 코드 복붙해놨음)
        어떻게 다운록드할지 로직 짜고 수정하던지 그냥 쓰던지.
        '''

        for row in json_file['rows']:
            download_url = row["fileFullPath"] # full path 추출
            with requests.get(download_url, stream=True) as res:    
                if res.status_code == 200:
                    with open(os.path.join("./outputs", download_url.split("/")[-1]), 'wb') as f:
                        for chunk in res.iter_content(chunk_size=8192): 
                            f.write(chunk)
                    print(f"Downloaded {download_url} successfully.")
                else:
                    print(f"Failed to download {download_url}. Status code: {res.status_code}")    



if __name__ == "__main__":
    filter = {
        # 'user': '조혁준',
        # # 'cell_id' : 'DA05B1S810',
        'test_name' : 'DE12ED0179', #no full path
        # # 'cell_id' : '',
        # #'requester_name' : '강서희',
        # 'requester_name' : '',
        # 'site' : '전체',
        # # 'test_request_id' : 'REQ-P-2025-027698',
        # 'test_request_id' : '',
    }

    serachtosdata = TosConnector()
    list_to_web = serachtosdata.search_tos_data(filter)
    print(list_to_web)




