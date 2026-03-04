import struct 
import pandas as pd
import os
import re

class AproProcessor():
    def __init__(self, config):
        self.col_dict = \
            {'nStatus':'StepType',
            'nCode':'Code',
            'nFault':'Fault', 
            'nCycle':'TotalCycle', 
            'nStep':'StepNo', 
            'nTemp':'Temp',
            'nCur':'Current', 
            'nVol':'Voltage',
            'nCap':'Capacity', 
            'nWatt':'Power', 
            'nWH':'WattHour', 
            'nTime':'TotalTime_sec', 
            'index':'DataSeq', 
            'nCurCycle':'Cyclenum', 
            'nDCIR':'Impedance',
            'cum_cap':'IntegralCapacity', 
            'C_cap':'ChargeCapacity', 
            'D_cap':'DischargeCapacity', 
            'cum_work':'IntergralWattHour', 
            'C_work':'ChargeWattHour', 
            'D_work':'DischargeWattHour',
            'nAvgVol':'AvgVoltage', 
            'nAvgCur':'AvgCurrent', 
            'nCVMode':'CVMode', 
            'CVRunTime':'CVEndTime', #맞나?? 
            'CVCapacity':'ChargeCVCap',
            'nWorkingTime':'WorkingTime'}
        self.config = config
        
    def _parse_year(self, file_path):
        file_path = os.path.basename(file_path)
        # 정규표현식: 언더바 사이의 숫자 6~8자리를 찾고 연도 부분만 추출
        pattern = re.compile(r'_(\d{2,4})\d{4}_')

        match = pattern.search(file_path)
        if match:
            year_full = match.group(1) # '24' 또는 '2024'
            year_2digit = year_full[-2:] # 뒤에서 두 글자만 슬라이싱
            return int(year_2digit)    
            
    def _read_record_data(self, file_path):
        record_list = []
        year = self._parse_year(file_path)
        format_string_1010 = ( # file_format_1010
            "@" # for little-endian byte order
            "BBB"  # nStatus, nCode, nFault (BYTE 3개)
            "hhh"   # nCycle, nStep, nTemp (short 3개)
            "iiiii"  # nCur, nVol, nCap, nWatt, nWH (int 5개)
            "II"   # nTime, index (UINT 2개)
            "h"    # nCurCycle (short 1개)
            "iiiiiiiii"    # nDCIR, cum_cap ,C_cap, D_cap, cum_work, C_work, D_work, nAvgVol, nAvgCur (int 9개)
            "B" # nCVMode (unsigned char 1개)
            "III" # CVRunTime, CVCapacity, nWorkingTime (UNIT 3개)
        )
        
        format_string_0912 = ( # file_format_0912
            "@" # for little-endian byte order
            "BBB"  # nStatus, nCode, nFault (BYTE 3개)
            "hhh"   # nCycle, nStep, nTemp (short 3개)
            "iiiii"  # nCur, nVol, nCap, nWatt, nWH (int 5개)
            "II"   # nTime, index (UINT 2개)
            "h"    # nCurCycle (short 1개)
            "iiiiiiiii"    # nDCIR, cum_cap ,C_cap, D_cap, cum_work, C_work, D_work, nAvgVol, nAvgCur (int 9개)
            "B" # nCVMode (unsigned char 1개)
            "II" # CVCapacity, nWorkingTime (UNIT 3개)
        )
        
        format_string_0932 = ( # file_format_0912
            "@" # for little-endian byte order
            "BBB"  # nStatus, nCode, nFault (BYTE 3개)
            "hhh"   # nCycle, nStep, nTemp (short 3개)
            "iiiii"  # nCur, nVol, nCap, nWatt, nWH (int 5개)
            "II"   # nTime, index (UINT 2개)
            "h"    # nCurCycle (short 1개)
            "iiiiiiiii"    # nDCIR, cum_cap ,C_cap, D_cap, cum_work, C_work, D_work, nAvgVol, nAvgCur (int 9개)
        )
        # 포맷의 크기 계산
        
            
        with open(file_path, 'rb') as file:
            file_size = os.fstat(file.fileno()).st_size
            if year != None:
                if year == 22 and file_size % 80 == 0:
                    format_string = format_string_0932
                    print("0932")
                elif year == 23 and file_size % 92 == 0:
                    format_string = format_string_0912
                    print("0912")
                elif year >= 24 and file_size % 96 == 0:   
                    format_string = format_string_1010
                    print("1010")   
                else:
                    if file_size % 80 == 0:
                        format_string = format_string_0932
                        print("0932")
                    elif file_size % 92 == 0:
                        format_string = format_string_0912
                        print("0912")
                    elif file_size % 96 == 0:   
                        format_string = format_string_1010
                        print("1010")     
            else:
                if file_size % 96 == 0:
                    format_string = format_string_1010
                    print("1010")
                elif file_size % 92 == 0:
                    format_string = format_string_0912
                    print("0912")
                elif file_size % 80 == 0:   
                    format_string = format_string_0932
                    print("0932")   
                else:
                    format_string = format_string_1010
                    print("1010")
            
            record_size = struct.calcsize(format_string)
            
            while True:
                data = file.read(record_size)
                if not data:
                    break
                record = struct.unpack(format_string, data)
                record_list.append(record)
                
        return record_list

    def _convert_binary_to_df(self, record_list):
        if len(record_list) == 0:
            return pd.DataFrame([], columns=[])
        
        if len(record_list[0]) == 27:
            col = ['nStatus','nCode', 'nFault', 'nCycle', 'nStep', 'nTemp', 'nCur', 'nVol',
            'nCap', 'nWatt', 'nWH', 'nTime', 'index', 'nCurCycle', 'nDCIR',
            'cum_cap', 'C_cap', 'D_cap', 'cum_work', 'C_work', 'D_work',
            'nAvgVol', 'nAvgCur', 'nCVMode', 'CVRunTime', 'CVCapacity','nWorkingTime']
        elif len(record_list[0]) == 26:
            col = ['nStatus','nCode', 'nFault', 'nCycle', 'nStep', 'nTemp', 'nCur', 'nVol',
            'nCap', 'nWatt', 'nWH', 'nTime', 'index', 'nCurCycle', 'nDCIR',
            'cum_cap', 'C_cap', 'D_cap', 'cum_work', 'C_work', 'D_work',
            'nAvgVol', 'nAvgCur', 'nCVMode', 'CVCapacity','nWorkingTime']
        elif len(record_list[0]) == 23:
            col = ['nStatus','nCode', 'nFault', 'nCycle', 'nStep', 'nTemp', 'nCur', 'nVol',
            'nCap', 'nWatt', 'nWH', 'nTime', 'index', 'nCurCycle', 'nDCIR',
            'cum_cap', 'C_cap', 'D_cap', 'cum_work', 'C_work', 'D_work',
            'nAvgVol', 'nAvgCur']
        raw_data = pd.DataFrame(record_list, columns=col)
        
        if len(record_list[0]) == 26 or len(record_list[0]) == 23:
            raw_data['CVRunTime'] = pd.NA
            
        return raw_data
    
    def _del_abnormal(self, df):
        # ncode 10이면서 마지막행 아닌것 제거하기
        drop_idx = df[df['nCode'] == 10].index[:-1]
        df = df.drop(drop_idx)
        return df

    def _convert_time(self, value):
        return pd.to_datetime(value * 10**6).dt.time

    def _convert_ncode(self, value):
        ncode_dict = {0:'Normal', 1:"Time Complete", 2:"Voltage Complete", 3:"Current Complete", 4:"Capacity Complete", 10:"Stopped by User"}
        return ncode_dict.get(value, "None")
    
    def _convert_type(self, value):
        ncode_dict = {2:'Charge', 4:'Charge', 5:'Charge', 8:'Discharge', 11:'Discharge', 17:'Pattern', 28:'Rest'} 
        return ncode_dict.get(value, "None")
    
    def _convert_nTime(self, df):
        # "공정 시간"을 초 단위로 timedelta 변환 (100으로 나눠서 초로 변환)
        df['TotalTime_diff'] = df['TotalTime_sec'].diff().fillna(0)
        df['TotalTime'] = pd.to_timedelta(df['TotalTime_sec'], unit='s')
        # 누적 시간 → "D:HH:MM:SS" 변환
        df['TotalTime'] = df['TotalTime'].apply(lambda x: f"{x.days}:{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")
        # 개별 공정 시간 계산 (현재 행 - 이전 행, 첫 번째 행은 그대로 사용)
        df['StepTime_sec'] = df.groupby('group_id')['TotalTime_diff'].cumsum().astype('float')
        df['StepTime']  = pd.to_timedelta(df['StepTime_sec'] , unit='s')
        # 개별 공정 시간 → "HH:MM:SS" 변환
        df['StepTime'] = df['StepTime'].apply(lambda x: f"{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")
        return df
    
    def _rename_column(self, df):
        df = df.rename(columns=self.col_dict) 
        return df

    def _apply_fraction(self, df):
        df['nTemp'] = df['nTemp'] * 0.1
        df['nTime'] = df['nTime'] * 0.01
        df[['nCur', 'nVol', 'nAvgVol', 'nAvgCur']] = df[['nCur', 'nVol', 'nAvgVol', 'nAvgCur']] * 0.0001
        df[['nCap','nWatt','nWH','nDCIR','cum_cap','C_cap','D_cap','cum_work','C_work','D_work']] = df[['nCap','nWatt','nWH','nDCIR','cum_cap','C_cap','D_cap','cum_work','C_work','D_work']]*0.001
        return df

    def _preprocess(self, df):
        df = self._del_abnormal(df)
        df = self._apply_fraction(df)
        df = df.rename(columns=self.col_dict) 
        
        df['Code'] = df['Code'].apply(self._convert_ncode)
        df['StepType'] = df['StepType'].apply(self._convert_type)
        df['group_id'] = (df['StepType'] != df['StepType'].shift(1)).cumsum()
        df = self._convert_nTime(df)
        
        stepend_df = df.query("Code!='Normal'").reset_index(drop=True)
        return df, stepend_df
    
    def parse_binary_file(self, file_path):
        record_data = self._read_record_data(file_path)
        raw_data = self._convert_binary_to_df(record_data)
        processed_data, stepend_data = self._preprocess(raw_data)
        
        return {
            'RawData': raw_data,
            'ProcessedData' : processed_data,
            'StepEndData' : stepend_data
            }