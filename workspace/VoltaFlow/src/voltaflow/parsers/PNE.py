import struct 
from .PNE_BASE import BaseProcessor
import pandas as pd
import sys

# sys.path에 상위 폴더(..)를 추가합니다.
sys.path.append("..")

class CycProcessor(BaseProcessor):
    def _parse_data_header(self, file):
        # Read and parse PS_RECORD_FILE_HEADER
        nColumCount = struct.unpack('@i', file.read(4))[0]
        awColumnItem = struct.unpack(f'@{self.config.CYC_ColumnItem[self.format_version]}H', file.read(self.config.CYC_ColumnItem[self.format_version] * 2))  # WORD is 2 bytes, assuming 42 columns

        column_names = [self.column_mapping.get(item, f'UNKNOWN_0x{item:02X}') for item in awColumnItem[:nColumCount]]
        
        awColumnItem_dict = dict(zip(column_names, awColumnItem[:nColumCount]))

        return {
            'nColumCount': nColumCount,
            'awColumnItem': awColumnItem_dict,
            'columnNames': column_names
        }    
        
    def _preprocess(self, data: list, column_names: list)-> pd.DataFrame:
        filtered_data = [record for record in data]

        # Convert filtered data to DataFrame
        df = pd.DataFrame(filtered_data, columns=column_names)

        # Divide PS_VOLTAGE, PS_CAPACITY, and PS_CURRENT by 1000 if they exist in the DataFrame
        for column in ['PS_VOLTAGE', 'PS_CAPACITY', 'PS_CURRENT', 'PS_WATT', 'PS_WATT_HOUR']:
            if column in df.columns:
                df[column] = df[column] / 1000

        # Set PS_WATT to negative if PS_CURRENT is negative
        if 'PS_CURRENT' in df.columns and 'PS_WATT' in df.columns:
            df.loc[df['PS_CURRENT'] < 0, 'PS_WATT'] = df['PS_WATT'].abs() * -1

        # Drop specific columns: PS_REALDATE, PS_REALCLOCK, PS_STATE if they exist
        columns_to_drop = ['PS_REALDATE', 'PS_REALCLOCK', 'PS_STATE']
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

        # step end dataframe은 cts에서 가져올 것이라 일단 주석
        # Extract rows where PS_STEP_TIME resets to '00:00:00.0000'
        # if 'PS_STEP_TIME' in df.columns:
        #     # Identify rows where PS_STEP_TIME is less than the previous row, indicating a reset
        #     step_end_df = df[df['PS_STEP_TIME'].astype('timedelta64[s]').diff().shift(-1) < pd.Timedelta(0)]

        # if 'PS_STEP_TIME' in step_end_df.columns:
        #     step_end_df['PS_STEP_TIME'] = pd.to_datetime(step_end_df['PS_STEP_TIME'], unit='s').dt.strftime('%H:%M:%S.%f').str[:-4]
        return df
    
    def _read_record_data(self, file, nColumCount):
        # Read the record data
        data = []
        while True:
            record = file.read(nColumCount * 4)  # float is 4 bytes
            if not record:
                break
            awColumnItem = struct.unpack(f'@{nColumCount}f', record)
            data.append(awColumnItem)

        return data

    def parse_binary_file(self, file_path):
        with open(file_path, 'rb') as file:
            # Read headers
            file_id_header = self._parse_id_header(file)
            self.format_version = str(file_id_header['szFileVersion'])
            record_file_header = self._parse_data_header(file)

            # Read record data
            nColumCount = record_file_header['nColumCount']
            record_data = self._read_record_data(file, nColumCount)
            processed_data = self._preprocess(record_data, record_file_header['columnNames'])

            return {
                'FileIDHeader': file_id_header,
                'RecordFileHeader': record_file_header,
                'ProcessedData' : processed_data
            }
    
class CtsProcessor(BaseProcessor):
    def __init__(self, config):
        super().__init__(config)
        self.col_dict = {
                'ChCode': 'Code',
                'Temperature': 'Temp',
                'Watt': 'Power',
                'TotalCycleNum': 'TotalCycle'
            }
        self.format_version = None
        self.chCodeDict = config.chCodeDict
        
    def _parse_data_header(self, file):
        # Read PS_TEST_FILE_HEADER
        record_bytes = int(self.config.RECORDBYTE[self.format_version])  # Get the record byte size for the given version
        szStartTime = file.read(64).decode('utf-8',errors='replace').strip('\x00')
        szEndTime = file.read(64).decode('utf-8',errors='replace').strip('\x00')
        szSerial = file.read(64).decode('utf-8',errors='replace').strip('\x00')
        szUserID = file.read(32).decode('utf-8',errors='replace').strip('\x00')
        szDescript = file.read(128).decode('utf-8',errors='replace').strip('\x00')
        szTrayNo = file.read(64).decode('utf-8',errors='replace').strip('\x00')
        szBuff = file.read(64).decode('utf-8',errors='replace').strip('\x00')
        
        nRecordSize = struct.unpack('@i', file.read(4))[0]
        wRecordItem = struct.unpack(f'@{record_bytes}H', file.read(record_bytes * 2))
        # 파일 구조에 2byte 만큼이 누락된건진 모르겠는데,,, 49임 원래는,,, 50으로 읽고 가니까 180byte로 딱 나누어 떨어짐!!
        column_names = [self.column_mapping.get(item, f'UNKNOWN_0x{item:02X}') for item in wRecordItem[:nRecordSize]]
        awColumnItem_dict = dict(zip(column_names, wRecordItem[:nRecordSize]))

        return {
            'szStartTime': [szStartTime],
            'szEndTime': [szEndTime],
            'szSerial': [szSerial],
            'szUserID': [szUserID],
            'szDescript': [szDescript],
            'szTrayNo': [szTrayNo],
            'szBuff': [szBuff],
            'nRecordSize': [nRecordSize],
            'awColumnItem': awColumnItem_dict,
            'columnNames': column_names
        }   
        
    def _convert_nTime(self, df):
        # "공정 시간"을 초 단위로 timedelta 변환 (100으로 나눠서 초로 변환)
        df['cumtime_td'] = pd.to_timedelta(df['TotalTime'], unit='s')
        # 누적 시간 → "D:HH:MM:SS" 변환
        df['TotalTime'] = df['cumtime_td'].apply(lambda x: f"{x.days}:{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")
        # 개별 공정 시간 계산 (현재 행 - 이전 행, 첫 번째 행은 그대로 사용)
        df['time'] = df['cumtime_td'].diff().fillna(df['cumtime_td'])
        # 개별 공정 시간 → "HH:MM:SS" 변환
        df['StepTime'] = df['time'].apply(lambda x: f"{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")
        # 필요 없는 timedelta 컬럼 삭제
        df = df.drop(columns=['cumtime_td', 'time'])
        
        return df
    
    def _read_record_data(self, file) -> list:
        # Read the record data
        data = []
        format_string = "@" + self.config.FORMAT[self.format_version]
        record_size = struct.calcsize(format_string)
        
        while True:
            record = file.read(record_size)  # 180바이트 읽기
            if not record:
                break
            awColumnItem = struct.unpack(format_string, record)
            data.append(awColumnItem)
        
        return data    
    
    def _preprocess(self, data):
        filtered_data = [record for record in data]
        # Convert filtered data to DataFrame
        df = pd.DataFrame(filtered_data, columns=self.config.COLUMNS[self.format_version])
        # Divide PS_VOLTAGE, PS_CAPACITY, and PS_CURRENT by 1000 if they exist in the DataFrame
        for column in ['Voltage', 'Capacity', 'Current', 'Watt', 'WattHour','ChargeCapacity', 'DischargeCapacity', 'AvgVoltage', 'AvgCurrent']:
            if column in df.columns:
                df[column] = df[column] / 1000

        # Set PS_WATT to negative if PS_CURRENT is negative
        if 'Current' in df.columns and 'Watt' in df.columns:
            df.loc[df['Current'] < 0, 'Watt'] = df['Watt'].abs() * -1
        df = self._convert_nTime(df) # 시간 변환
        df['StepType'] = df['StepType'].replace({0:"None",1:"Charge", 2:"Discharge", 3:"Rest",4:"OCV",5:"IMPEDANCE", 6:"Completed", 7:"ADV_Cycle", 8:"Loop", 9:"Pattern", 10:"Balance", 11:"USERMAP"})
        df['Mode'] = df['Mode'].replace({0:"None", 1:"CCCV", 2:"CC", 3:"CV", 4:"DCIMP", 5:"ACIMP", 6:"CP", 7:"PUSE", 8:"CR"})
        df['ChCode'] = df['ChCode'].map(lambda x: self.chCodeDict.get(str(x)))
        df = df.rename(columns=self.col_dict)
        return df

    def parse_binary_file(self, file_path):
        with open(file_path, 'rb') as file:
            # Read headers
            file_id_header = self._parse_id_header(file)
            format_version = str(file_id_header['szFileVersion'])
            if format_version not in self.config.FORMAT:
                raise ValueError(f"Unsupported format version: {format_version}")
            if format_version == '4101' and str(file_id_header['szFileID']) == '20200701': # 4101인데 Gas로 해야 맞음... 헤더에는 Gas를 식별할 수 있는 게 안붙어있어서 걍 이렇게 가정.
                format_version = '4101_Gas'
            self.format_version = format_version
            record_file_header = self._parse_data_header(file)
            # # Read record data
            # nColumCount = record_file_header['nColumCount']
            record_data = self._read_record_data(file)
            processed_data = self._preprocess(record_data)
            
            return {
                'FileIDHeader': file_id_header,
                'RecordFileHeader': record_file_header,
                'StepEndData' : processed_data
            }