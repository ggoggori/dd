import struct 
from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    def __init__(self, config):
        self.config = config
        self.column_mapping = {
            0x00: 'PS_STATE', 0x01: 'PS_VOLTAGE', 0x02: 'PS_CURRENT', 0x03: 'PS_CAPACITY',
            0x04: 'PS_IMPEDANCE', 0x05: 'PS_CODE', 0x06: 'PS_STEP_TIME', 0x07: 'PS_TOT_TIME',
            0x08: 'PS_GRADE_CODE', 0x09: 'PS_STEP_NO', 0x0A: 'PS_WATT', 0x0B: 'PS_WATT_HOUR',
            0x0C: 'PS_TEMPERATURE', 0x0D: 'PS_PRESSURE', 0x0E: 'PS_STEP_TYPE', 0x0F: 'PS_CUR_CYCLE',
            0x10: 'PS_TOT_CYCLE', 0x11: 'PS_TEST_NAME', 0x12: 'PS_SCHEDULE_NAME', 0x13: 'PS_CHANNEL_NO',
            0x14: 'PS_MODULE_NO', 0x15: 'PS_LOT_NO', 0x16: 'PS_DATA_SEQ', 0x17: 'PS_AVG_CURRENT',
            0x18: 'PS_AVG_VOLTAGE', 0x19: 'PS_CAPACITY_SUM', 0x1A: 'PS_CHARGE_CAP', 0x1B: 'PS_DISCHARGE_CAP',
            0x1C: 'PS_METER_DATA', 0x1D: 'PS_START_TIME', 0x1E: 'PS_END_TIME', 0x1F: 'PS_SHARING_INFO',
            0x20: 'PS_GOTO_COUNT', 0x21: 'PS_WATTHOUR_SUM', 0x22: 'PS_CHAR_WATTHOUR',
            0x23: 'PS_DISCHAR_WATTHOUR', 0x24: 'PS_INTEGRAL_CAPACITY', 0x25: 'PS_INTEGRAL_WATTHOUR',
            0x26: 'PS_CV_END_TIME', 0x27: 'PS_CYCLE_NUM', 0x28: 'PS_TOT_TIME_CARRY',
            0x29: 'PS_FARAD', 0x2A: 'PS_TEMPERATURE2', 0x2B: 'PS_DQDV', 0x2C: 'PS_CHARGE_CC_CAP',
            0x2D: 'PS_CHARGE_CV_CAP', 0x2E: 'PS_DISCHARGE_CC_CAP', 0x2F: 'PS_DISCHARGE_CV_CAP',
            0x30: 'PS_REALDATE', 0x31: 'PS_REALCLOCK', 0x32: 'PS_CHAMBER_TEMPERATURE',
            0x33: 'PS_AUX_TEMPERATURE', 0x34: 'PS_AUX_VOLTAGE', 0x3B: 'PS_AUX_THICKNESS2',
            0x3C: 'PS_AUX_PRESSURE1', 0x3D: 'PS_AUX_PRESSURE2', 0x3E: 'PS_AUX_PRESSURE3',
            0x3F: 'PS_AUX_PRESSURE4', 0x40: 'PS_AUX_THICKNESS1', 0x41: 'PS_GAS_CO2', 0x42: 'PS_GAS_TEMP', 
            0x43: 'PS_GAS_AH',  0x44: 'PS_GAS_BASELINE', 0x45: 'PS_GAS_TVOC', 0x46: 'PS_GAS_ETHANOL', 
            0x47: 'PS_GAS_H2', 0x49: 'PS_AMBIENT_TEMP', 0x4A: 'PS_GAS_VOLTAGE', 0x4B: 'PS_IMPEDANCE_100MS', 
            0x4C: 'PS_IMPEDANCE_1S', 0x4D: 'PS_IMPEDANCE_5S', 0x4E: 'PS_IMPEDANCE_30S', 0x4F: 'PS_IMPEDANCE_60S',
            0x70: 'PS_RESTORED_FLAG', 0x68: 'PS_CHAMBER_TEMP_SV', 0x69: 'PS_CHILER_TEMP_PV',
            0x6A: 'PS_CHILER_TEMP_SV', 0x6B: 'PS_CHILER_PUMP_PV', 0x6C: 'PS_CHILER_PUMP_SV',
            0x6D: 'PS_AUX_THICKNESS3', 0x6E: 'PS_AUX_THICKNESS4'
        }
        
    def _parse_id_header(self, file):
        # Read and parse PS_FILE_ID_HEADER
        szFileID, szFileVersion = struct.unpack('@II', file.read(8))
        szCreateDateTime = file.read(64).decode('utf-8', errors='replace').rstrip('\x00')
        szDescription = file.read(128).decode('utf-8', errors='replace').rstrip('\x00')
        szReserved = file.read(128).decode('utf-8', errors='replace').rstrip('\x00')

        return {
            'szFileID': szFileID,
            'szFileVersion': szFileVersion,
            'szCreateDateTime': szCreateDateTime,
            'szDescription': szDescription,
            'szReserved': szReserved,
        }
    
    @abstractmethod
    def _preprocess(self):
        return 
    
    @abstractmethod
    def parse_binary_file(self):
        return 