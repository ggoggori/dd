# 신규 version 추가 시에 fReseved[5]는 f가 5개 필요하다는 것임을 명심하기!!

CYC_ColumnItem = {
    '4103' : 46,
    '4102' : 42,
    '4101' : 42,
    '4101_Gas' : 42,
    '4100' : 35,
    '4099' : 24
}

FORMAT = {
    '4099':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "f"             # fTemperature2;
    ), 
        
    '4100':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "fffff"         # fChambertemp, fChargeCCCapacity, fChargeCVCapacity, fDischargeCCCapacity, fDischargeCVCapacity
        "HH"            # wChCode, wReserved
        "fffffff"       # fReserved[5]; fRealDate;fRealClock;
    ), 
    
    '4101':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "fffff"         # fChambertemp, fChargeCCCapacity, fChargeCVCapacity, fDischargeCCCapacity, fDischargeCVCapacity
        "HH"            # wChCode, wReserved
        "fffffffff"     # fImpedance100ms, fImpedance1s, fImpedance5s, fImpedance30s, fImpedance60s, fAmbientTemp, fGasVoltage, fRealDate, fRealClock
    ), 
    '4101_Gas':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "fffff"         # fChambertemp, fChargeCCCapacity, fChargeCVCapacity, fDischargeCCCapacity, fDischargeCVCapacity
        "HH"            # wChCode, wReserved
        "ffffffffffffff"# fReserved[5]; fRealDate;fRealClock; fGasCO2; fGasTemp; fGasAH; fGasBaseline; fGasTVOC; fGasEthanol; fGasH2;
    ), 
    '4102':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "fffff"         # fChambertemp, fChargeCCCapacity, fChargeCVCapacity, fDischargeCCCapacity, fDischargeCVCapacity
        "HH"            # wChCode, wReserved
        "fffffffff"     # fImpedance100ms, fImpedance1s, fImpedance5s, fImpedance30s, fImpedance60s, fAmbientTemp, fGasVoltage, fRealDate, fRealClock
    )
    , 
        '4103':
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "ffffffff"      # fIntegralCapacity, fIntegralWattHour, fChargeCapacity, fDischargeCapacity, fChargeWattHour, fDischargeWattHour, fCVEndTime, fFarad
        "cc"            # totalTimeCarry, cReserved2
        "H"             # nCycleNum
        "fffff"         # fChambertemp, fChargeCCCapacity, fChargeCVCapacity, fDischargeCCCapacity, fDischargeCVCapacity
        "HH"            # wChCode, wReserved
        "ffffffffffffff"     # fImpedance100ms, fImpedance1s, fImpedance5s, fImpedance30s, fImpedance60s, fAmbientTemp, fGasVoltage, fRealDate, fRealClock, fChamberTempSV, fChilerTempPV,fChilerTempSV, fChilerPumpPV, fChilerPumpSV
    )
    , 
    #     '4096': # ver 1
    # (
    #     "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
    #     "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
    #     "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
    #     "L"             # ulGotoCycleNum
    #     "f"             # fIntegralCapacity;
    # )
    '4096': # ver 2
    (
        "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
        "LLLLLL"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
        "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
        "L"             # ulGotoCycleNum
        "f"             # fIntegralCapacity;
        "l"             # fIntegralWattHour;
        "LLLLL"         # ulChargeCapacity, ulDischargeCapacity, ulChargeWattHour, ulDischargeWattHour, ulCVEndTime
        "fff"             # fReserved[3];
    )
    
    #     '4096':
    # (
    #     "BBBBBBBB"      # chNo, chStepNo, chState, chStepType, chDataSelect, cReserved, chGradeCode, chMode
    #     "IIIIII"        # ulIndexFrom, ulIndexTo, ulCurrentCycleNum, ulTotalCycleNum, ulSaveSequence, ulReserved
    #     "ffffffffffff"  # fVoltage, fCurrent, fCapacity, fWatt, fWattHour, fStepTime, fTotalTime, fImpedance, fTemperature, fPressure, fAvgVoltage, fAvgCurrent
    #     "I"             # ulGotoCycleNum
    #     "f"             # fIntegralCapacity;
    #     "i"             # fIntegralWattHour;
    #     "IIIII"         # ulChargeCapacity, ulDischargeCapacity, ulChargeWattHour, ulDischargeWattHour, ulCVEndTime
    #     "fff"             # fReserved[3];
    # )
}

RECORDBYTE = {'4100': 42,
            '4101':50, # 파일 구조에 2byte 만큼이 누락된건진 모르겠는데,,, 49임 원래는,,, 50으로 읽고 가니까 180byte로 딱 나누어 떨어짐!!
            '4101_Gas':42,
            '4102':50,
            '4103':50,
            '4096':40, # structure에는 39로 되어있음.
            '4099':42}

COLUMNS = {
    '4099':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved", # <- 원래 Code, Mode를 Code로 써서 바꿈
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "ulReserved",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "reserved;",
    "CycleNum",
    "Temperature2",
    ],
    '4100':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved1",
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "ulReserved",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "Reserved2",
    "CycleNum",
    "Temperature2",
    "ChargeCCCapacity",
    "ChargeCVCapacity",
    "DischargeCCCapacity",
    "DischargeCVCapacity",
    "ChCode",
    "Reserved",
    "Reserved[5]1",
    "Reserved[5]2",
    "Reserved[5]3",
    "Reserved[5]4",
    "Reserved[5]5",
    "RealDate",
    "RealClock",
],
    
    '4101':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved1",
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "Reserved2",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "Reserved3",
    "CycleNum",
    "ChamberTemp",
    "ChargeCCCapacity",
    "ChargeCVCapacity",
    "DischargeCCCapacity",
    "DischargeCVCapacity",
    "ChCode",
    "Reserved4",
    "Impedance100ms",
    "Impedance1s",
    "Impedance5s",
    "Impedance30s",
    "Impedance60s",
    "AmbientTemp",
    "GasVoltage",
    "RealDate",
    "RealClock"
],
    '4101_Gas':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved1",
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "Reserved2",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "Reserved3",
    "CycleNum",
    "ChamberTemp",
    "ChargeCCCapacity",
    "ChargeCVCapacity",
    "DischargeCCCapacity",
    "DischargeCVCapacity",
    "ChCode",
    "Reserved4",
    "Reserved[5]1",
    "Reserved[5]2",
    "Reserved[5]3",
    "Reserved[5]4",
    "Reserved[5]5",
    "RealDate",
    "RealClock",
    "GasCO2",
    "GasTemp",
    "GasAH",
    "GasBaseline",
    "GasTVOC",
    "GasEthanol",
    "GasH2",
],
    '4102':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved1",
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "Reserved2",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "Reserved3",
    "CycleNum",
    "ChamberTemp",
    "ChargeCCCapacity",
    "ChargeCVCapacity",
    "DischargeCCCapacity",
    "DischargeCVCapacity",
    "ChCode",
    "Reserved4",
    "Impedance100ms",
    "Impedance1s",
    "Impedance5s",
    "Impedance30s",
    "Impedance60s",
    "AmbientTemp",
    "GasVoltage",
    "RealDate",
    "RealClock"
], 
     '4103':
    [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Reserved1",
    "GradeCode",
    "Mode",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "Reserved2",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Farad",
    "TotalTimeCarry",
    "Reserved3",
    "CycleNum",
    "ChamberTemp",
    "ChargeCCCapacity",
    "ChargeCVCapacity",
    "DischargeCCCapacity",
    "DischargeCVCapacity",
    "ChCode",
    "Reserved4",
    "Impedance100ms",
    "Impedance1s",
    "Impedance5s",
    "Impedance30s",
    "Impedance60s",
    "AmbientTemp",
    "GasVoltage",
    "RealDate",
    "RealClock",
    "fChamberTempSV",
    "fChilerTempPV",
    "fChilerTempSV",
    "fChilerPumpPV",
    "fChilerPumpSV",
], 
    '4096':
     [
    "No",
    "StepNo",
    "State",
    "StepType",
    "DataSelect",
    "Mode", #원래 Code인데,,, 없어서 바꿔봄
    "GradeCode",
    "Reserved",
    "IndexFrom",
    "IndexTo",
    "CurrentCycleNum",
    "TotalCycleNum",
    "SaveSequence",
    "Reserved",
    "Voltage",
    "Current",
    "Capacity",
    "Watt",
    "WattHour",
    "StepTime",
    "TotalTime",
    "Impedance",
    "Temperature",
    "Pressure",
    "AvgVoltage",
    "AvgCurrent",
    "GotoCycleNum",
    "IntegralCapacity",
    "IntegralWattHour",
    "ChargeCapacity",
    "DischargeCapacity",
    "ChargeWattHour",
    "DischargeWattHour",
    "CVEndTime",
    "Reserved[3]",
    "Reserved[3]1",
    "Reserved[3]2",
]}