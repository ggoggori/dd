from datetime import datetime
import numpy as np
import pandas as pd
import xlwings as xw
import os
import sys

def get_resource_path(relative_path):
    """번들된 자원의 절대 경로를 얻습니다."""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

def read_template():
    template_path = get_resource_path(os.path.join("assets", "template.xlsx"))
    app = xw.App(visible=False)  # 엑셀 창이 안 보이도록 설정
    wb = app.books.open(template_path)  # app을 명시적으로 사용
    sheet = wb.sheets["template"]

    now = datetime.now().strftime('%Y-%m-%d')
    sheet.range('A2').value = now
    return app, wb, sheet

def read_data(data_path):
    try:
        df = pd.read_csv(data_path, encoding="cp949")
    except UnicodeDecodeError:
        df = pd.read_csv(data_path, encoding="utf-8")
        
    return df

def insert_data(df, sheet, exp_name, ch_location):
    num_cycle = 1
    rpt_cycle, dcir_cycle = [], []
    target_col = ['Voltage', 'Current', 'Capacity', 'WattHour','Temp','Impedance','Rest OCV[V]']
    ch_col = [col + '_ch' for col in target_col]
    dch_col = [col + '_dch' for col in target_col]
    temp_df = pd.DataFrame(columns=ch_col + dch_col)
    break_flag = False
    
    for idx, row in df.iterrows():
        rest_ocv = None
        cur_type = row['StepType']  
        try:
            next_row = df.iloc[idx+1, [df.columns.get_loc('StepType'), df.columns.get_loc('Voltage')]] 
        except IndexError:
            break_flag = True
        
        if cur_type == 'Discharge' or cur_type == 'Charge' :
            temp = row[['Voltage', 'Current', 'Capacity', 'WattHour','Temp','Impedance']]
            
            if next_row['StepType'] == 'Rest':
                rest_ocv = next_row['Voltage']

            if cur_type =='Charge':
                num_cycle += 1
                temp_df.loc[num_cycle,['Voltage_ch', 'Current_ch', 'Capacity_ch', 'WattHour_ch','Temp_ch','Impedance_ch']] = temp.values
                temp_df.loc[num_cycle,['Rest OCV[V]_ch']] = rest_ocv
                
            if cur_type =='Discharge':
                temp_df.loc[num_cycle,['Voltage_dch', 'Current_dch', 'Capacity_dch', 'WattHour_dch','Temp_dch','Impedance_dch']] = temp.values
                temp_df.loc[num_cycle,['Rest OCV[V]_dch']] = rest_ocv
                
                try:
                    if df.iloc[idx+2, df.columns.get_loc('StepType')]  == 'Discharge':
                        num_cycle += 1
                except IndexError:
                    pass
        if row['IsRptCapa'] == True:
            rpt_cycle.append(num_cycle)
        if row['IsRptDcir'] == True:
            dcir_cycle.append(num_cycle)
            
        if break_flag == True:
            break
        
    sheet.range(f'A1').value = exp_name      
    sheet.range(f'C1').value = ch_location   
    sheet.range(f'B4').value = temp_df.index.values.reshape(-1, 1)  # cycle
    sheet.range(f'L4').value = temp_df['Rest OCV[V]_ch'].values.reshape(-1, 1)  
    sheet.range(f'U4').value = temp_df['Rest OCV[V]_dch'].values.reshape(-1, 1)    
    sheet.range(f'AB4').value = np.array(rpt_cycle).reshape(-1, 1)  # rpt_cycle
    sheet.range(f'AE4').value = np.array(dcir_cycle).reshape(-1, 1)  # rpt_cycle
    sheet.range(f'D4').value = temp_df[ch_col].drop('Rest OCV[V]_ch', axis=1).values
    sheet.range(f'M4').value = temp_df[dch_col].drop('Rest OCV[V]_dch', axis=1).values