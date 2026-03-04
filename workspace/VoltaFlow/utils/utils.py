import pathlib
import os
import sys
import re
from collections import defaultdict
from email.header import Header
from email.utils import formataddr
from django.core.mail import send_mail
from django.conf import settings

if not settings.configured:
    settings.configure(
        # 필수 설정: 어떤 이메일 백엔드를 사용할 것인지 지정
        # 이메일 전송 테스트 중이라면 'django.core.mail.backends.console.EmailBackend' 사용
        EMAIL_BACKEND='django.core.mail.backends.smtp.EmailBackend', 
        
        # SMTP 서버 설정 (예: GMail, AWS SES 등)
        EMAIL_HOST="spam.lgensol.com",             # 환경 변수에서 가져옵니다.
        EMAIL_PORT=25,  # 포트 번호 (일반적으로 587 또는 465)
        EMAIL_USE_TLS=False,                            # TLS 사용 여부 (일반적으로 True)
    )


def send_email(user_email, subject, message):
    display_name = Header('수명모델링팀', 'utf-8').encode()
    from_email = formataddr((display_name, 'blmt2024@lgensol.com'))
    
    send_mail(
        subject = subject,
        message = message,
        from_email = from_email,
        recipient_list=[user_email],
        fail_silently=False,
    )

def find_files(base_path):
    path = pathlib.Path(base_path)
    return list(path.glob("**/*.dat")) + list(path.glob("**/*.cts")) + list(path.glob("**/*.cyc"))

def extract_id_from_experiment_name(experiment_name: str):
    # 첫 글자가 영어(a-zA-Z)이고, 그 뒤에 영어나 숫자가 9개 이상 연속되는 패턴을 찾습니다.
    # 즉, 전체 ID는 최소 10자 이상입니다.
    match = re.search(r"[a-zA-Z][a-zA-Z0-9]{9,}", experiment_name)
    if match:
        return match.group(0)
    else:
        return "None"
def process_rpt(stepend, cell_type):
    if cell_type == 'JF1':
        criteria_current = 14.5
    elif cell_type == 'JF1R':
        criteria_current = 8.625
    elif cell_type == 'JF2':
        criteria_current = 39.80
    elif cell_type == 'JF2S':
        criteria_current = 38.950
    elif cell_type == 'JF3':
        criteria_current = 43.275
    else:
        criteria_current = None
        
    stepend['TestMode'] = 'In-situ'
    stepend['IsRptCapa'] = False
    stepend['IsRptDcir'] = False
    
    temp = stepend['StepNo'].value_counts()
    rpt_temp = temp[temp < temp.values.mean()].sort_index()

    stepend_rpt = stepend[(stepend['StepNo'].isin(rpt_temp.index))]
    stepend.loc[stepend_rpt.index, 'TestMode'] = 'RPT'

    def cluster_numbers_by_difference(numbers, threshold=10):
        if not numbers:
            return []

        clusters = []
        current_cluster = [numbers[0]]

        for i in range(1, len(numbers)):
            if numbers[i] - numbers[i-1] > threshold:
                clusters.append(current_cluster)
                current_cluster = [numbers[i]]
            else:
                current_cluster.append(numbers[i])

        clusters.append(current_cluster)

        return clusters

    rpt_group = cluster_numbers_by_difference(stepend_rpt.index.tolist())  #rpt 별로 group 을 만들어준다. 그룹 내에서 capa가 제일 큰걸로 rpt 용량인지 판별하기 때문에
    rpt_capa_idx, rpt_dcir_idx = [], []
    
    for mini_group in rpt_group:
        rpt_df = stepend.loc[mini_group]
        rpt_df = rpt_df[(rpt_df['StepType'] == 'Discharge') | (rpt_df['StepType'] == 'Charge')]
        if rpt_df.empty:
            continue
        
        if criteria_current is not None:
            condition = (rpt_df["Current"].abs() >= criteria_current - 2) & \
                    (rpt_df["Current"].abs() <= criteria_current + 2) & (rpt_df['StepType'] == 'Discharge')

            rpt_idx_discharge_df = rpt_df[condition]
            if len(rpt_idx_discharge_df) != 0:
                if len(rpt_idx_discharge_df) >= 2:
                    rpt_idx_discharge = rpt_idx_discharge_df["DischargeCapacity"].idxmax()
                else:
                    rpt_idx_discharge = rpt_idx_discharge_df.index[0]
            else:
                rpt_idx_discharge = rpt_df["DischargeCapacity"].idxmax() 
            
        else:
            rpt_idx_discharge = rpt_df["DischargeCapacity"].idxmax() 

        charge_df = rpt_df.loc[:rpt_idx_discharge].query("StepType == 'Charge'")
        rpt_idx_charge = charge_df.index.max() if not charge_df.empty else None
        
        if rpt_idx_charge is not None:
            rpt_capa_idx.append(rpt_idx_charge)
        if rpt_idx_discharge is not None:
            rpt_capa_idx.append(rpt_idx_discharge)
            
    stepend.loc[rpt_capa_idx, 'IsRptCapa'] = True
    stepend.loc[(stepend["StepType"] == "Discharge") & (stepend["Code"] == "Time Complete"), "IsRptDcir"] = True
    
    return stepend 
