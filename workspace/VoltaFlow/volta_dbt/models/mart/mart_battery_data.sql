{{config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['cell_id', 'exp_id'],
    database='postgres_db',
    indexes=[
        {'columns': ['cell_id']},
        {'columns': ['exp_id']},
    ] 
)}}
-- # posthook으로 lastdb_insert_datetime 업데이트하기.
-- 증분로직 이렇게 짰는데, 생각해보니까 pipeline_queue에서 이미 완료된 애들은 걸러지고, cell_id, exp_id로 delete+insert할거면 굳이 증분으로 할 필요가 없을 수도 있겠다는 생각이 드네. 
-- 어차피 pipeline_queue에서 걸러지는 애들은 이미 완료된 애들이니까, 그냥 전체 데이터를 매번 새로 넣어도 될 것 같음.
-- 이렇게 생각했었는데, 생각해보니까 table로 바꾸면 전체가 다 지워지고, download_required_yn이 True인 애들만 업데이트 되는거라 내가 생각한거는 incremental이 맞겠다.

WITH queue_tb AS(
        SELECT exp_id
        FROM {{source('pg_db', 'pipeline_queue_tb')}}
        WHERE download_required_yn = TRUE
    ),
    raw_data AS(    
        SELECT serial_id,
                cell_id,
                exp_id,
                StepNo as stepno,
                StepType as steptype, 
                Code as code,
                TotalCycle as totalcycle,
                Voltage as voltage,
                Current as current,
                Capacity as capacity,
                Power as power,
                WattHour as watthour, 
                StepTime as steptime,
                TotalTime as totaltime,
                Impedance as impedance,
                Temp as temp,
                AvgVoltage as avgvoltage,
                AvgCurrent as avgcurrent,
                ChargeCapacity as chargecapacity,
                DischargeCapacity as dischargecapacity,
                ChargeWattHour as chargewatthour,
                DischargeWattHour as dischargewatthour,
                CvEndTime as cvendtime,
                TestMode as testmode,
                IsRptCapa as isrptcapa,
                IsRptDcir as isrptdcir
                
        FROM {{ref('stg_battery_data')}}
        {% if is_incremental() %}
        WHERE exp_id IN (SELECT exp_id FROM queue_tb)
        {% endif %}
    )

SELECT *
FROM raw_data
