{{config(
    materialized='table',
    database='postgres_db'
)}}

WITH raw_data AS (
    SELECT 
        COLUMNS(c -> c NOT ILIKE '%Reserved%'), -- 'Reserved'가 포함되지 않은(NOT ILIKE) 모든 컬럼을 선택
        split_part(filename, '/', 4) as cell_id,
        split_part(filename, '/', 5) as exp_id,
        row_number() over (partition by filename order by (SELECT NULL)) as serial_id
    FROM read_parquet(
        {{source('minio_source', 'step_end')}},
        union_by_name=true,
        filename=true
    )
)

SELECT * FROM raw_data