import psycopg2
from config import DB_CONFIG, ETL_START_DATE, ETL_END_DATE


def fill_dm_table(
    start_dt: str = ETL_START_DATE,
    end_dt:   str = ETL_END_DATE,
) -> None:
    """
    Calls s_sql_dds.fn_dm_data_load(start_dt, end_dt) which:
    - fills dimension tables from t_sql_source_structured
    - fills t_dm_task by joining structured table with dimensions
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select s_sql_dds.fn_dm_data_load(%s::date, %s::date);",
                    (start_dt, end_dt),
                )
        print(f"[fill_dm_table] fn_dm_data_load выполнена за период {start_dt} → {end_dt}.")
    finally:
        conn.close()