import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config import DB_CONFIG

SCHEMA = "s_sql_dds"
TABLE  = "t_sql_source_unstructured"


def load_data_to_db(df: pd.DataFrame) -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # очищаем таблицу перед загрузкой
                cur.execute(f"truncate table {SCHEMA}.{TABLE};")

                # массовая вставка
                columns = list(df.columns)
                rows    = [tuple(row) for row in df.itertuples(index=False, name=None)]
                sql = f"""
                    insert into {SCHEMA}.{TABLE}
                        ({', '.join(columns)})
                    values %s
                """
                execute_values(cur, sql, rows)

        print(f"[load_data_to_db] загружено {len(df)} строк в {SCHEMA}.{TABLE}.")
    finally:
        conn.close()