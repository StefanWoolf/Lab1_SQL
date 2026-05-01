import math
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config import DB_CONFIG

SCHEMA = "s_sql_dds"
TABLE  = "t_sql_source_unstructured"


def _sanitize_row(row: tuple) -> tuple:
    """
    Replaces float('nan') with None so psycopg2 inserts NULL instead of 'NaN'.
    pandas stores missing values in object columns as float nan, not None.
    """
    return tuple(None if (isinstance(v, float) and math.isnan(v)) else v for v in row)


def load_data_to_db(df: pd.DataFrame) -> None:
    """
    Loads a raw DataFrame into the PostgreSQL table t_sql_source_unstructured
    as-is — all fields as VARCHAR.

    DDL for the schema and table lives in sql/dds/s_sql_dds/table/.
    The table must exist before running this function.

    The table is truncated before each load to ensure idempotency.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"truncate table {SCHEMA}.{TABLE};")

                columns = list(df.columns)
                rows = [_sanitize_row(row) for row in df.itertuples(index=False, name=None)]

                sql = f"""
                    insert into {SCHEMA}.{TABLE}
                        ({', '.join(columns)})
                    values %s
                """
                execute_values(cur, sql, rows)

        print(f"[load_data_to_db] loaded {len(df)} rows into {SCHEMA}.{TABLE}.")
    finally:
        conn.close()