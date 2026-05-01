import psycopg2
from config import DB_CONFIG, ETL_START_DATE, ETL_END_DATE


def fill_structured_table(
    start_date: str = ETL_START_DATE,
    end_date:   str = ETL_END_DATE,
) -> None:
    """
    Вызывает PostgreSQL-функцию s_sql_dds.fn_etl_data_load(start_date, end_date),
    которая очищает данные из неструктурированной таблицы и заполняет
    структурированную таблицу t_sql_source_structured.

    Вся логика трансформации реализована на стороне SQL — здесь только вызов.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # Вызываем SQL-функцию с передачей временного диапазона
                cur.execute(
                    "select s_sql_dds.fn_etl_data_load(%s::date, %s::date);",
                    (start_date, end_date),
                )
        print(
            f"[fill_structured_table] fn_etl_data_load выполнена "
            f"за период {start_date} → {end_date}."
        )
    finally:
        conn.close()
