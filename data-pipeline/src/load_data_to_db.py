import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config import DB_CONFIG

# Схема и таблица назначения для сырых данных
SCHEMA = "s_sql_dds"
TABLE  = "t_sql_source_unstructured"

# DDL создаётся здесь же, чтобы функция была самодостаточной:
# при первом запуске схема и таблица создадутся автоматически.
DDL = f"""
create schema if not exists {SCHEMA};

create table if not exists {SCHEMA}.{TABLE} (
    student_name    varchar,
    group_name      varchar,
    subject         varchar,
    teacher         varchar,
    control_type    varchar,
    exam_date       varchar,
    retake_date     varchar,
    grade           varchar,
    attendance_pct  varchar,
    hours_studied   varchar
);
"""


def load_data_to_db(df: pd.DataFrame) -> None:
    """
    Загружает сырой DataFrame в PostgreSQL-таблицу t_sql_source_unstructured
    «как есть» — все поля в формате VARCHAR.

    Перед загрузкой таблица очищается (TRUNCATE), чтобы повторные запуски
    не создавали дублей на уровне БД.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # Создаём схему и таблицу, если ещё не существуют
                cur.execute(DDL)

                # Очищаем таблицу перед загрузкой (идемпотентность)
                cur.execute(f"truncate table {SCHEMA}.{TABLE};")

                # Массовая вставка через execute_values (быстрее чем построчно)
                columns = list(df.columns)
                rows    = [tuple(row) for row in df.itertuples(index=False, name=None)]
                sql = f"""
                    insert into {SCHEMA}.{TABLE}
                        ({', '.join(columns)})
                    values %s
                """
                execute_values(cur, sql, rows)

        print(f"[load_data_to_db] Загружено {len(df)} строк в {SCHEMA}.{TABLE}.")
    finally:
        # Закрываем соединение в любом случае — даже при ошибке
        conn.close()
