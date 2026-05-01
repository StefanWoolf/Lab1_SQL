import psycopg2
import mysql.connector
from config import DB_CONFIG, MYSQL_CONFIG


DDL_MYSQL = """
create table if not exists t_dm_task (
    student_id      bigint,
    group_id        bigint,
    subject_id      bigint,
    teacher_id      bigint,
    control_type_id bigint,
    exam_date       date,
    retake_date     date,
    grade           smallint,
    attendance_pct  smallint,
    hours_studied   decimal(6, 1),
    valid_from      date,
    valid_to        date,
    is_current      tinyint(1)
);
"""


def transfer_to_mysql() -> None:
    """
    Reads data from v_dm_task (PostgreSQL) and inserts it into t_dm_task (MySQL).
    """
    # читаем из PostgreSQL
    pg_conn = psycopg2.connect(**DB_CONFIG)
    try:
        with pg_conn.cursor() as cur:
            cur.execute("select * from s_sql_dds.v_dm_task;")
            rows = cur.fetchall()
    finally:
        pg_conn.close()

    # пишем в MySQL
    my_conn = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = my_conn.cursor()
        cursor.execute(DDL_MYSQL)
        cursor.execute("truncate table t_dm_task;")

        cursor.executemany("""
            insert into t_dm_task (
                student_id, group_id, subject_id, teacher_id, control_type_id,
                exam_date, retake_date, grade, attendance_pct, hours_studied,
                valid_from, valid_to, is_current
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)

        my_conn.commit()
        print(f"[transfer_to_mysql] перенесено {len(rows)} строк в MySQL t_dm_task.")
    finally:
        cursor.close()
        my_conn.close()