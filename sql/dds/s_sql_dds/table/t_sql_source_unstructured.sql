create schema if not exists s_sql_dds;

create table if not exists s_sql_dds.t_sql_source_unstructured (
    student_name    varchar,   -- фио студента
    group_name      varchar,   -- учебная группа
    subject         varchar,   -- название предмета
    teacher         varchar,   -- преподаватель
    control_type    varchar,   -- форма контроля
    exam_date       varchar,   -- дата сдачи (сырая строка)
    retake_date     varchar,   -- дата пересдачи (сырая строка)
    grade           varchar,   -- оценка (может содержать мусор)
    attendance_pct  varchar,   -- процент посещаемости (может содержать мусор)
    hours_studied   varchar,   -- часов подготовки (может содержать мусор)
    valid_from      varchar,   -- дата начала действия версии записи scd type 2 (сырая строка)
    valid_to        varchar,   -- дата окончания действия версии scd type 2 (сырая строка)
    is_current      varchar    -- флаг актуальности версии (может содержать мусор)
);