-- DDL: s_sql_dds.t_sql_source_unstructured
-- Сырая landing-таблица — все колонки хранятся как VARCHAR («данные как есть»).
-- Данные загружаются сюда без какой-либо трансформации из Python-приложения.

create schema if not exists s_sql_dds;

create table if not exists s_sql_dds.t_sql_source_unstructured (
    student_name    varchar,   -- ФИО студента
    group_name      varchar,   -- учебная группа
    subject         varchar,   -- название предмета
    teacher         varchar,   -- преподаватель
    control_type    varchar,   -- форма контроля
    exam_date       varchar,   -- дата сдачи (сырая строка)
    retake_date     varchar,   -- дата пересдачи (сырая строка)
    grade           varchar,   -- оценка (может содержать мусор)
    attendance_pct  varchar,   -- процент посещаемости (может содержать мусор)
    hours_studied   varchar    -- часов подготовки (может содержать мусор)
);
