-- DDL: s_sql_dds.t_sql_source_structured
-- Целевая таблица с очищенными и типизированными данными.
-- Заполняется SQL-функцией fn_etl_data_load — напрямую не пишем.

create table if not exists s_sql_dds.t_sql_source_structured (
    student_name    varchar(150)    not null,  -- ФИО студента (категориальный)
    group_name      varchar(20)     not null,  -- учебная группа (категориальный)
    subject         varchar(100)    not null,  -- название предмета (категориальный)
    teacher         varchar(100)    not null,  -- преподаватель (категориальный)
    control_type    varchar(50)     not null,  -- форма контроля (категориальный)
    exam_date       date            not null,  -- дата сдачи
    retake_date     date            not null,  -- дата пересдачи (>= exam_date)
    grade           smallint        not null,  -- оценка [2, 5]
    attendance_pct  smallint        not null,  -- процент посещаемости [0, 100]
    hours_studied   numeric(6, 1)   not null   -- часов подготовки [0.5, 200]
);
