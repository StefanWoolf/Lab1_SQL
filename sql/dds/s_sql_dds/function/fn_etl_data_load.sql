create or replace function s_sql_dds.fn_etl_data_load(
    start_date date,
    end_date   date
)
returns void
language plpgsql
as $$
begin

    -- перед вставкой очищаем целевую таблицу для идемпотентности повторных запусков
    truncate table s_sql_dds.t_sql_source_structured;

    insert into s_sql_dds.t_sql_source_structured (
        student_name,
        group_name,
        subject,
        teacher,
        control_type,
        exam_date,
        retake_date,
        grade,
        attendance_pct,
        hours_studied,
        valid_from,
        valid_to,
        is_current
    )
    with

    -- шаг 1: оставляем одну строку для каждого уникального набора значений
    deduped as (
        select distinct on (
            trim(student_name),
            trim(group_name),
            trim(subject),
            trim(teacher),
            trim(control_type),
            trim(exam_date),
            trim(retake_date),
            trim(grade),
            trim(attendance_pct),
            trim(hours_studied),
            trim(valid_from),
            trim(valid_to),
            trim(is_current)
        )
            student_name,
            group_name,
            subject,
            teacher,
            control_type,
            exam_date,
            retake_date,
            grade,
            attendance_pct,
            hours_studied,
            valid_from,
            valid_to,
            is_current
        from s_sql_dds.t_sql_source_unstructured
        order by
            trim(student_name),
            trim(group_name),
            trim(subject),
            trim(teacher),
            trim(control_type),
            trim(exam_date),
            trim(retake_date),
            trim(grade),
            trim(attendance_pct),
            trim(hours_studied),
            trim(valid_from),
            trim(valid_to),
            trim(is_current)
    ),

    -- шаг 2: отброс строк с null или пустыми значениями в обязательных полях
    not_null as (
        select *
        from deduped
        where
            trim(coalesce(student_name,  '')) <> ''
            and trim(coalesce(group_name,   '')) <> ''
            and trim(coalesce(subject,      '')) <> ''
            and trim(coalesce(teacher,      '')) <> ''
            and trim(coalesce(control_type, '')) <> ''
            and trim(coalesce(exam_date,    '')) <> ''
            and trim(coalesce(valid_from,   '')) <> ''
            and trim(coalesce(valid_to,     '')) <> ''
            and trim(coalesce(is_current,   '')) <> ''
    ),

    -- шаг 3: отброс строк с непарсируемыми датами (ожидается формат yyyy-mm-dd)
    valid_dates as (
        select *
        from not_null
        where
            trim(exam_date)   ~ '^\d{4}-\d{2}-\d{2}$'
            and trim(retake_date) ~ '^\d{4}-\d{2}-\d{2}$'
            and trim(valid_from)  ~ '^\d{4}-\d{2}-\d{2}$'
            and trim(valid_to)    ~ '^\d{4}-\d{2}-\d{2}$'
    ),

    -- шаг 4: фильтрация по переданному временному диапазону (по exam_date)
    in_range as (
        select *
        from valid_dates
        where trim(exam_date)::date between start_date and end_date
    ),

    -- шаг 5: отброс строк, где grade не является целым числом
    valid_grade as (
        select *
        from in_range
        where trim(grade) ~ '^-?\d+$'
    ),

    -- шаг 6: отброс строк, где attendance_pct не является целым числом
    valid_attendance as (
        select *
        from valid_grade
        where trim(attendance_pct) ~ '^-?\d+$'
    ),

    -- шаг 7: отброс строк, где hours_studied не является числом
    valid_hours as (
        select *
        from valid_attendance
        where trim(hours_studied) ~ '^-?\d+(\.\d+)?$'
    ),

    -- шаг 8: отброс строк с некорректным значением is_current
    -- допустимые значения для булевого поля в postgresql
    valid_is_current as (
        select *
        from valid_hours
        where lower(trim(is_current)) in ('true', 'false', 't', 'f', '1', '0', 'yes', 'no')
    ),

    -- шаг 9: финальная трансформация — приведение типов, исправление аномалий
    cleaned as (
        select
            trim(student_name)               as student_name,
            trim(group_name)                 as group_name,
            trim(subject)                    as subject,
            trim(teacher)                    as teacher,
            lower(trim(control_type))        as control_type,

            trim(exam_date)::date            as exam_date,

            -- исправление инверсии дат: retake_date не может быть раньше exam_date
            greatest(
                trim(retake_date)::date,
                trim(exam_date)::date
            )                                as retake_date,

            -- оценки в допустимый диапазон [2, 5]
            greatest(2, least(5, trim(grade)::smallint))
                                             as grade,

            -- посещаемость в диапазон [0, 100]
            greatest(0, least(100, trim(attendance_pct)::smallint))
                                             as attendance_pct,

            -- часы подготовки в диапазон [0.5, 200]
            greatest(0.5, least(200.0, trim(hours_studied)::numeric(6, 1)))
                                             as hours_studied,

            -- scd type 2: valid_from не может быть позже exam_date
            least(
                trim(valid_from)::date,
                trim(exam_date)::date
            )                                as valid_from,

            trim(valid_to)::date             as valid_to,

            -- приведение is_current к boolean
            lower(trim(is_current)) in ('true', 't', '1', 'yes')
                                             as is_current

        from valid_is_current
    )

    select
        student_name,
        group_name,
        subject,
        teacher,
        control_type,
        exam_date,
        retake_date,
        grade,
        attendance_pct,
        hours_studied,
        valid_from,
        valid_to,
        is_current
    from cleaned;

    raise notice 'fn_etl_data_load: вставлено % строк в t_sql_source_structured.',
        (select count(*) from s_sql_dds.t_sql_source_structured);

end;
$$;