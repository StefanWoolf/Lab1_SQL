create or replace function s_sql_dds.fn_dm_data_load(
    start_dt date,
    end_dt   date
)
returns void
language plpgsql
as $$
begin

    -- заполняем справочники уникальными значениями (insert if not exists)
    insert into s_sql_dds.t_dim_student (name)
    select distinct student_name
    from s_sql_dds.t_sql_source_structured
    where student_name not in (select name from s_sql_dds.t_dim_student);

    insert into s_sql_dds.t_dim_group (name)
    select distinct group_name
    from s_sql_dds.t_sql_source_structured
    where group_name not in (select name from s_sql_dds.t_dim_group);

    insert into s_sql_dds.t_dim_subject (name)
    select distinct subject
    from s_sql_dds.t_sql_source_structured
    where subject not in (select name from s_sql_dds.t_dim_subject);

    insert into s_sql_dds.t_dim_teacher (name)
    select distinct teacher
    from s_sql_dds.t_sql_source_structured
    where teacher not in (select name from s_sql_dds.t_dim_teacher);

    insert into s_sql_dds.t_dim_control_type (name)
    select distinct control_type
    from s_sql_dds.t_sql_source_structured
    where control_type not in (select name from s_sql_dds.t_dim_control_type);

    -- заполняем t_dm_task
    truncate table s_sql_dds.t_dm_task;

    insert into s_sql_dds.t_dm_task (
        student_id,
        group_id,
        subject_id,
        teacher_id,
        control_type_id,
        exam_date,
        retake_date,
        grade,
        attendance_pct,
        hours_studied,
        valid_from,
        valid_to,
        is_current
    )
    select
        ds.id,
        dg.id,
        dsub.id,
        dt.id,
        dc.id,
        s.exam_date,
        s.retake_date,
        s.grade,
        s.attendance_pct,
        s.hours_studied,
        s.valid_from,
        s.valid_to,
        s.is_current
    from s_sql_dds.t_sql_source_structured s
    join s_sql_dds.t_dim_student      ds   on ds.name   = s.student_name
    join s_sql_dds.t_dim_group        dg   on dg.name   = s.group_name
    join s_sql_dds.t_dim_subject      dsub on dsub.name = s.subject
    join s_sql_dds.t_dim_teacher      dt   on dt.name   = s.teacher
    join s_sql_dds.t_dim_control_type dc   on dc.name   = s.control_type
    where s.exam_date between start_dt and end_dt;

    raise notice 'fn_dm_data_load: вставлено % строк в t_dm_task.',
        (select count(*) from s_sql_dds.t_dm_task);

end;
$$;