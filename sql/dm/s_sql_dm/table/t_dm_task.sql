create table if not exists s_sql_dds.t_dm_task (
    student_id      bigint,
    group_id        bigint,
    subject_id      bigint,
    teacher_id      bigint,
    control_type_id bigint,
    exam_date       date,
    retake_date     date,
    grade           smallint,
    attendance_pct  smallint,
    hours_studied   numeric(6, 1),
    valid_from      date,
    valid_to        date,
    is_current      boolean
);