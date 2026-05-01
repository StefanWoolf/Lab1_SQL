create or replace view s_sql_dds.v_dm_task as
select
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
from s_sql_dds.t_dm_task;