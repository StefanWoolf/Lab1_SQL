import random
import pandas as pd
from datetime import datetime, timedelta

STUDENTS = [
    "Иванов Алексей", "Петрова Мария", "Сидоров Дмитрий", "Козлова Анна",
    "Новиков Сергей", "Морозова Елена", "Волков Андрей", "Соколова Ольга",
    "Лебедев Кирилл", "Николаева Юлия", "Орлов Павел", "Зайцева Наталья",
]

SUBJECTS = [
    "Математика", "Физика", "Информатика", "История",
    "Химия", "Английский язык", "Экономика", "Философия",
]

GROUPS = ["ИТ-101", "ИТ-102", "ЭК-201", "ЭК-202", "МА-301", "МА-302"]

TEACHERS = [
    "Смирнов А.В.", "Кузнецова И.П.", "Попов Д.С.", "Васильева Т.Н.",
    "Захаров М.О.", "Федорова Л.Г.",
]

CONTROL_TYPES = ["экзамен", "зачет", "контрольная", "курсовая"]

GRADE_GARBAGE   = ["нет", "н/а", "??", "-", "не сдал"]
HOURS_GARBAGE   = ["много", "н/а", ""]
DATE_GARBAGE    = ["не указано", "n/a", "00-00-0000", ""]
CURRENT_GARBAGE = ["да", "1", "yes", "TRUE", None]


def _random_date(start: datetime, end: datetime) -> datetime:
    """Returns a random date in the range [start, end]."""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def get_dataset(n_rows: int = 500) -> pd.DataFrame:
    random.seed(67)

    date_start = datetime(2023, 9, 1)
    date_end   = datetime(2024, 6, 30)

    records = []
    for _ in range(n_rows):
        exam_date   = _random_date(date_start, date_end)
        retake_date = _random_date(exam_date, date_end)

        is_historical = random.random() < 0.25
        valid_from = _random_date(datetime(2023, 1, 1), exam_date)
        if is_historical:
            valid_to   = _random_date(valid_from, exam_date).strftime("%Y-%m-%d")
            is_current = "false"
        else:
            valid_to   = "9999-12-31"
            is_current = "true"

        records.append({
            "student_name":   random.choice(STUDENTS),
            "group_name":     random.choice(GROUPS),
            "subject":        random.choice(SUBJECTS),
            "teacher":        random.choice(TEACHERS),
            "control_type":   random.choice(CONTROL_TYPES),
            "exam_date":      exam_date.strftime("%Y-%m-%d"),
            "retake_date":    retake_date.strftime("%Y-%m-%d"),
            "grade":          random.randint(2, 5),
            "attendance_pct": random.randint(40, 100),
            "hours_studied":  round(random.uniform(1.0, 80.0), 1),
            "valid_from":     valid_from.strftime("%Y-%m-%d"),
            "valid_to":       valid_to,
            "is_current":     is_current,
        })

    df = pd.DataFrame(records)

    for col in ["grade", "attendance_pct", "hours_studied"]:
        df[col] = df[col].astype(object)

    dup_idx = df.sample(frac=0.05, random_state=1).index
    df = pd.concat([df, df.loc[dup_idx].copy()], ignore_index=True)

    for col in ["student_name", "subject", "group_name", "teacher"]:
        null_idx = df.sample(frac=0.07, random_state=2).index
        df.loc[null_idx, col] = None

    for idx in df.sample(frac=0.04, random_state=3).index:
        df.at[idx, "grade"] = random.choice(GRADE_GARBAGE)

    for idx in df.sample(frac=0.04, random_state=4).index:
        df.at[idx, "hours_studied"] = random.choice(HOURS_GARBAGE)

    for idx in df.sample(frac=0.03, random_state=5).index:
        df.at[idx, "grade"] = random.choice([0, 99, -1, 100])

    for idx in df.sample(frac=0.03, random_state=6).index:
        df.at[idx, "attendance_pct"] = random.choice([-10, 0, 999])

    inv_idx = df.sample(frac=0.04, random_state=7).index
    df.loc[inv_idx, "retake_date"] = "2022-01-01"

    for idx in df.sample(frac=0.05, random_state=8).index:
        if isinstance(df.at[idx, "student_name"], str):
            df.at[idx, "student_name"] = f"  {df.at[idx, 'student_name']}  "

    for idx in df.sample(frac=0.05, random_state=9).index:
        if isinstance(df.at[idx, "subject"], str):
            df.at[idx, "subject"] = f" {df.at[idx, 'subject']} "

    for idx in df.sample(frac=0.03, random_state=10).index:
        df.at[idx, "valid_from"] = random.choice(DATE_GARBAGE)

    for idx in df.sample(frac=0.03, random_state=11).index:
        df.at[idx, "is_current"] = random.choice(CURRENT_GARBAGE)

    df = df.astype(str)
    df = df.replace({"None": None, "nan": None, "NaN": None, "<NA>": None})

    print(f"[get_dataset] Сгенерировано {len(df)} строк с аномалиями")
    return df