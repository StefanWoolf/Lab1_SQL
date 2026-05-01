import random
import pandas as pd
from datetime import datetime, timedelta

# Справочники с русскими значениями
СТУДЕНТЫ = [
    "Иванов Алексей", "Петрова Мария", "Сидоров Дмитрий", "Козлова Анна",
    "Новиков Сергей", "Морозова Елена", "Волков Андрей", "Соколова Ольга",
    "Лебедев Кирилл", "Николаева Юлия", "Орлов Павел", "Зайцева Наталья",
]

ПРЕДМЕТЫ = [
    "Математика", "Физика", "Информатика", "История",
    "Химия", "Английский язык", "Экономика", "Философия",
]

ГРУППЫ = ["ИТ-101", "ИТ-102", "ЭК-201", "ЭК-202", "МА-301", "МА-302"]

ПРЕПОДАВАТЕЛИ = [
    "Смирнов А.В.", "Кузнецова И.П.", "Попов Д.С.", "Васильева Т.Н.",
    "Захаров М.О.", "Федорова Л.Г.",
]

ФОРМА_КОНТРОЛЯ = ["экзамен", "зачет", "контрольная", "курсовая"]


def _случайная_дата(start: datetime, end: datetime) -> datetime:
    """Возвращает случайную дату в диапазоне [start, end]."""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def get_dataset(n_rows: int = 500) -> pd.DataFrame:
    """
    Генерирует синтетический датасет успеваемости студентов.

    Поля:
        student_name    — ФИО студента (категориальный)
        group_name      — учебная группа (категориальный)
        subject         — название предмета (категориальный)
        teacher         — преподаватель (категориальный)
        control_type    — форма контроля (категориальный)
        exam_date       — дата сдачи
        retake_date     — дата пересдачи
        grade           — оценка (2–5)
        attendance_pct  — процент посещаемости
        hours_studied   — часов подготовки

    Намеренно внедрённые аномалии:
        - Дубликаты строк (~5%)
        - NULL в обязательных полях (~7%)
        - Строковый мусор в числовых полях (~4%)
        - Выбросы: оценка=99, посещаемость=-10 (~3%)
        - Инверсия дат: retake_date < exam_date (~4%)
        - Лишние пробелы в строковых полях (~5%)
    """
    random.seed(42)

    date_start = datetime(2023, 9, 1)
    date_end   = datetime(2024, 6, 30)

    # Генерация чистых записей
    records = []
    for i in range(1, n_rows + 1):
        exam_date   = _случайная_дата(date_start, date_end)
        retake_date = _случайная_дата(exam_date, date_end)

        records.append({
            "student_name":   random.choice(СТУДЕНТЫ),
            "group_name":     random.choice(ГРУППЫ),
            "subject":        random.choice(ПРЕДМЕТЫ),
            "teacher":        random.choice(ПРЕПОДАВАТЕЛИ),
            "control_type":   random.choice(ФОРМА_КОНТРОЛЯ),
            "exam_date":      exam_date.strftime("%Y-%m-%d"),
            "retake_date":    retake_date.strftime("%Y-%m-%d"),
            "grade":          random.randint(2, 5),
            "attendance_pct": random.randint(40, 100),
            "hours_studied":  round(random.uniform(1.0, 80.0), 1),
        })

    df = pd.DataFrame(records)

    # Приводим числовые колонки к object, чтобы можно было вставить строковый мусор
    for col in ["grade", "attendance_pct", "hours_studied"]:
        df[col] = df[col].astype(object)

    # ── Внедрение аномалий ────────────────────────────────────────────────────

    # 1. Дубликаты строк (~5%)
    dup_idx = df.sample(frac=0.05, random_state=1).index
    df = pd.concat([df, df.loc[dup_idx].copy()], ignore_index=True)

    # 2. NULL в обязательных категориальных полях (~7%)
    for col in ["student_name", "subject", "group_name", "teacher"]:
        null_idx = df.sample(frac=0.07, random_state=2).index
        df.loc[null_idx, col] = None

    # 3. Строковый мусор в числовых полях (~4%)
    мусор_оценки = ["нет", "н/а", "??", "-", "не сдал"]
    for idx in df.sample(frac=0.04, random_state=3).index:
        df.at[idx, "grade"] = random.choice(мусор_оценки)

    мусор_часов = ["много", "н/а", ""]
    for idx in df.sample(frac=0.04, random_state=4).index:
        df.at[idx, "hours_studied"] = random.choice(мусор_часов)

    # 4. Выбросы в числовых полях (~3%)
    for idx in df.sample(frac=0.03, random_state=5).index:
        df.at[idx, "grade"] = random.choice([0, 99, -1, 100])

    for idx in df.sample(frac=0.03, random_state=6).index:
        df.at[idx, "attendance_pct"] = random.choice([-10, 0, 999])

    # 5. Инверсия дат: retake_date раньше exam_date (~4%)
    inv_idx = df.sample(frac=0.04, random_state=7).index
    df.loc[inv_idx, "retake_date"] = "2022-01-01"

    # 6. Лишние пробелы в строковых полях (~5%)
    for idx in df.sample(frac=0.05, random_state=8).index:
        if isinstance(df.at[idx, "student_name"], str):
            df.at[idx, "student_name"] = f"  {df.at[idx, 'student_name']}  "

    for idx in df.sample(frac=0.05, random_state=9).index:
        if isinstance(df.at[idx, "subject"], str):
            df.at[idx, "subject"] = f" {df.at[idx, 'subject']} "

    # Все колонки приводим к строке — в неструктурированной таблице всё VARCHAR
    df = df.astype(str).replace("None", None).replace("nan", None)

    print(f"[get_dataset] Сгенерировано {len(df)} строк с намеренными аномалиями.")
    return df
