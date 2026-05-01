# ETL Pipeline — Успеваемость студентов

## Описание

Синтетический датасет успеваемости студентов университета.
ETL-конвейер: генерация → загрузка сырых данных в PostgreSQL → очистка через SQL-функцию.

## Структура данных (10 полей)

| Поле | Тип | Описание |
|---|---|---|
| student_name | varchar | ФИО студента (категориальный) |
| group_name | varchar | Учебная группа (категориальный) |
| subject | varchar | Название предмета (категориальный) |
| teacher | varchar | Преподаватель (категориальный) |
| control_type | varchar | Форма контроля (категориальный) |
| exam_date | date | Дата сдачи |
| retake_date | date | Дата пересдачи (>= exam_date) |
| grade | smallint | Оценка [2, 5] (количественный) |
| attendance_pct | smallint | Процент посещаемости [0, 100] (количественный) |
| hours_studied | numeric(6,1) | Часов подготовки (количественный) |

## Аномалии в сырых данных

- Дубликаты строк (~5%)
- NULL в обязательных полях (~7%)
- Строковый мусор в числовых полях (`"нет"`, `"н/а"`, `"не сдал"`)
- Выбросы (`grade=99`, `attendance_pct=-10`)
- Инверсия дат (`retake_date < exam_date`)
- Лишние пробелы в строковых полях

## Быстрый старт

### 1. Установить зависимости
```bash
cd data-pipeline
pip install -r requirements.txt
```

### 2. Настроить config.py
```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "sql_lab",
    "user":     "postgres",
    "password": "ваш_пароль",
}
```

### 3. Создать БД и применить SQL
```bash
psql -U postgres -c "create database sql_lab;"
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/table/t_sql_source_unstructured.sql
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/table/t_sql_source_structured.sql
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/function/fn_etl_data_load.sql
```

### 4. Запустить ETL
```bash
cd data-pipeline
python main.py
```
