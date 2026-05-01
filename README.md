# ETL Pipeline — Успеваемость студентов

## Описание

Синтетический датасет успеваемости студентов университета.
ETL-конвейер: генерация → загрузка сырых данных в PostgreSQL → очистка через SQL-функцию → витрина в MySQL.

## Структура данных (13 полей)

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
| valid_from | date | Дата начала действия версии записи (SCD Type 2) |
| valid_to | date | Дата окончания версии (9999-12-31 = актуальна) |
| is_current | boolean | Флаг актуальности версии |

## Аномалии в сырых данных

- Дубликаты строк (~5%)
- NULL в обязательных полях (~7%)
- Строковый мусор в числовых полях (`"нет"`, `"н/а"`, `"не сдал"`)
- Выбросы (`grade=99`, `attendance_pct=-10`)
- Инверсия дат (`retake_date < exam_date`)
- Лишние пробелы в строковых полях
- Мусор в полях историчности (`valid_from="n/a"`, `is_current="да"`)

## Быстрый старт

### 1. Установить зависимости
```bash
cd data-pipeline
pip install -r requirements.txt
pip install mysql-connector-python
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

MYSQL_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "database": "sql_lab",
    "user":     "root",
    "password": "ваш_пароль_mysql",
}
```

### 3. Создать БД и применить SQL (PostgreSQL)
```bash
psql -U postgres -c "create database sql_lab;"

# ЛР1 — ETL
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/table/t_sql_source_unstructured.sql
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/table/t_sql_source_structured.sql
psql -U postgres -d sql_lab -f sql/dds/s_sql_dds/function/fn_etl_data_load.sql

# ЛР2 — DWH и витрина
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dim_student.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dim_group.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dim_subject.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dim_teacher.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dim_control_type.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/table/t_dm_task.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/function/fn_dm_data_load.sql
psql -U postgres -d sql_lab -f sql/dm/s_sql_dds/view/v_dm_task.sql
```

### 4. Создать БД в MySQL
```sql
create database sql_lab;
```

### 5. Запустить ETL
```bash
cd data-pipeline
python main.py
```

## Архитектура ЛР2 — схема звезда

Центральная таблица `t_dm_task` хранит ID из справочников и количественные факты.

```
t_dim_student ─┐
t_dim_group   ─┤
t_dim_subject ─┼─→ t_dm_task ─→ v_dm_task ─→ MySQL t_dm_task
t_dim_teacher ─┤
t_dim_control_type ─┘
```

Витрина `v_dm_task` — представление над `t_dm_task` с явным перечислением полей.
Данные из витрины перекладываются в MySQL через `transfer_to_mysql()`.