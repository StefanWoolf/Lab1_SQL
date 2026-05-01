from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table
from config import DATASET_ROWS, ETL_START_DATE, ETL_END_DATE


def etl() -> None:
    """
    Верхнеуровневая функция ETL-конвейера без входных параметров.

    Шаги:
        1. get_dataset()           — генерация синтетического датасета с аномалиями
        2. load_data_to_db()       — загрузка сырых данных в t_sql_source_unstructured
        3. fill_structured_table() — вызов SQL-функции fn_etl_data_load для очистки
                                     и заполнения t_sql_source_structured
    """
    print("=" * 55)
    print("ETL-конвейер запущен")
    print("=" * 55)

    # Шаг 1 — генерация данных с намеренными аномалиями
    df = get_dataset(n_rows=DATASET_ROWS)

    # Шаг 2 — загрузка сырых данных в PostgreSQL (неструктурированная таблица)
    load_data_to_db(df)

    # Шаг 3 — трансформация и загрузка в структурированную таблицу через SQL-функцию
    fill_structured_table(
        start_date=ETL_START_DATE,
        end_date=ETL_END_DATE,
    )

    print("=" * 55)
    print("ETL-конвейер успешно завершён.")
    print("=" * 55)
