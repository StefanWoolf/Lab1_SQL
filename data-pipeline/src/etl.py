from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table
from src.fill_dm_table import fill_dm_table
from src.transfer_to_mysql import transfer_to_mysql
from config import DATASET_ROWS, ETL_START_DATE, ETL_END_DATE


def etl() -> None:
    """
    Top-level ETL pipeline (no input parameters).

    Steps:
        1. get_dataset()          — generate synthetic dataset with anomalies
        2. load_data_to_db()      — load raw data into t_sql_source_unstructured
        3. fill_structured_table() — clean and load into t_sql_source_structured
        4. fill_dm_table()        — fill dimensions and t_dm_task
        5. transfer_to_mysql()    — copy v_dm_task data to MySQL
    """
    print("=" * 55)
    print("ETL-конвейер запущен")
    print("=" * 55)

    df = get_dataset(n_rows=DATASET_ROWS)
    load_data_to_db(df)
    fill_structured_table(start_date=ETL_START_DATE, end_date=ETL_END_DATE)
    fill_dm_table(start_dt=ETL_START_DATE, end_dt=ETL_END_DATE)
    transfer_to_mysql()

    print("=" * 55)
    print("ETL-конвейер успешно завершён.")
    print("=" * 55)