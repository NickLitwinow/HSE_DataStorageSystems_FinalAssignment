# Основной модуль ETL процесса
# Выполняет полный цикл: создание структур, загрузка в stage, ETL в Data Vault

import argparse
import os
from typing import List

from etl.db import get_connection
from etl.stage_load import load_stage_from_csv


def get_sql_path(filename: str) -> str:
    """Получение полного пути к SQL файлу"""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", filename)


def run_sql_files_in_order(conn, paths: List[str]) -> None:
    """Выполнение SQL файлов в одном подключении"""
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def main(csv_path: str):
    # 1. Создание структур Data Vault
    with get_connection() as conn:
        run_sql_files_in_order(
            conn,
            [
                get_sql_path("00_schemas.sql"),  # Схемы stage и dv
                get_sql_path("01_stage.sql"),    # Стейджинговая таблица
                get_sql_path("10_hubs.sql"),     # Хабы и их сателлиты
                get_sql_path("20_links.sql"),    # Линки и их сателлиты
                get_sql_path("40_constraints.sql"), # Ограничения и индексы
            ],
        )

    # 2. Загрузка в stage
    staged = load_stage_from_csv(csv_path)
    print({"stage_rows": staged})

    # 3. ETL в Data Vault и проверка
    with get_connection() as conn:
        run_sql_files_in_order(conn, [
            get_sql_path("50_etl.sql"),     # Загрузка в Data Vault
            get_sql_path("60_verify.sql")    # Проверка результатов
        ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-to-end run: create DV, load stage, ETL, verify")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    main(args.csv)