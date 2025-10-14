# Модуль выполнения SQL файлов
# Позволяет запускать один или несколько SQL файлов в заданном порядке

import argparse
import os
from typing import List

from etl.db import get_connection


def run_sql_file(path: str) -> None:
    """Выполнение одного SQL файла"""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    sql = open(path, "r", encoding="utf-8").read()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def run_sql_files_in_order(paths: List[str]) -> None:
    """Последовательное выполнение нескольких SQL файлов в одной транзакции"""
    with get_connection() as conn:
        for p in paths:
            if not os.path.exists(p):
                raise FileNotFoundError(p)
            with open(p, "r", encoding="utf-8") as f:
                sql = f.read()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL files against Greenplum")
    parser.add_argument("files", nargs="*", help="Paths to SQL files; if empty, uses default order")
    args = parser.parse_args()

    if args.files:
        run_sql_files_in_order(args.files)
    else:
        # Стандартный порядок выполнения SQL файлов
        default_order = [
            "sql/00_schemas.sql",
            "sql/01_stage.sql",
            "sql/10_hubs.sql",
            "sql/20_links.sql",
            "sql/40_constraints.sql",
            "sql/50_etl.sql",
            "sql/60_verify.sql",
        ]
        run_sql_files_in_order(default_order)
        print({"status": "ok", "ran_files": default_order})