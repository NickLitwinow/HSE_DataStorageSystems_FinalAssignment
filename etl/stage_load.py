# Модуль загрузки данных в стейджинг
# Загружает CSV файл в stage.sample_superstore с поддержкой очистки таблицы

import argparse
import os
from etl.db import get_connection


def load_stage_from_csv(csv_path: str) -> int:
    """
    Загрузка CSV файла в стейджинговую таблицу
    Возвращает количество загруженных строк
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    # Очистка таблицы, если TRUNCATE_STAGE=1
    truncate = os.environ.get("TRUNCATE_STAGE", "1") not in ("0", "false", "False")

    with get_connection() as conn:
        with conn.cursor() as cur:
            if truncate:
                cur.execute("TRUNCATE TABLE stage.sample_superstore")

            # COPY с указанием колонок для надежности
            copy_sql = (
                "COPY stage.sample_superstore (\n"
                "  ship_mode, segment, country, city, state, postal_code, region,\n"
                "  category, sub_category, sales, quantity, discount, profit\n"
                ") FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            )

            # Потоковое чтение и загрузка файла
            with open(csv_path, "r", encoding="utf-8") as f:
                with cur.copy(copy_sql) as copy:
                    while data := f.read(8192):
                        copy.write(data)

        conn.commit()

        # Проверка количества загруженных строк
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM stage.sample_superstore")
            rowcount = cur.fetchone()[0]
            return int(rowcount)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SampleSuperstore.csv into stage.sample_superstore")
    parser.add_argument("--csv", required=True, help="Path to SampleSuperstore.csv")
    args = parser.parse_args()
    total = load_stage_from_csv(args.csv)
    print({"stage_rows": total})