# Модуль проверки результатов
# Выводит статистику и пример аналитического запроса

from etl.db import get_connection


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Подсчет количества записей
            cur.execute(
                """
                SELECT 'stage_rows' AS metric, count(*) AS value FROM stage.sample_superstore
                UNION ALL
                SELECT 'h_ship_mode', count(*) FROM dv.h_ship_mode
                UNION ALL
                SELECT 'h_segment', count(*) FROM dv.h_segment
                UNION ALL
                SELECT 'h_geography', count(*) FROM dv.h_geography
                UNION ALL
                SELECT 'h_category', count(*) FROM dv.h_category
                UNION ALL
                SELECT 'h_sub_category', count(*) FROM dv.h_sub_category
                UNION ALL
                SELECT 'l_sale', count(*) FROM dv.l_sale
                UNION ALL
                SELECT 's_sale_metrics', count(*) FROM dv.s_sale_metrics
                """
            )
            rows = cur.fetchall()
            print({"counts": [{"metric": r[0], "value": int(r[1])} for r in rows]})

            # Пример аналитики: продажи по регионам и категориям
            cur.execute(
                """
                SELECT g.bk_region AS region, c.bk_category AS category,
                       sum(sm.sales) AS total_sales, sum(sm.profit) AS total_profit
                FROM dv.s_sale_metrics sm
                JOIN dv.l_sale l ON l.sale_hk = sm.sale_hk
                JOIN dv.h_geography g ON g.geography_hk = l.geography_hk
                JOIN dv.h_category c ON c.category_hk = l.category_hk
                GROUP BY g.bk_region, c.bk_category
                ORDER BY total_sales DESC
                LIMIT 20
                """
            )
            rows = cur.fetchall()
            results = [
                {
                    "region": r[0],
                    "category": r[1],
                    "total_sales": float(r[2]) if r[2] is not None else None,
                    "total_profit": float(r[3]) if r[3] is not None else None,
                }
                for r in rows
            ]
            print({"top_sales_by_region_category": results})


if __name__ == "__main__":
    main()