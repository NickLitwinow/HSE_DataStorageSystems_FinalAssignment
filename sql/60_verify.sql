-- Проверочные запросы для оценки результатов загрузки

-- 1. Подсчет количества записей во всех таблицах
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
SELECT 's_sale_metrics', count(*) FROM dv.s_sale_metrics;

-- 2. Пример аналитического запроса: продажи и прибыль по регионам и категориям
SELECT g.bk_region AS region, c.bk_category AS category,
       sum(sm.sales) AS total_sales, sum(sm.profit) AS total_profit
FROM dv.s_sale_metrics sm
JOIN dv.l_sale l ON l.sale_hk = sm.sale_hk
JOIN dv.h_geography g ON g.geography_hk = l.geography_hk
JOIN dv.h_category c ON c.category_hk = l.category_hk
GROUP BY g.bk_region, c.bk_category
ORDER BY total_sales DESC
LIMIT 20;