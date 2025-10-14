-- Стейджинговая таблица для загрузки SampleSuperstore.csv
-- Структура соответствует исходному файлу + технические поля

CREATE TABLE IF NOT EXISTS stage.sample_superstore (
    record_id bigserial, -- Уникальный идентификатор записи
    ship_mode text, -- Способ доставки
    segment text, -- Сегмент клиента
    country text, -- Страна
    city text, -- Город
    state text, -- Штат
    postal_code text, -- Почтовый индекс (text для сохранения ведущих нулей)
    region text, -- Регион
    category text, -- Категория товара
    sub_category text, -- Подкатегория товара
    sales numeric(18,4), -- Сумма продажи
    quantity integer, -- Количество
    discount numeric(9,4), -- Скидка
    profit numeric(18,4), -- Прибыль
    load_dts timestamp without time zone default now(), -- Дата/время загрузки
    record_source text default 'SampleSuperstore.csv' -- Источник данных
)
DISTRIBUTED BY (record_id); -- Распределение по ID для равномерной загрузки

COMMENT ON TABLE stage.sample_superstore IS 'Raw staging table for SampleSuperstore.csv';