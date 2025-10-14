-- Создание схем для стейджинга и Data Vault
-- Схема stage для сырых данных, схема dv для Data Vault структур

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dv;

COMMENT ON SCHEMA stage IS 'Staging area for raw loads from SampleSuperstore.csv';
COMMENT ON SCHEMA dv IS 'Data Vault (hubs, links, satellites)';