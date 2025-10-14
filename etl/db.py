# Модуль подключения к Greenplum
# Обеспечивает безопасное соединение с БД через переменные окружения

import os
import psycopg
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Получение значения из переменных окружения с поддержкой значений по умолчанию"""
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def get_connection():
    """
    Создание подключения к Greenplum в Yandex Cloud
    
    Требуемые переменные окружения:
    - GP_HOSTS: хосты через запятую
    - GP_PORT: порт (обычно 6432)
    - GP_DB: имя базы данных
    - GP_USER: пользователь
    - GP_PASSWORD: пароль
    """
    hosts = get_env("GP_HOSTS")
    if not hosts:
        raise RuntimeError("GP_HOSTS is required (comma-separated hostnames)")

    port = int(get_env("GP_PORT", "6432"))
    dbname = get_env("GP_DB", "postgres")
    user = get_env("GP_USER")
    password = get_env("GP_PASSWORD")
    if not user or not password:
        raise RuntimeError("GP_USER and GP_PASSWORD are required")

    target_session_attrs = get_env("GP_TARGET_SESSION_ATTRS", "read-write")
    sslmode = get_env("GP_SSLMODE", "verify-full")
    sslrootcert = os.path.expanduser(get_env("GP_SSLROOTCERT", "~/.postgresql/root.crt"))

    dsn = (
        f"host={hosts} port={port} dbname={dbname} user={user} password={password} "
        f"target_session_attrs={target_session_attrs} sslmode={sslmode} sslrootcert={sslrootcert}"
    )
    conn = psycopg.connect(dsn)
    return conn


if __name__ == "__main__":
    # Тестовое подключение и вывод версии БД
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), version()")
            row = cur.fetchone()
            print({"database": row[0], "version": row[1].split(" on ")[0]})