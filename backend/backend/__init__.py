import os

if os.getenv("DJANGO_DB_ENGINE", "sqlite").lower() == "mysql":
    try:
        import pymysql

        pymysql.install_as_MySQLdb()
    except Exception:
        # Fallback: Django will raise a clear DB backend error on startup.
        pass
