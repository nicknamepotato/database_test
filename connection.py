# Database connection configuration
DB_CONFIG = {
    "host": "192.168.1.155",
    "database": "postgres",
    "user": "pgsql",
    "password": "pgsql123",
    "port": "5432"
}

# Construct the DSN (Data Source Name) string for psycopg2
DSN = (
    f"host={DB_CONFIG['host']} "
    f"dbname={DB_CONFIG['database']} "
    f"user={DB_CONFIG['user']} "
    f"port={DB_CONFIG['port']} "
    f"password={DB_CONFIG['password']}"
)