"""
Database connection utilities - reads credentials from config and connects.
"""
import pandas as pd


def _get_config_key(db_type):
    """Map UI database name to config key."""
    mapping = {
        "Netezza": "netezza",
        "Snowflake": "snowflake",
        "SQL Server": "sql_server",
        "PostgreSQL": "postgresql",
        "MySQL": "mysql",
        "Oracle": "oracle",
    }
    return mapping.get(db_type, db_type.lower().replace(" ", "_"))


def connect_db(db_type, config):
    """
    Connect to database using config. Returns (conn, error_msg).
    conn is the connection object or None; error_msg is None on success.
    """
    cfg_key = _get_config_key(db_type)
    db_config = config.get("databases", {}).get(cfg_key)
    if not db_config:
        return None, f"No config found for {db_type}. Add 'databases.{cfg_key}' to config."

    try:
        if db_type == "Netezza":
            return _connect_netezza(db_config)
        elif db_type == "Snowflake":
            return _connect_snowflake(db_config)
        elif db_type == "SQL Server":
            return _connect_sqlserver(db_config)
        elif db_type == "PostgreSQL":
            return _connect_postgresql(db_config)
        elif db_type == "MySQL":
            return _connect_mysql(db_config)
        elif db_type == "Oracle":
            return _connect_oracle(db_config)
        else:
            return None, f"Unsupported database: {db_type}"
    except Exception as e:
        return None, str(e)


def run_query(conn, db_type, query):
    """Run a query and return DataFrame. conn from connect_db()."""
    if conn is None:
        return None
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        raise e


def _quote_table(db_type, table_name):
    """Quote table name for the given database type. Handles schema.table format."""
    if "." in table_name:
        parts = table_name.split(".", 1)
        if db_type == "SQL Server":
            return f"[{parts[0]}].[{parts[1]}]"
        return f'"{parts[0]}"."{parts[1]}"'
    if db_type == "SQL Server":
        return f"[{table_name}]"
    return f'"{table_name}"'


def get_table_columns(conn, db_type, table_name):
    """Get list of column names for a table. Returns (columns, error_msg)."""
    if conn is None:
        return [], "No connection"
    try:
        quoted = _quote_table(db_type, table_name)
        if db_type == "SQL Server":
            q = f"SELECT TOP 0 * FROM {quoted}"
        elif db_type == "Oracle":
            q = f"SELECT * FROM {quoted} WHERE ROWNUM < 1"
        else:
            q = f"SELECT * FROM {quoted} LIMIT 0"
        df = pd.read_sql(q, conn)
        return list(df.columns), None
    except Exception as e:
        return [], str(e)


def _quote_col(db_type, col):
    """Quote column name for the given database type."""
    if db_type == "SQL Server":
        return f"[{col}]"
    return f'"{col}"'


def fetch_table_data(conn, db_type, table_name, columns, limit=100):
    """Fetch data for specified columns from table. Returns (DataFrame, error_msg)."""
    if conn is None:
        return None, "No connection"
    if not columns:
        return pd.DataFrame(), None
    try:
        quoted = _quote_table(db_type, table_name)
        col_list = ", ".join(_quote_col(db_type, c) for c in columns)
        if db_type == "SQL Server":
            q = f"SELECT TOP {limit} {col_list} FROM {quoted}"
        elif db_type == "Oracle":
            q = f"SELECT {col_list} FROM {quoted} WHERE ROWNUM <= {limit}"
        else:
            q = f"SELECT {col_list} FROM {quoted} LIMIT {limit}"
        df = pd.read_sql(q, conn)
        return df, None
    except Exception as e:
        return None, str(e)


def _connect_netezza(cfg):
    try:
        import nzpy
    except ImportError:
        return None, "Install nzpy: pip install nzpy"
    conn = nzpy.connect(
        user=cfg.get("username"),
        password=cfg.get("password"),
        host=cfg.get("host"),
        port=int(cfg.get("port", 5480)),
        database=cfg.get("database"),
        securityLevel=1,
        logLevel=0,
    )
    return conn, None


def _connect_snowflake(cfg):
    try:
        import snowflake.connector
    except ImportError:
        return None, "Install snowflake-connector-python: pip install snowflake-connector-python"
    conn = snowflake.connector.connect(
        account=cfg.get("account"),
        user=cfg.get("username"),
        password=cfg.get("password"),
        warehouse=cfg.get("warehouse"),
        database=cfg.get("database"),
        schema=cfg.get("schema", "PUBLIC"),
        role=cfg.get("role"),
    )
    return conn, None


def _connect_sqlserver(cfg):
    try:
        import pyodbc
    except ImportError:
        return None, "Install pyodbc: pip install pyodbc"
    driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={cfg.get('host')},{cfg.get('port', 1433)};"
        f"DATABASE={cfg.get('database')};"
        f"UID={cfg.get('username')};"
        f"PWD={cfg.get('password')}"
    )
    conn = pyodbc.connect(conn_str)
    return conn, None


def _connect_postgresql(cfg):
    try:
        import psycopg2
    except ImportError:
        return None, "Install psycopg2: pip install psycopg2-binary"
    conn = psycopg2.connect(
        host=cfg.get("host"),
        port=int(cfg.get("port", 5432)),
        dbname=cfg.get("database"),
        user=cfg.get("username"),
        password=cfg.get("password"),
        sslmode=cfg.get("sslmode"),
    )
    return conn, None


def _connect_mysql(cfg):
    try:
        import pymysql
    except ImportError:
        return None, "Install pymysql: pip install pymysql"
    conn = pymysql.connect(
        host=cfg.get("host"),
        port=int(cfg.get("port", 3306)),
        database=cfg.get("database"),
        user=cfg.get("username"),
        password=cfg.get("password"),
    )
    return conn, None


def _connect_oracle(cfg):
    try:
        import oracledb
    except ImportError:
        try:
            import cx_Oracle as oracledb
        except ImportError:
            return None, "Install oracledb: pip install oracledb"
    dsn = oracledb.makedsn(
        cfg.get("host"),
        int(cfg.get("port", 1521)),
        service_name=cfg.get("service_name", "ORCL"),
    )
    conn = oracledb.connect(
        user=cfg.get("username"),
        password=cfg.get("password"),
        dsn=dsn,
    )
    return conn, None
