import os
import sys
import yaml
import pandas as pd
import datacompy
import warnings
import mysql.connector
from snowflake.connector import connect


# Suppress warning
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

output_report_file_name = "Dimension_validation_report.csv"
filter_start_ts = "2025-05-01 "
filter_end_ts = "2025-06-01"


def get_snowflake_connection():
    """
    Establish a connection to Snowflake using environment variables.
    Variables:
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    - SNOWFLAKE_ROLE
    """
    return connect(
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        insecure_mode=True  # ⚠️⚠️⚠️ not for production, only for local validation.
    )


def get_mysql_connection():
    """
    Establishes a connection to the MySQL database.

    Returns:
        mysql.connector.connection.MySQLConnection: Active connection.
    """
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    )


def fetch_count_mysql(conn, schema, table, filter_clause=None):
    """
    Fetches all rows from a MySQL table as a pandas DataFrame.

    Args:
        conn (MySQLConnection): Active MySQL connection.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        pd.DataFrame: Table contents as a DataFrame.
    """
    cursor = conn.cursor()
    cursor.execute(f"USE {schema};")
    query = f"SELECT COUNT(*) FROM {table}"
    if filter_clause:
        query += f" WHERE {filter_clause} > '{filter_start_ts}' and {filter_clause} < '{filter_end_ts}'"
    df = pd.read_sql(query, conn)
    return df


def fetch_dataframe_mysql(conn, schema, table, id, filter_clause=None):
    """
    Fetches all rows from a MySQL table as a pandas DataFrame.

    Args:
        conn (MySQLConnection): Active MySQL connection.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        pd.DataFrame: Table contents as a DataFrame.
    """
    cursor = conn.cursor()
    cursor.execute(f"USE {schema};")
    # query = f'SELECT t1.* , "" AS _SNOWFLAKE_INSERTED_AT, "" AS _SNOWFLAKE_UPDATED_AT, "" AS _SNOWFLAKE_DELETED FROM {table} as t1'
    query = f'SELECT * FROM {table}'
    if filter_clause:
        query += f" WHERE {filter_clause} > '{filter_start_ts}' and {filter_clause} < '{filter_end_ts}'"
    query += f" ORDER BY {id} LIMIT 100;"
    df = pd.read_sql(query, conn)
    return df


def fetch_count_snowflake(conn, database, schema, table, filter_clause=None):
    """
    Fetches all rows from a Snowflake table as a pandas DataFrame.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
        database (str): Database name.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        pd.DataFrame: Table contents as a DataFrame.
    """
    query = f'SELECT COUNT(*) FROM {database}."{schema}"."{table}"'
    if filter_clause:
        query += f" WHERE {filter_clause} > '{filter_start_ts}' and {filter_clause} < '{filter_end_ts}'"
    return pd.read_sql(query, conn)


def fetch_dataframe_snowflake(conn, database, schema, table, id, filter_clause=None):
    """
    Fetches all rows from a Snowflake table as a pandas DataFrame.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
        database (str): Database name.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        pd.DataFrame: Table contents as a DataFrame.
    """
    query = f'SELECT * FROM {database}."{schema}"."{table}"'
    if filter_clause:
        query += f" WHERE {filter_clause} > '{filter_start_ts}' and {filter_clause} < '{filter_end_ts}'"
    query += f" ORDER BY {id} LIMIT 100;"
    return pd.read_sql(query, conn)


def sanitize_bytearrays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts bytearray columns to hex strings to avoid unhashable type errors.
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, bytearray)).any():
            df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytearray) else x)
    return df


def compare_dataframes(**kwargs):
    """
    Compares two DataFrames from MySQL and Snowflake for:
    - Row count
    - Schema column match
    - Row-level data match
    - Null values check
    - Data type match

    Saves a summary to a CSV and a full row diff report using datacompy.
    """
    src_df = kwargs["src_df"]
    tgt_df = kwargs["tgt_df"]
    src_cnt = kwargs["src_cnt"]
    tgt_cnt = kwargs["tgt_cnt"]
    src_table = kwargs["src_table"]
    src_id = kwargs["src_id"]
    tgt_table = kwargs["tgt_table"]
    src_schema = kwargs["src_schema"]
    tgt_database = kwargs["tgt_database"]
    tgt_schema = kwargs["tgt_schema"]

    print(f"\n⏳ Comparing: {src_schema}.{src_table} → {tgt_database}.{tgt_schema}.{tgt_table}")
    
    # 1. Row Count Match
    src_row_count = src_cnt.iloc[0, 0]
    tgt_row_count = tgt_cnt.iloc[0, 0]

    count_val_status = "Match" if src_row_count == tgt_row_count else "Mismatch"
    print(f"Row Count → Source: {src_row_count}, Target: {tgt_row_count} → {count_val_status}")

    # 2. Schema Match (column names)
    src_cols = set(col.lower().replace('"',"" ) for col in src_df.columns)
    tgt_cols = set(col.lower().replace('"',"" ) for col in tgt_df.columns)
    schema_val_status = "Match" if src_cols == tgt_cols else "Mismatch"
    print(f"Schema Columns → {schema_val_status}")

    # 3. Data Type Check
    src_dtypes = src_df.dtypes.apply(lambda x: str(x)).sort_index()
    tgt_dtypes = tgt_df.dtypes.apply(lambda x: str(x)).sort_index()
    dtype_check = src_dtypes.equals(tgt_dtypes)
    dtype_val_status = "Match" if dtype_check else "Equivalent"
    print(f"Data Types → {dtype_val_status}")

    # 4. Null values comparison
    null_val_status = "Match"
    null_mismatch = []

    common_columns = set(src_df.columns) & set(tgt_df.columns)

    null_val_status = "Match"
    null_mismatch = []

    for col in common_columns:
        src_nulls = src_df[col].isnull().sum()
        tgt_nulls = tgt_df[col].isnull().sum()
        if src_nulls != tgt_nulls:
            null_mismatch.append((col, src_nulls, tgt_nulls))
            null_val_status = "Mismatch"

    if null_val_status == "Match":
        print("Null Values → Match")
    else:
        print("Null Values → Mismatch")
        for col, src, tgt in null_mismatch:
            print(f"Column '{col}': Source NULLs = {src}, Target NULLs = {tgt}")

    # 5. Sanitize bytearrays if needed
    src_df = sanitize_bytearrays(src_df)
    tgt_df = sanitize_bytearrays(tgt_df)

    # 6. Sort & Compare - Row Level validation
    src_df_sorted = src_df.sort_index(axis=1).sort_values(by=sorted(src_df.columns), ignore_index=True)
    tgt_df_sorted = tgt_df.sort_index(axis=1).sort_values(by=sorted(tgt_df.columns), ignore_index=True)

    data_val_status = "Match" if src_df_sorted.equals(tgt_df_sorted) else "Mismatch"
    print(f"Data Validation → {data_val_status}")
    print("\n ↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪↪")

    # 7. Write summary CSV
    with open(f"outputs/{output_report_file_name}", 'a', encoding="utf-8") as f:
        f.write(f"{src_schema}.{src_table},{tgt_database}.{tgt_schema}.{tgt_table},{src_row_count},{tgt_row_count},{count_val_status},{schema_val_status},{dtype_val_status},{null_val_status},{data_val_status}\n")

    # 8. Full report with datacompy
    src_df = src_df.copy()
    tgt_df = tgt_df.copy()
    comp = datacompy.Compare(
        df1=src_df_sorted,
        df2=tgt_df_sorted,
        join_columns=[src_id],
        df1_name="MySQL",
        df2_name="Snowflake"
    )

    output_compare_file_name = f"outputs/{tgt_table}_datacompy_report.txt"
    os.makedirs("outputs", exist_ok=True)
    with open(output_compare_file_name, 'w', encoding="utf-8") as f:
        f.write(comp.report())
        f.close()
    # print(comp.report())


def main():
    """
    Main execution function.
    Loads table configurations from YAML,
    fetches data from MySQL and Snowflake,
    and compares source vs. target tables.
    """
    mysql_conn = get_mysql_connection()
    sf_conn = get_snowflake_connection()

    os.makedirs("outputs", exist_ok=True)
    with open(f"outputs/{output_report_file_name}", 'w', encoding="utf-8") as f:
        f.write("Source Table,Target Table,Source Count,Target Count,Count Match,Schema Match,DataType Match,Null Value Match,Data Match\n")
        f.close()
    
    with open("dimension_schema.yaml", "r") as f:
        schema_config = yaml.safe_load(f)

    for config in schema_config["table_map"]:
        src = config["source"]
        tgt = config["target"]

        src_schema = src.get("schema") or src.get("database")

        src_filter = src.get("filter")
        tgt_filter = tgt.get("filter")

        if not src_filter or not tgt_filter:
            print(f"No filters applied for {src['schema']}.{src['table']} or {tgt['schema']}.{tgt['table']}. This may lead to count full table.")

        src_cnt = fetch_count_mysql(mysql_conn, src_schema, src["table"], src_filter)
        tgt_cnt = fetch_count_snowflake(sf_conn, tgt["database"], tgt["schema"], tgt["table"], tgt_filter)

        src_df = fetch_dataframe_mysql(mysql_conn, src_schema, src["table"], src["id"], src_filter)
        tgt_df = fetch_dataframe_snowflake(sf_conn, tgt["database"], tgt["schema"], tgt["table"], tgt["id"], tgt_filter)

        compare_dataframes(
            src_df=src_df, 
            tgt_df=tgt_df, 
            src_cnt=src_cnt, 
            tgt_cnt=tgt_cnt, 
            src_table=src["table"],
            src_id=src["id"], 
            tgt_table=tgt["table"], 
            src_schema=src_schema, 
            tgt_database=tgt["database"], 
            tgt_schema=tgt["schema"],
            tgt_id=tgt["id"]
            )

    mysql_conn.close()
    sf_conn.close()

if __name__ == "__main__":
    main()