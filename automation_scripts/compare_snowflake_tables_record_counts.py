import os
import sys
import csv
import warnings
import pandas as pd
from snowflake.connector import connect

# Suppress warning
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

output_report_file_name = "Dimension_Validation_SF_Tables_Report.csv"

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
        role=os.getenv("SNOWFLAKE_ROLE")
    )


def fetch_dataframe(conn, query):
    """
    Executes an SQL query and returns the results as a DataFrame.

    Args:
    conn: connection to the database.
    query (str): SQL command.

    Returns:
    pd.DataFrame: query result.
    """
    return pd.read_sql(query, conn)


def check_counts(sf_conn, source , target):
    """
    Performs reconciliation between MySQL and Snowflake tables with validations.

    Args:
    schema (dict): configuration dictionary loaded from YAML.
    """

    src_df = fetch_dataframe(sf_conn, f"SELECT COUNT(*) FROM {source}")
    tgt_df = fetch_dataframe(sf_conn, f"SELECT COUNT(*) FROM {target}")

    print(f"\n⏳ Comparing: {source} → {target}")

    # 1. Row Count Match
    src_row_count = src_df.iloc[0, 0]
    tgt_row_count = tgt_df.iloc[0, 0]

    count_val_status = "Match" if src_row_count == tgt_row_count else "Mismatch"
    print(f"Row Count → Source: {src_row_count}, Target: {tgt_row_count} → {count_val_status}")


    # 7. Write summary CSV
    with open(f"outputs/{output_report_file_name}", 'a', encoding="utf-8") as f:
        f.write(f"{target},{src_row_count},{tgt_row_count},{count_val_status}\n")


if __name__ == "__main__":
    # Establish connection
    sf_conn = get_snowflake_connection()

    os.makedirs("outputs", exist_ok=True)
    with open(f"outputs/{output_report_file_name}", 'w', encoding="utf-8") as f:
        f.write("DWH-Table,Source Count,Target Count,Count Status \n")
        f.close()

    with open(sys.argv[1], mode ='r') as f:
        schema_config = csv.reader(f)

        for row in schema_config:
            check_counts(sf_conn, row[0], row[1])

    sf_conn.close()
