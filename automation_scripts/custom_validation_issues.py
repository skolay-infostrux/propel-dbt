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


def validate_dim_payment_schedule_item_type(sf_conn):
    issues = []
    
    query = """
        SELECT COUNT(*) AS INVALID_ITEM_TYPES
        FROM DWH_DEV."DWH"."DIM_PAYMENT_SCHEDULE"
        WHERE item_type IS NOT NULL AND item_type NOT IN ('C', 'D');

        """
    result = pd.read_sql(query, sf_conn)

    # Check Item Types
    issues.append(f"\n Test Case: dwh.dim_payment_schedule -> item_type values should only be in C, D, NULL.")
    issues.append(f"Invalid count: item_type → {result.iloc[0]['INVALID_ITEM_TYPES']}.")

    return issues


def validate_dim_customer_gross_monthly(sf_conn):
    issues = []

    query = """
        SELECT COUNT(*) AS INVALID_GROSS_MONTHLY_VALUES
        FROM DWH_DEV."DWH"."DIM_CUSTOMER"
        WHERE grossmonthly IS NOT NULL OR NOT RLIKE(grossmonthly, '^[0-9.]');

        """
    result = pd.read_sql(query, sf_conn)

    # Check Item Types
    issues.append(f"\n Test Case: dwh.dim_customer -> grossmonthly values should only be in numbers and periods.")
    issues.append(f"Invalid count: grossmonthly → {result.iloc[0]['INVALID_GROSS_MONTHLY_VALUES']}.")

    return issues


def validate_dim_customer_paycheck_amount(sf_conn):
    issues = []

    query = """
        SELECT COUNT(*) AS INVALID_PAYCHECK_MONTHLY_VALUES
        FROM DWH_DEV."DWH"."DIM_CUSTOMER"
        WHERE paycheck_amount IS NOT NULL OR NOT RLIKE(paycheck_amount, '^[0-9.]+$');

        """
    result = pd.read_sql(query, sf_conn)

    # Check Item Types
    issues.append(f"\n Test Case: dwh.dim_customer -> paycheck_amount values should only be in numbers and periods.")
    issues.append(f"Invalid count: paycheck_amount → {result.iloc[0]['INVALID_PAYCHECK_MONTHLY_VALUES']}.")

    return issues


def main():

    sf_conn = get_snowflake_connection()

    # Custom column function validation
    column_issues = validate_dim_payment_schedule_item_type(sf_conn)
    column_issues += validate_dim_customer_gross_monthly(sf_conn)
    column_issues += validate_dim_customer_paycheck_amount(sf_conn)

    if column_issues:
        print("\n Column-level ckeck applied!")
        for issue in column_issues:
            print(f"{issue}")
            output_custom_validation = f"outputs/Custom_Validation_Output.txt"
            os.makedirs("outputs", exist_ok=True)
            with open(output_custom_validation, "w", encoding="utf-8") as f:
                for issue in column_issues:
                    f.write(f"{issue}\n")

    sf_conn.close()

if __name__ == "__main__":
    main()
