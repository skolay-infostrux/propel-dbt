# MySQL - Snowflake Compare Tables

## Overview

As per current design, Propel has decided to ingest data from source databases (MS SQL) via Openflow streams and for transformations, Snowflake stored procedures would be used to populate dimension models. Ingestion testing is not covered and assumption is Openflow jobs or by other means, source should be available in Snowflake Landing area to test both full load and incremental loads. After every single sync, SPs should ensure all the Snowflake models are populated with transformed datasets.

Since testing the ingestion procedure is not in scope, assumption is source data in the landing zone should be complete and no  duplicated entries would be present. 

Data reconciliation tests are automated scripts by writing in python. 
   1. Compare landing tables between MySQL source and SF target
   2. Compare dimension tables between MySQL source and SF target
   3. Compare SF tables (between different schemas).
   4. Check validation issues accordingly with specific business rules. 

This project provides list of Python scripts (
   `compare_mysql_sf_landing_tables.py`, 
   `compare_mysql_sf_dimension_tables.py`,
   `compare_snowflake_tables_record_counts.py`,
   `custom_validation_issues.py`
   ) that:

1. Connects to Snowflake/MySql using credentials from environment variables.
2. Loads data from a source/target landing schema and table into a pandas DataFrame by landing_schema.yaml.
3. Loads data from a source/target dimension schema and table into a pandas DataFrame by dimension_schema.yaml.
4. Run data compare and validation checks.
5. Performs a full DataFrame report equality check as a log output and as a file at (
   `outputs/Landing_validation_report.csv`,
   `outputs/Dimension_validation_report.csv`,
   `outputs/{dimension_table_name}_datacompy_report.txt`,
   `outputs/SF_Table_Counts_Check_Report.csv`,
   `outputs/Custom_Validation_Output.txt`
)

## Pre-requisites

* Python 3.8 or higher
* `pip` for package installation
* Access to a Snowflake account with:

  * Username, authenticator
  * Account, Warehouse, database, and schema.

* Access to a MySql account with:

  * Host, User
  * Password and database.

* **Don't forgot to create an enviroment before the following next steps ahead**: 
   - [Link](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/)

## Setup

1. **Clone the repository** (or copy files locally):

   ```bash
   git clone <repo-url>
   cd <project-folder>
   ```

2. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables creating a new file as .env** (replace placeholder values):

   ```bash
   export SNOWFLAKE_USER="your_user"
   export SNOWFLAKE_AUTHENTICATOR="your_authenticator"
   export SNOWFLAKE_ACCOUNT="your_account_id"
   export SNOWFLAKE_WAREHOUSE="your_warehouse"
   export SNOWFLAKE_DATABASE="your_database"
   export SNOWFLAKE_SCHEMA="your_schema"
   export SNOWFLAKE_ROLE="your_role"

   export MYSQL_HOST=YOUR_HOST
   export MYSQL_USER=YOUR_USER
   export MYSQL_PASSWORD=YOUR_PASSWORD
   ```

## Running the Script

With your variables set, simply run:


For loading variables:
```bash
source .env
```

For compare-validate landing tables from MySQL to SF:
```bash
python or python3 compare_mysql_sf_landing_tables.py
```

For compare-validate dimension tables from MySQL to SF:
```bash
python or python3 compare_mysql_sf_dimension_tables.py
```

For compare-validate SF tables:
```bash
python or python3 compare_snowflake_tables_record_counts.py input.csv
```

For output custom validations accordingly with business rules:
```bash
python or python3 custom_validation_issues.py
```

The script will:

1. Establish a Snowflake/MySql connection.
2. Load the source and target tables into pandas DataFrames.
3. Print a log output and save a report for validation checks.
4. Compare DataFrames equalities.

## Files

* `outputs\`: folder to save reports from landing and dimension scripts
* `compare_mysql_sf_landing_tables.py`: Main Python script for check landing table validation between MySql and SF tables.
* `compare_mysql_sf_dimension_tables.py`: Main Python script for check dimension table validation between MySQL SF tables.
* `compare_snowflake_tables_record_counts.py`: Compare-Validate differences between snowflake schemas.
* `custom_validation_issues.py`: Output log with check information validation accordingly business rules defined.
* `landing_schema.yaml`: Schema variables for landing tables
* `dimension_schema.yaml`: Schema variables for dimension tables
* `requirements.txt`: Lists `pandas`, `snowflake-connector-python`, `mysql-connector-python`, `pyyaml` and `python-dotenv` and so on...
* `README.md`: This documentation.

## Troubleshooting

* **Connection errors**: Check that all Snowflake and MySQL environment variables are correct.
* **Import errors**: Ensure you've installed the requirements.txt:

  ```bash
  pip install -r requirements.txt
  ```
