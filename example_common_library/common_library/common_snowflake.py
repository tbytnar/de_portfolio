from typing import List
import common_library.common_aws as common_aws
from snowflake import connector
import logging
from snowflake.sqlalchemy import URL
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, inspect
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
import uuid
from snowflake.connector.pandas_tools import pd_writer
import traceback

log = logging.getLogger(__name__)


def ConvertTimestampToSQLDateTime(value):
    """
    Converts a Python datetime object into a Snowflake-compatible SQL timestamp format.

    Parameters:
    ----------
    value : datetime
        The datetime object to be converted.

    Functionality:
    --------------
    - Converts a `datetime` object into a string formatted as `YYYY-MM-DD HH:MM:SS`.
    - Ensures compatibility with Snowflake's `TIMESTAMP_NTZ` format.

    Returns:
    -------
    str
        A formatted timestamp string compatible with Snowflake.

    Example:
    --------
    ```python
    timestamp_str = ConvertTimestampToSQLDateTime(datetime(2024, 3, 1, 14, 30, 0))
    print(timestamp_str)  # Output: "2024-03-01 14:30:00"
    ```
    """

    return str(datetime.strftime(value, "%Y-%m-%d %H:%M:%S"))


def GetConnectionURL(database: str, schema: str):
    """
    Generates a Snowflake connection URL using stored credentials.

    Parameters:
    ----------
    database : str
        The target Snowflake database.

    schema : str
        The target schema within the database.

    Functionality:
    --------------
    - Retrieves Snowflake credentials from AWS Secrets Manager.
    - Constructs a connection URL for SQLAlchemy using the stored credentials.
    - Ensures authentication parameters (user, password, role, warehouse) are included.

    Returns:
    -------
    str
        A Snowflake connection URL string.

    Raises:
    -------
    RuntimeError
        If credentials cannot be retrieved from AWS Secrets Manager.

    Example:
    --------
    ```python
    conn_url = GetConnectionURL("MY_DATABASE", "PUBLIC")
    print(conn_url)
    ```
    """

    log.info(f"Generating Connection URL for database: {database}  schema: {schema}")
    return URL(
        user=common_aws.GetSecretString("SNOWFLAKE_USER"),
        password=common_aws.GetSecretString("SNOWFLAKE_PASSWORD"),
        account=common_aws.GetSecretString("SNOWFLAKE_ACCOUNT"),
        warehouse=common_aws.GetSecretString("SNOWFLAKE_WAREHOUSE"),
        database=database,
        schema=schema,
        role=common_aws.GetSecretString("SNOWFLAKE_ROLE"),
        arrow_number_to_decimal=True,
    )


def GetConnection(database: str, schema: str):
    """
    Establishes a connection to Snowflake using stored credentials.

    Parameters:
    ----------
    database : str
        The target Snowflake database.

    schema : str
        The target schema within the database.

    Functionality:
    --------------
    - Retrieves credentials from AWS Secrets Manager.
    - Creates a Snowflake connection using the Snowflake connector.
    - Returns an active database connection object.

    Returns:
    -------
    snowflake.connector.connection
        A Snowflake database connection object.

    Raises:
    -------
    RuntimeError
        If authentication fails or credentials are missing.

    Example:
    --------
    ```python
    conn = GetConnection("MY_DATABASE", "PUBLIC")
    ```
    """

    return connector.connect(
        user=common_aws.GetSecretString("SNOWFLAKE_USER"),
        password=common_aws.GetSecretString("SNOWFLAKE_PASSWORD"),
        account=common_aws.GetSecretString("SNOWFLAKE_ACCOUNT"),
        warehouse=common_aws.GetSecretString("SNOWFLAKE_WAREHOUSE"),
        database=database,
        schema=schema,
        role=common_aws.GetSecretString("SNOWFLAKE_ROLE"),
    )


def ExecuteQuery(sql_query: str, database: str, schema: str, params=None, as_pandas: bool = False):
    """
    Executes a SQL query against a Snowflake database.

    Parameters:
    ----------
    sql_query : str
        The SQL query to be executed.

    database : str
        The target Snowflake database.

    schema : str
        The target schema within the database.

    params : dict, optional
        Query parameters for parameterized execution.

    as_pandas : bool, optional
        If True, returns the results as a Pandas DataFrame.

    Functionality:
    --------------
    - Establishes a connection to Snowflake.
    - Executes the query using the provided parameters (if any).
    - Returns results as either a list of tuples or a Pandas DataFrame.
    - Closes the connection after execution.

    Returns:
    -------
    list or pd.DataFrame
        Query results as a list of tuples or a DataFrame (if `as_pandas=True`).

    Raises:
    -------
    RuntimeError
        If query execution fails.

    Example:
    --------
    ```python
    results = ExecuteQuery("SELECT * FROM users", "MY_DATABASE", "PUBLIC", as_pandas=True)
    print(results.head())
    ```
    """

    with GetConnection(database=database, schema=schema) as ctx:
        cursor = ctx.cursor()
        log.info(f"Executing Query:\n {sql_query}")
        cursor.execute(sql_query, params)
        if as_pandas:
            result = cursor.fetch_pandas_all()
        else:
            result = cursor.fetchall()
        cursor.close()
        ctx.close()
        return result


def ExecuteSQLtoPandas(sql_query: str, database: str, schema: str, params=None):
    """
    Executes a SQL query and returns results as a Pandas DataFrame.

    Parameters:
    ----------
    sql_query : str
        The SQL query to be executed.

    database : str
        The target Snowflake database.

    schema : str
        The target schema within the database.

    params : dict, optional
        Query parameters for parameterized execution.

    Functionality:
    --------------
    - Uses SQLAlchemy to establish a Snowflake connection.
    - Executes the query and loads results into a Pandas DataFrame.
    - Ensures column names are converted to uppercase for consistency.

    Returns:
    -------
    pd.DataFrame
        The query results in a Pandas DataFrame.

    Raises:
    -------
    RuntimeError
        If query execution fails.

    Example:
    --------
    ```python
    df = ExecuteSQLtoPandas("SELECT * FROM sales", "MY_DATABASE", "PUBLIC")
    print(df)
    ```
    """

    sf_url = GetConnectionURL(database=database, schema=schema)
    engine = create_engine(sf_url, connect_args={"session_parameters": {"QUOTED_IDENTIFIERS_IGNORE_CASE": False}})
    log.info(f"Executing Query:\n {sql_query}")
    try:
        # NOTE: NULLs will return as <NA>.  Use this to fill them with blanks (Nonetype): df = df.replace({float('nan'): None})
        log.warning("Dataframe Results: NULLs will return as <NA>.  Use this to fill them with blanks (Nonetype): df = df.replace({float('nan'): None})")

        df = pd.read_sql(sql_query, engine, dtype_backend="pyarrow")
        df.rename(columns=str.upper, inplace=True)
        return df
    except Exception as e:
        print(e)


def snowflakeQueries(queries):
    """
    Executes one or multiple SQL queries in Snowflake.

    Parameters:
    ----------
    queries : str or list
        A single SQL query string or a list of queries to be executed.

    Functionality:
    --------------
    - Establishes a connection to Snowflake using stored credentials.
    - Executes the provided SQL queries one by one.
    - If `queries` is a string, it is converted into a list.
    - Ensures each query ends with a semicolon (`;`) for proper execution.
    - Logs execution details for debugging purposes.
    - Returns the query results as a list of tuples.

    Returns:
    -------
    list
        A list of tuples containing query execution results.

    Raises:
    -------
    RuntimeError
        If query execution fails.

    Example:
    --------
    ```python
    results = snowflakeQueries([
        "SELECT COUNT(*) FROM sales;",
        "SELECT MAX(order_date) FROM orders;"
    ])
    print(results)
    ```
    """

    if type(queries) is str:
        queries = [queries]

    results = []
    with connector.connect(
        user=common_aws.GetSecretString("SNOWFLAKE_USER"),
        password=common_aws.GetSecretString("SNOWFLAKE_PASSWORD"),
        account=common_aws.GetSecretString("SNOWFLAKE_ACCOUNT"),
        warehouse=common_aws.GetSecretString("SNOWFLAKE_WAREHOUSE"),
        database=common_aws.GetSecretString("SNOWFLAKE_DATABASE"),
        schema="SCHEMA",
        role=common_aws.GetSecretString("SNOWFLAKE_ROLE"),
    ) as ctx:
        cursor = ctx.cursor()
        for query in queries:
            if query[-1] != ";":
                query = f"{query};"
            log.info(f"DEBUG - Executing Query:  {query}")
            results.append(cursor.execute(query).fetchall())
    return results


def generate_create_table_sql(database, schema, table_name, dataframe):
    """
    Generates a `CREATE TABLE` SQL statement based on the schema of a Pandas DataFrame.

    Parameters:
    ----------
    database : str
        The name of the target Snowflake database.

    schema : str
        The name of the target schema.

    table_name : str
        The name of the table to be created.

    dataframe : pd.DataFrame
        The DataFrame whose column names and data types will define the table schema.

    Functionality:
    --------------
    - Analyzes the DataFrame's columns and their data types.
    - Maps Pandas data types to Snowflake SQL data types:
        - `int` → `NUMBER`
        - `float` → `FLOAT`
        - `string` → `VARCHAR(16777216)`
        - `datetime` → `TIMESTAMP_NTZ`
        - `bool` → `BOOLEAN`
    - Includes additional metadata fields:
        - `INSERTED_ON TIMESTAMP_NTZ`
        - `UPDATED_ON TIMESTAMP_NTZ`
        - `UPDATED_BY VARCHAR(16777216)`
    - Constructs and logs the final `CREATE TABLE` SQL statement.

    Returns:
    -------
    str
        A Snowflake `CREATE TABLE` SQL statement.

    Example:
    --------
    ```python
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
    create_sql = generate_create_table_sql("MY_DATABASE", "PUBLIC", "users", df)
    print(create_sql)
    ```
    """

    columns_sql = []
    for column_name, dtype in zip(dataframe.columns, dataframe.dtypes):
        # Map pandas/numpy dtypes to Snowflake SQL data types
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "NUMBER"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        elif pd.api.types.is_string_dtype(dtype):
            sql_type = "VARCHAR(16777216)"  # Max size for Snowflake VARCHAR
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMP_NTZ"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        else:
            sql_type = "VARCHAR(16777216)"  # Default to VARCHAR for other types

        if " " in column_name or column_name.isupper() is False:
            column_name = f'"{column_name}"'

        columns_sql.append(f"{column_name} {sql_type}")

    columns_sql.append("INSERTED_ON TIMESTAMP_NTZ")
    columns_sql.append("SOURCE_DATA VARCHAR(16777216)")
    columns_sql.append("UPDATED_ON TIMESTAMP_NTZ")
    columns_sql.append("UPDATED_BY VARCHAR(16777216)")

    columns_sql_str = ",\n    ".join(columns_sql)

    create_table_sql = f"""
    CREATE TABLE {database}.{schema}.{table_name.upper()} (
        {columns_sql_str}
    );
    """
    log.info(create_table_sql)
    return create_table_sql


def DataframeToSnowflake(
    target_database: str,
    target_schema: str,
    target_table: str,
    primary_key: str,
    dataframe: pd.DataFrame,
    operation: str = "upsert",
    include_metrics: bool = False,
    source_data: str = None,
    chunk_size: int = 1000,
    target_ddl: str = None,
    debug_mode: bool = False,
    ignore_autoincrementing: bool = True,
):
    """
    Inserts or updates a Pandas DataFrame into a Snowflake table.

    Parameters:
    ----------
    target_database : str
        The database where the table resides.

    target_schema : str
        The schema containing the target table.

    target_table : str
        The name of the target table.

    primary_key : str
        The primary key column(s) used for `upsert` operations.

    dataframe : pd.DataFrame
        The DataFrame containing data to be inserted or updated.

    operation : str, optional
        The type of operation (`"upsert"`, `"append"`, `"replace"`).

    include_metrics : bool, optional
        If True, includes timestamp and source metadata columns.

    source_data : str, optional
        The source file or dataset name.

    chunk_size : int, optional
        The batch size for data insertion.

    target_ddl : str, optional
        The DDL statement to create the table if it doesn't exist.

    debug_mode : bool, optional
        If True, retains temporary tables for debugging.

    ignore_autoincrementing : bool, optional
        If True, ignores auto-incrementing columns during insertion.

    Functionality:
    --------------
    - Establishes a connection to Snowflake using SQLAlchemy.
    - Checks if the table exists; if not, creates it using `target_ddl`.
    - Supports three operations:
        - `"upsert"`: Inserts new rows and updates existing ones based on the primary key.
        - `"append"`: Appends data to the table without modifying existing records.
        - `"replace"`: Truncates and replaces the table with new data.
    - Converts Pandas `datetime64[ns]` columns to Snowflake-compatible timestamp formats.
    - Uses Snowflake's `MERGE` statement for `upsert` operations.
    - Logs query execution and data insertion status.

    Returns:
    -------
    None

    Raises:
    -------
    RuntimeError
        If the data insertion or update process fails.

    Example:
    --------
    ```python
    DataframeToSnowflake("MY_DATABASE", "PUBLIC", "sales", "sale_id", df, operation="upsert")
    ```
    """

    # Validate data_type parameter
    if operation not in ["upsert", "append", "replace"]:
        raise ValueError("data_type must be 'upsert', 'append', or 'replace'.")

    # Snowflake does NOT like the datetime64[ns] data type.  Convert all instances of that here.
    # for col in dataframe.select_dtypes(include=["datetime64[ns]"]).columns:
    #     dataframe[col] = dataframe[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Create connection URL
    sf_url = GetConnectionURL(database=target_database, schema=target_schema)

    # Check if the target table exists
    try:
        engine = create_engine(sf_url)
        connection = engine.connect()
        inspector = inspect(connection)
        tables = inspector.get_table_names(schema=target_schema)
        log.info(f"Tables Found: {tables}")
        table_exists = target_table.lower() in tables
    except Exception as e:
        log.error(traceback.format_exc())
        raise RuntimeError(f"Error executing operation: {e}")
    finally:
        connection.close()
        engine.dispose()

    target_table = target_table.upper()

    if not table_exists:
        # Table does not exist, create it
        if target_ddl:
            create_table_sql = target_ddl
        else:
            create_table_sql = generate_create_table_sql(target_database, target_schema, target_table, dataframe)
        ExecuteQuery(sql_query=create_table_sql, database=target_database, schema=target_schema)
        log.info(f"Created table {target_database}.{target_schema}.{target_table}")

    if operation == "replace" or operation == "append":
        # Table does not exist or replace requested, create it
        log.info("Table does exist or operation replace was specified.")
        if operation == "replace" and table_exists:
            ExecuteQuery(sql_query=f'TRUNCATE TABLE {target_database}.{target_schema}."{target_table}";', database=target_database, schema=target_schema)
            log.info(f"Truncated table {target_database}.{target_schema}.{target_table}")

        # Set metrics
        if include_metrics:
            dataframe["INSERTED_ON"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dataframe["SOURCE_DATA"] = source_data
            dataframe["UPDATED_ON"] = None
            dataframe["UPDATED_BY"] = None

        # Log DataFrame contents for debugging
        log.info(f"DataFrame Top 3 Rows:\n{dataframe.head(3).to_string()}")
        log.info(f"Data will be inserted into {target_database}.{target_schema}.{target_table}")

        # Write the dataframe to Snowflake
        try:
            engine = create_engine(sf_url)
            connection = engine.connect()
            sfd = SnowflakeDialect()
            columns = sfd.get_columns(connection=connection, schema=target_schema, table_name=target_table)

            if ignore_autoincrementing:
                for index, item in enumerate(columns):
                    if item["autoincrement"]:
                        log.info(f"Autoincremening column: {item['name']} found.  Removing from column list to match dataframe.")
                        columns.pop(index)

            denorm_columns = [sfd.denormalize_name(col["name"]) for col in columns]
            dataframe.columns = denorm_columns

            log.info(f"Denormalized TableName = {target_table}")

            dataframe.to_sql(name=target_table, con=engine, index=False, if_exists="append", method=pd_writer)
        except Exception as e:
            log.error(traceback.format_exc())
            if "Length mismatch" in str(e):
                engine = create_engine(sf_url)
                connection = engine.connect()
                inspector = inspect(connection)
                table_columns = inspector.get_columns(table_name=target_table.lower(), schema_name=target_schema)
                table_columns_list = [column["name"] for column in table_columns]
                table_uniques = [item for item in table_columns_list if item not in list(dataframe.columns)]
                dataframe_uniques = [item for item in dataframe.columns if item.lower() not in table_columns_list]
                log.error(f"Live Table has {len(table_columns_list)} columns but dataframe has {len(dataframe.columns)} columns.")
                log.error(f"Live Table Missing Columns: {dataframe_uniques}")
                log.error(f"Dataframe Missing Columns: (NOTE: Autoincrementing Columns Are Excluded Purposely) {table_uniques}")
                raise RuntimeError(f"Error executing operation: {e}")
            else:
                raise RuntimeError(f"Error executing operation: {e}")
        finally:
            if connection:
                connection.close()
                engine.dispose()

        # sf_conn = GetConnection(database=target_database, schema=target_schema)
        # write_pandas(conn=sf_conn, df=dataframe, database=target_database, schema=target_schema, table_name=target_table)
        # sf_conn.close()
    elif operation == "upsert":
        # Perform upsert operation
        if not primary_key:
            raise ValueError("Primary key must be specified for upsert operation.")

        # Generate temporary table name based on a guid
        temp_table_name = f"{target_table}_temp_{str(uuid.uuid4())}"

        # Generate UPDATE portion of the SQL MERGE query
        columns = dataframe.columns.tolist()
        columns = [f'"{x}"' if any(char in x for char in [" ", "#", "?"]) or not x.isupper() else x for x in columns]

        if include_metrics:
            update_columns = ", ".join([f"target.{col} = source.{col}" for col in columns])
            update_columns = update_columns + ", target.UPDATED_ON = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), target.UPDATED_BY = 'SYSTEM'"
            insert_columns = ", ".join(columns)
            insert_columns = insert_columns + ", INSERTED_ON, SOURCE_DATA"
            insert_values = ", ".join([f"source.{col}" for col in columns])
            insert_values = insert_values + f", TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), '{source_data}'"
        else:
            update_columns = ", ".join([f"target.{col} = source.{col}" for col in columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

        primary_keys = [k.strip() for k in primary_key.split(",")]

        primary_keys = [f'"{k}"' if any(char in k for char in [" ", "#", "?"]) or not k.isupper() else k for k in primary_keys]
        primary_key = f'"{primary_key}"' if any(char in primary_key for char in [" ", "#", "?"]) or not primary_key.isupper() else primary_key

        if len(primary_keys) > 1:
            using_statement = f'USING {target_database}.{target_schema}."{temp_table_name}" source ON '
            using_statement += " AND ".join([f"IFNULL(target.{k}, 'NULL') = IFNULL(source.{k}, 'NULL')" for k in primary_keys])
        else:
            using_statement = f'USING {target_database}.{target_schema}."{temp_table_name}" source ON target.{primary_key} = source.{primary_key}'

        # Full MERGE query
        merge_query = f"""
            MERGE INTO {target_database}.{target_schema}.{target_table} target
            {using_statement}
                WHEN MATCHED THEN UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});
        """

        # Log DataFrame contents and the generated query for debugging
        log.info(f"DataFrame Top 3 Rows:\n{dataframe.head(3).to_string()}")
        log.info(f"Generated Merge Query:\n{merge_query}")

        # Snowflake-ify the dataframe's columns
        # dataframe.columns = columns

        # Upload DataFrame to temporary table
        # dataframe.to_sql(
        #     name=temp_table_name,
        #     con=conn,
        #     schema=target_schema,
        #     if_exists="replace",  # Replace if the table exists
        #     index=False,  # Do not write the DataFrame's index
        #     chunksize=chunk_size,  # Use chunking for large datasets
        # )

        # Execute the merge inside a transaction block
        # sf_conn = GetConnection(database=target_database, schema=target_schema)
        # write_pandas(conn=sf_conn, df=dataframe, database=target_database, schema=target_schema, table_name=temp_table_name, auto_create_table=True)
        # cursor = sf_conn.cursor()

        # Write the dataframe to Snowflake
        log.info(f"Creating and inserting data into temp table: {temp_table_name}")
        try:
            engine = create_engine(sf_url)
            connection = engine.connect()
            # sfd = SnowflakeDialect()
            # columns = sfd.get_columns(connection=connection, schema=target_schema, table_name=target_table)

            # denorm_columns = [sfd.denormalize_name(col["name"]) for col in columns]
            # dataframe.columns = denorm_columns

            # dataframe.to_sql(name=temp_table_name, con=engine, index=False, if_exists="replace", method=pd_writer)
            dataframe.to_sql(
                name=temp_table_name,
                con=connection,
                schema=target_schema,
                if_exists="replace",  # Replace if the table exists
                index=False,  # Do not write the DataFrame's index
                chunksize=chunk_size,  # Use chunking for large datasets
            )
        except Exception as e:
            log.error(traceback.format_exc())
            raise RuntimeError(f"Error executing operation: {e}")
        finally:
            connection.close()
            engine.dispose()

        # Execute merge statement to upsert the dataframe data
        log.info(f"Executing merge statement.  {len(dataframe)} records will be UPSERTED in to {target_database}.{target_schema}.{target_table}")
        try:
            engine = create_engine(sf_url)
            connection = engine.connect()
            trans = connection.begin()
            trans.connection.execute("ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = False;")
            trans.connection.execute(merge_query)
            if not debug_mode:
                trans.connection.execute(f'DROP TABLE IF EXISTS {target_database}.{target_schema}."{temp_table_name}";')
            trans.commit()
        except Exception as e:
            trans.rollback()
            log.error(traceback.format_exc())
            raise RuntimeError(f"Error executing operation: {e}")
        finally:
            connection.close()
            engine.dispose()
    else:
        # This else block is redundant due to earlier validation but kept for completeness
        raise ValueError("Invalid data_type. Must be 'upsert', 'append', or 'replace'.")

    log.info("Operation completed successfully.")
