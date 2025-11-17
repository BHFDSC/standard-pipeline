"""
Module name: table_management.py

This module provides functions for managing tables within a PySpark environment.

Functions:
- load_table: Load a table from Spark, optionally filtering by archive date and standardising the columns.
- save_table: Save a DataFrame to a database table.
- get_archive_versions: Get list of unique archive versions
- standardise_table: Standardise the DataFrame according to the specified method, including renaming person ID variables,
    formatting dates into yyyy-MM-dd format, and cleaning column names.
"""
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from typing import List
from .json_utils import read_json_file
from .environment_utils import get_spark_session
from .data_wrangling import clean_column_names

def load_table(table: str, table_directory: str = None, method: str = None) -> DataFrame:
    """
    Load a table from Spark, optionally filtering by archive date and standardising the data.

    Args:
        table (str): The name of the table to load.
        table_directory (str): Path to the JSON file containing table directories.
        method (str): The method used to standardise the table for column names (e.g. `person_id`).
            See `standardise_table` function for description.

    Returns:
        pyspark.sql.DataFrame: The loaded and optionally standardised DataFrame.

    Example:
        # Load table GDPPR with column names standardised
        df = load_table('gdppr', method='gdppr')
    """

    # Load table directory from JSON file
    if table_directory is None:
        table_directory = read_json_file('./config/table_directory.json')
    elif isinstance(table_directory, str):
        table_directory = read_json_file(table_directory)
    else:
        raise ValueError("table_directory should be a string or None.")

    # Check table key exists
    assert table in table_directory.keys(), f"Table key '{table}' not found in table_directory"

    # Read parameters from table_directory
    database = table_directory[table]['database']
    table_name = table_directory[table]['table_name']
    archive_date = table_directory[table].get('archive_date', None)
    max_archive_date = table_directory[table].get('max_archive_date', None)

    # Assert that only one archive version filter is specified, if any
    assert (archive_date is None and max_archive_date is None) or \
       (archive_date is not None and max_archive_date is None) or \
       (archive_date is None and max_archive_date is not None), \
       "Only one of 'archive_date' or 'max_archive_date' can be specified."

    # Get spark session
    spark = get_spark_session() 

    # Load table
    df = spark.table(f"{database}.{table_name}")

    # Filter for archive date
    if archive_date == 'latest':
        archive_date_max = (
            df
            .agg(f.max('archived_on').alias('_max_archive_date'))
            .collect()[0][0]
        )
        df = df.filter(f.col('archived_on') == f.lit(archive_date_max))
    elif archive_date is not None:
        df = df.filter(f.col('archived_on') == f.lit(archive_date))
    elif max_archive_date is not None:
        df = df.filter(f.col('archived_on') <= f.lit(max_archive_date))

    # Standardise table
    if method is not None:
        df = standardise_table(df, method=method)

    return df


def save_table(df, table: str, table_directory=None, partition_by=None) -> None:
    """
    Saves a DataFrame to a database table.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to be saved.
        table (str): The name of the table to save.
        table_directory (str): Path to the JSON file containing table directories.
        partition_by (str or list[str], optional): Columns to partition by.

    Returns:
        None
    """
    # Load table directory from JSON file
    if table_directory is None:
        table_directory = read_json_file('./config/table_directory.json')
    elif isinstance(table_directory, str):
        table_directory = read_json_file(table_directory)
    else:
        raise ValueError("table_directory should be a string or None.")

    # Check table key exists
    assert table in table_directory.keys(), f"Table key '{table}' not found in table_directory"

    # Ensure partition_by is either a string or a list of strings
    assert partition_by is None or isinstance(partition_by, str) or all(isinstance(col, str) for col in partition_by), \
        "partition_by should be a string or a list of strings."

    # Read parameters from table_directory
    database = table_directory[table]['database']
    table_name = table_directory[table]['table_name']

    # Write table to database
    if partition_by is not None:
        if isinstance(partition_by, str):
            partition_by = [partition_by]  # Convert string to list
        df.write.mode('overwrite').option('overwriteSchema', 'true').partitionBy(*partition_by).saveAsTable(f"{database}.{table_name}")

    else:
        df.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f"{database}.{table_name}")


def get_archive_versions(df: DataFrame, version_column: str = 'archived_on') -> List[str]:
    """
    Get distinct version archive dates from the DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame.
        version_column (str): The name of the column containing version information. Default is 'archived_on'.

    Returns:
        list[str]: A list of distinct archive versions.
    """
    return list(
        df
        .select(f.col(version_column).cast('string'))  
        .distinct()  
        .orderBy(version_column) 
        .toPandas()[version_column] 
    )

def standardise_table(df, method):
    """
    Standardise the DataFrame according to the specified method. In general, these include the following steps:
    - Renaming Person ID Variables: Rename variables representing person IDs to 'person_id' for consistency across datasets.
    - Date Formatting: Format date variables into yyyy-MM-dd format to ensure uniformity and facilitate analysis.
    - Cleaning Column Names: Standardise column names by converting them to lowercase and replacing spaces with underscores.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to be standardised.
        method (str): The method used for standardization. Valid methods include: 
            'deaths', 'gdppr', 'hes_apc', 'hes_op', 'hes_ae', 'pillar_2', 'ssnap', 'sgss', 'vaccine_status'.

    Returns:
        pyspark.sql.DataFrame: The standardised DataFrame.
    """

    method_functions = {
        'deaths': standardise_deaths_table,
        'gdppr': standardise_gdppr_table,
        'hes_apc': standardise_hes_apc_table,
        'hes_op': standardise_hes_op_table,
        'hes_ae': standardise_hes_ae_table,
        'pillar_2': standardise_pillar_2_table,
        'ssnap': standardise_ssnap_table,
        'sgss': standardise_sgss_table,
        'vaccine_status': standardise_vaccine_status_table,
        'primary_care_meds': standardise_primary_care_meds_table,
        'chess': standardise_chess_table
    }
    
    if method not in method_functions:
        raise ValueError(
            f"'{method}' is not a recognised standardise_table method. "
            f"Available methods: deaths, gdppr, hes_apc, hes_op, hes_ae, pillar_2, ssnap, sgss, vaccine_status, primary_care_meds, chess"
        )
    
    return method_functions[method](df)


def standardise_deaths_table(df):
    return(
        df
        .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id')
        .withColumnRenamed('REG_DATE_OF_DEATH', 'date_of_death')
        .transform(clean_column_names)
        .withColumn('reg_date', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
        .withColumn(
            'date_of_death',
            f.when(
                f.col('date_of_death').rlike('\d{8}'),
                f.to_date(f.col('date_of_death'), 'yyyyMMdd')
            )
        )
    )

def standardise_gdppr_table(df):
    return(
        df
        .withColumnRenamed('NHS_NUMBER_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_hes_apc_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_hes_op_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_hes_ae_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_pillar_2_table(df):
    return(
        df
        .withColumnRenamed('Person_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_ssnap_table(df):
    return(
        df
        .withColumnRenamed('Person_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_sgss_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_vaccine_status_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)
        .withColumn('recorded_date', f.to_date(f.col('recorded_date'), 'yyyyMMdd'))
        .withColumn('expiry_date', f.to_date(f.col('expiry_date'), 'yyyyMMdd'))
        .withColumn('date_and_time', f.to_timestamp(f.col('date_and_time'), "yyyyMMdd'T'HHmmssSS"))
    )

def standardise_primary_care_meds_table(df):
    return(
        df
        .withColumnRenamed('Person_ID_DEID', 'person_id')
        .transform(clean_column_names)
    )

def standardise_chess_table(df):
    return(
        df
        .withColumnRenamed('PERSON_ID_DEID', 'person_id')
        .transform(clean_column_names)

    )
