"""
Module name: csv_utils.py

Description:
    This module provides utilities for reading and writing CSV files in Python, with support for Spark DataFrame.
"""
import os
import pandas as pd
from pyspark.sql import DataFrame
from .environment_utils import get_spark_session
from .environment_utils import resolve_path

def read_csv_file(path: str, repo: str = None, keep_default_na: bool =False, **kwargs) -> DataFrame:
    """
    Read .csv file and return as Spark DataFrame
    
    Args:
    - path (str): Path to the CSV file. Can be an absolute path, a relative path with './', or a path within a repo.
    - repo (str): Name of the repo if the path is relative within a repo.
    - keep_default_na (boolean): Whether or not to include the default NaN values when parsing the data.
    - **kwargs: Additional keyword arguments to be passed to pd.read_csv()
    
    Returns:
    - spark_df (DataFrame): Spark DataFrame containing the data read from the CSV file

    Example:
        >>> read_csv_file('./relative/path/in/project.csv')
        >>> read_csv_file('/Workspace/absolute/path.csv')
        >>> read_csv_file(path='path/in/repo.csv', repo='common_repo')
    """
    # Resolve the path
    resolved_path = resolve_path(path, repo)
    
    # Read the CSV file into a Pandas DataFrame, passing additional arguments
    pandas_df = pd.read_csv(resolved_path, keep_default_na=keep_default_na, **kwargs)
    
    # Get spark session
    spark = get_spark_session()

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)
    
    return spark_df


def write_csv_file(df: DataFrame, path: str, repo: str = None, index: bool = False, max_rows_threshold: int = 1000, **kwargs) -> None:
    """
    Write a Spark DataFrame to a CSV file.

    Args:
    - df (DataFrame): The Spark DataFrame to be written to a CSV file.
    - path (str): Path to the CSV file. Can be an absolute path, a relative path with './', or a path within a repo.
    - repo (str): Name of the repo if the path is relative within a repo.
    - index (bool): Whether to include the index (row numbers) in the CSV file. Default is False.
    - max_rows_threshold (int): The maximum number of rows allowed in the DataFrame before raising an error. Default is 1000.
    - **kwargs: Additional keyword arguments to be passed to pd.DataFrame.to_csv().

    Returns:
    - None

    Raises:
    - ValueError: If the DataFrame exceeds the maximum rows threshold, the directory does not exist, or the DataFrame is empty.
    - IOError: If there is an issue writing the CSV file.

    Example:
        >>> write_csv_file(spark_df, './relative/path/in/project.csv')
        >>> write_csv_file(spark_df, '/Workspace/absolute/path.csv')
        >>> write_csv_file(spark_df, path='path/in/repo.csv', repo='common_repo')
    """
    # Check if DataFrame exceeds the maximum rows threshold
    if df.count() > max_rows_threshold:
        raise ValueError(f"DataFrame exceeds maximum rows threshold of {max_rows_threshold}. "
                         "This function is not meant for writing large datasets. "
                         "Consider using save_table() function to save to the database.")

    # Resolve the path
    resolved_path = resolve_path(path, repo)

    # Check if the directory exists
    directory = os.path.dirname(resolved_path)
    if not os.path.exists(directory):
        raise ValueError(f"Directory '{directory}' does not exist.")

    # Check if DataFrame is empty
    if df.count() == 0:
        raise ValueError("DataFrame is empty")

    try:
        # Convert the Spark DataFrame to a Pandas DataFrame and write it to a CSV file
        df.toPandas().to_csv(resolved_path, index=index, **kwargs)
    except Exception as e:
        raise IOError(f"Error writing DataFrame to CSV file: {str(e)}")


def create_dict_from_csv(path: str, key_column: str, value_columns, retain_column_names: bool = False, cast_key_as_string: bool = True, repo: str = None) -> dict:
    """
    Reads a .csv file and creates a dictionary based on specified columns for the key and value(s).

    Args:
    - path (str): Path to the .csv file. Can be an absolute path, a relative path with './', or a path within a repo.
    - key_column (str): The column name to be used as keys in the dictionary.
    - value_columns (list of str or str): A list of column names or a single column name to be used as values in the dictionary.
    - retain_column_names (bool, optional): If True, retains the column names of value_columns as keys in a sub dictionary.
        Defaults to False.
    - cast_key_as_string (bool, optional): If True, casts the key as a string. Defaults to True.
    - repo (str): Name of the repo if the path is relative within a repo.

    Returns:
    - dict: A dictionary with keys from the specified key_column and corresponding values from value_columns.

    Raises:
    - ValueError: If the key column is not unique in the CSV file.

    Example:
        >>> result = csv_to_dict('./data.csv', 'Name', ['Age', 'Gender'], retain_column_names=False)
        >>> print(result)
        {'John': ['30', 'Male'], 'Alice': ['25', 'Female']}
        >>> result = csv_to_dict('./data.csv', 'Name', ['Age', 'Gender'], retain_column_names=True)
        >>> print(result)
        {'John': {'Age': '30', 'Gender': 'Male'}, 'Alice': {'Age': '25', 'Gender': 'Female'}}
    """
    if isinstance(value_columns, str):  # Convert to list if input is a string
        value_columns = [value_columns]
    
    # Resolve the path
    resolved_path = resolve_path(path, repo)

    # Read table
    df = pd.read_csv(resolved_path)
    
    # Check if the key column is unique
    if not df[key_column].is_unique:
        raise ValueError("Key column '{}' is not unique".format(key_column))
    
    result_dict = {}
    for index, row in df.iterrows():
        key = str(row[key_column]) if cast_key_as_string else row[key_column]
        values = {col: row[col] for col in value_columns}
        if len(value_columns) == 1:  # If only one value column is provided
            values = next(iter(values.values()))  # Extract the single value
        elif not retain_column_names:
            values = list(values.values())  # If multiple value columns but retain_column_names is False
        result_dict[key] = values
    return result_dict