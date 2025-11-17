"""
Module name: data_privacy.py

Description:
    This module provides functions for managing data privacy in compliance with output restrictions
    that require the suppression of sensitive information. It enables users to manipulate 
    numerical data to protect sensitive counts from unauthorised disclosure.

Functions:
    - round_counts_to_multiple: Rounds the values in specified columns of a Spark DataFrame 
      to the nearest specified multiple, ensuring that sensitive counts remain less precise.
    - redact_low_counts: Redacts (masks) values in specified columns of a Spark DataFrame that are 
      below a defined threshold, replacing them with a specified redaction value or None.
"""


from pyspark.sql import functions as f, DataFrame
from typing import List, Optional, Union

def round_counts_to_multiple(df: DataFrame, columns: List[str], multiple: int = 5) -> DataFrame:
    """
    Rounds the values of specified columns in a Spark DataFrame to the nearest specified multiple.

    Args:
        df (DataFrame): The input Spark DataFrame containing the columns to be rounded.
        columns (List[str]): A list of column names (as strings) in the DataFrame to be rounded.
        multiple (int): The multiple to which the values in the specified columns should be rounded.
            Defaults to 5.

    Returns:
        DataFrame: A new Spark DataFrame with the specified columns rounded to the nearest multiple of 
            the provided value.

    Raises:
        TypeError: If df is not a DataFrame, columns is not a list of strings, or multiple is not an int.
        ValueError: If any column specified in columns does not exist in the DataFrame, or if multiple is not positive.

    Example:
        >>> df = spark.createDataFrame([(1, 7), (2, 17)], ["id", "count"])
        >>> rounded_df = round_counts_to_multiple(df, ["count"], multiple=5)
        >>> rounded_df.show()
        +---+-----+
        | id|count|
        +---+-----+
        |  1|    5|
        |  2|   20|
        +---+-----+
    """
    # Check if df is a DataFrame
    if not isinstance(df, DataFrame):
        raise TypeError("The input 'df' must be a Spark DataFrame.")
    
    # Check if columns is a list of strings
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise TypeError("The 'columns' argument must be a list of strings.")
    
    # Check if multiple is a positive integer
    if not isinstance(multiple, int) or multiple <= 0:
        raise ValueError("The 'multiple' argument must be a positive integer.")
    
    # Check if the columns exist in the DataFrame
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"The column '{col}' does not exist in the DataFrame.")
    
    for col in columns:
        df = df.withColumn(col, f.round(f.col(col) / multiple) * multiple)

    return df


def redact_low_counts(df: DataFrame, columns: List[str], threshold: int, redaction_value: Optional[Union[str, int]] = None) -> DataFrame:
    """
    Redacts (or masks) counts in the specified columns of a DataFrame that are below a specified threshold value.

    Args:
        df (DataFrame): A Spark DataFrame containing the data.
        columns (List[str]): A list of column names in the DataFrame where counts will be redacted.
        threshold (int): The threshold value below which counts will be redacted. Must be a positive integer.
        redaction_value (Optional[Union[str, int]]): The value to replace redacted counts. This can be a string, an integer, or None. 
            If None, redacted counts will be set to None. 
            Note: If a string is provided, the column will be cast to string type.

    Returns:
        DataFrame: A new DataFrame with counts below the threshold redacted in the specified columns.

    Raises:
        ValueError: If any column name in `columns` does not exist in the DataFrame, or if the threshold is not a positive integer.
        TypeError: If `threshold` is not an integer or if `columns` is not a list of strings.

    Example:
        >>> df = spark.createDataFrame([(1, 7), (2, 17)], ["id", "count"])
        >>> rounded_df = redact_low_counts(df, ["count"], threshold=10, redaction_value="[:REDACTED:]")
        >>> rounded_df.show()
        +---+------------+
        | id|       count|
        +---+------------+
        |  1|[:REDACTED:]|
        |  2|          17|
        +---+------------+
    """
    # Check if the threshold is a positive integer
    if not isinstance(threshold, int) or threshold <= 0:
        raise ValueError("Threshold must be a positive integer.")

    # Check if columns is a list of strings and if they exist in the DataFrame
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise TypeError("Columns must be a list of strings.")
    
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' does not exist in the DataFrame.")

    # Set the redaction value to None if not provided
    redaction_value = f.lit(redaction_value) if redaction_value is not None else f.lit(None)

    # Build a list of transformations
    for col in columns:
        df = df.withColumn(col, f.when(f.col(col) >= threshold, f.col(col)).otherwise(redaction_value))

    return df
