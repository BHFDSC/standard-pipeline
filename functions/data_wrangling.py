"""
Module name: data_wrangling.py

This module includes functions for common data wrangling tasks in PySpark.

Functions:
- melt: Convert a PySpark DataFrame from wide to long format.
- clean_column_names: Clean column names of a PySpark DataFrame.
- map_column_values: Map column values from one value to another based on a dictionary.
"""

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame
from typing import Dict
from itertools import chain

def melt(
    df: DataFrame,
    id_vars: t.Iterable[str],
    value_vars: t.Iterable[str],
    var_name: str='variable',
    value_name: str='value'
) -> DataFrame:
    """
    Convert a PySpark DataFrame from wide to long format.

    Notes:
        This function provides functionality similar to `.melt()` available in Spark version 3.4.0 and later.

    Args:
        df (DataFrame): The input DataFrame to be melted.
        id_vars (Iterable[str]): A list of column names to be retained as identifier variables.
        value_vars (Iterable[str]): A list of column names to be melted.
        var_name (str, optional): The name of the variable column after melting (default is 'variable').
        value_name (str, optional): The name of the value column after melting (default is 'value').

    Returns:
        DataFrame: A DataFrame in long format with columns specified by `id_vars`, `var_name`, and `value_name`.

    Example:
        >>> df = spark.createDataFrame([(1, 'A', 10, 100), (2, 'B', 20, 200)], ['ID', 'Category', 'Value1', 'Value2'])
        >>> melted_df = melt(df, ['ID', 'Category'], ['Value1', 'Value2'])
        >>> melted_df.show()
        +---+--------+---------+-----+
        | ID|Category|variable |value|
        +---+--------+---------+-----+
        |  1|       A|   Value1|   10|
        |  1|       A|   Value2|  100|
        |  2|       B|   Value1|   20|
        |  2|       B|   Value2|  200|
        +---+--------+---------+-----+
    """
    # Create array
    vars_and_vals = f.array(*(
        f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name))
        for c in value_vars
    ))

    # Add to the DataFrame and explode
    melted_df = df.withColumn('vars_and_vals', f.explode(vars_and_vals))

    cols = id_vars + [
        f.col('vars_and_vals')[x].alias(x)
        for x in [var_name, value_name]
    ]

    return melted_df.select(*cols)


def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Clean column names of a PySpark DataFrame by replacing non-alphanumeric characters with underscores,
    ensuring they don't start with a number, and converting them to lowercase. Duplicate column names are made unique
    by appending a suffix.

    Args:
        df (DataFrame): The DataFrame whose column names are to be cleaned.

    Returns:
        DataFrame: A new DataFrame with cleaned column names.

    Example:
        >>> data = [("John Doe", 30), ("Jane Smith", 25)]
        >>> df = spark.createDataFrame(data, ["Name", "Age"])
        >>> df = df.select("Name", f.col("Name").alias("0_N@me!"), f.col("Name").alias("0_N@me!"))
        >>> df.show()
        +----------+----------+----------+
        |      Name|   0_N@me!|   0_N@me!|
        +----------+----------+----------+
        |  John Doe|  John Doe|  John Doe|
        |Jane Smith|Jane Smith|Jane Smith|
        +----------+----------+----------+
        >>> df_cleaned = clean_column_names(df)
        >>> df_cleaned.show()
        +----------+----------+----------+
        |      name|  _0_n_me_|_0_n_me__2|
        +----------+----------+----------+
        |  John Doe|  John Doe|  John Doe|
        |Jane Smith|Jane Smith|Jane Smith|
        +----------+----------+----------+
    """
    def clean_name(name: str) -> str:
        # Replace non-alphanumeric characters with underscores
        cleaned_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
        # Ensure column name doesn't start with a number
        if cleaned_name[0].isdigit():
            cleaned_name = '_' + cleaned_name
        return cleaned_name.lower()

    # Clean column names
    cleaned_columns = [clean_name(col) for col in df.columns]

    # Check for duplicate column names and make them unique
    seen = {}
    new_columns = []
    for col in cleaned_columns:
        if col not in seen:
            seen[col] = 1
            new_columns.append(col)
        else:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")

    # Rename columns in the DataFrame
    return df.toDF(*new_columns)


def map_column_values(df: DataFrame, map_dict: Dict, column: str, new_column: str = "") -> DataFrame:
    """
    Method for mapping column values from one value to another based on a dictionary.

    Args:
        df (DataFrame): DataFrame to operate on.
        map_dict (Dict): Dictionary containing the values to map from and to.
        column (str): The column containing the values to be mapped.
        new_column (str, optional): The name of the column to store the mapped values in.
            If not specified, the values will be stored in the original column.

    Returns:
        DataFrame: DataFrame with mapped values.

    Example:
        >>> data = [('A',), ('B',), ('C',), ('D',)]
        >>> df = spark.createDataFrame(data, ['column_to_map'])
        >>> map_dict = {'A': 'Apple', 'B': 'Banana', 'C': 'Cherry'}
        >>> mapped_df = map_column_values(df, map_dict, column='column_to_map', new_column='mapped_column')
        >>> mapped_df.show()
        +---------------+--------------+
        | column_to_map | mapped_column|
        +---------------+--------------+
        |              A|         Apple|
        |              B|        Banana|
        |              C|        Cherry|
        |              D|          null|
        +---------------+--------------+
    """

    if column not in df.columns:
        raise ValueError(f"Column '{column}' does not exist in the DataFrame.")

    if not map_dict:
        raise ValueError("Empty mapping dictionary provided.")

    spark_map = f.create_map(*[f.lit(x) for x in chain(*map_dict.items())])

    if new_column and new_column in df.columns:
        raise ValueError(f"Column '{new_column}' already exists in the DataFrame.")

    new_col_name = new_column or column

    return df.withColumn(new_col_name, spark_map[df[column]])