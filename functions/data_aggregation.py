"""
Module name: data_aggregation.py

Description:
A module for handling data aggregation tasks that may not be directly supported by PySpark's built-in functions

Functions:
- select_top_rows: Core function for row selection functions first_dense_rank(), first_rank() and first_row()
- first_dense_rank: Selects the rows corresponding to the first 'n' dense ranks based on specified ordering for each partition in a PySpark DataFrame.
- first_rank: Selects the rows corresponding to the first 'n' ranks based on specified ordering for each partition in a PySpark DataFrame.
- first_row: Selects the first 'n' rows based on specified ordering for each partition in a PySpark DataFrame.

"""

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import Window

def select_top_rows(
    df, method, n=1, partition_by=None, order_by=None, return_index_column=False, index_column_name='row_index'
) -> DataFrame:
    """
    Selects top rows for each partition in a PySpark DataFrame based on the specified row indexing method,
    optionally retaining the row indices.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be processed.
        method (str): The method for row indexing. Allowed values are 'row_number', 'rank', and 'dense_rank'.
        n (int, optional): The number of rows to retain for each partition. Default is 1.
        partition_by (list, optional): A list of column names to partition the data. Default is None.
        order_by (list, optional): A list of column names to order the data within each partition. Default is None.
        return_index_column (bool, optional): Whether to include the row index column in the output DataFrame. Default is False.
        index_column_name (str, optional): The name of the index column. Default is 'row_index'.

    Note:
        - If the order_by columns contain null values, it's important to specify the treatment of nulls explicitly.
          By default, null values are treated as smallest when using orderBy. To specify whether nulls should come first or last,
          consider using asc_nulls_last() or desc_nulls_first(), or a combination of asc(), desc(), nulls_first(), and nulls_last() functions.

    Returns:
        pyspark.sql.DataFrame: A new PySpark DataFrame containing the top 'n' rows for each partition.

    Example:
        >>> df = spark.createDataFrame([("A", 1), ("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("B", 6)], ["Group", "Value"])
        >>> result = select_top_rows(df, method='row_number', n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'row_number')
        >>> result.show()
        +-----+-----+----------+
        |Group|Value|row_number|
        +-----+-----+----------+
        |    A|    1|         1|
        |    A|    1|         2|
        |    B|    4|         1|
        |    B|    5|         2|
        +-----+-----+----------+
        >>> result = select_top_rows(df, method='rank', n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'rank_index')
        >>> result.show()
        +-----+-----+----------+
        |Group|Value|rank_index|
        +-----+-----+----------+
        |    A|    1|         1|
        |    A|    1|         1|
        |    B|    4|         1|
        |    B|    5|         2|
        +-----+-----+----------+
        >>> result = select_top_rows(df, method='dense_rank', n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'dense_rank_index')
        >>> result.show()
        +-----+-----+----------------+
        |Group|Value|dense_rank_index|
        +-----+-----+----------------+
        |    A|    1|               1|
        |    A|    1|               1|
        |    A|    2|               2|
        |    B|    4|               1|
        |    B|    5|               2|
        +-----+-----+----------------+
    """

    # Input validation for method
    assert method in ['row_number', 'rank', 'dense_rank'], "Invalid method. Allowed values are 'row_number', 'rank', and 'dense_rank'."

    # Input validation for n
    assert isinstance(n, int) and n > 0, 'n must be a positive, non-zero integer'

    # Add '_dummy_column' if partition_by is not provided
    if partition_by is None:
        partition_by = ['_dummy_column']
        df = df.withColumn('_dummy_column', f.lit(1))

    # Adjust the window specification for ordering in ascending order
    window_spec = Window.partitionBy(*partition_by)
    if order_by is not None:
        window_spec = window_spec.orderBy(*order_by)

    # Add row index column based on the window specification and method
    if method == 'row_number':
        df = df.withColumn(index_column_name, f.row_number().over(window_spec))
    elif method == 'rank':
        df = df.withColumn(index_column_name, f.rank().over(window_spec))
    elif method == 'dense_rank':
        df = df.withColumn(index_column_name, f.dense_rank().over(window_spec))

    # Filter for the first 'n' rows after ordering in ascending order
    df = df.filter(f.col(index_column_name) <= n)

    # Drop unnecessary columns
    if not return_index_column:
        df = df.drop(index_column_name)
    if partition_by == ['_dummy_column']:
        df = df.drop('_dummy_column')

    return df


def first_row(
    df, n = 1, partition_by = None, order_by = None, return_index_column = False, index_column_name = 'row_index'
) -> DataFrame:
    """
    Selects the first 'n' rows based on specified ordering for each partition in a PySpark DataFrame, optionally retaining the row indices.
    A wrapper function for select_top_rows() with method = 'row_number'.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be processed.
        n (int): The number of rows to retain for each partition.
        partition_by (list, optional): A list of column names to partition the data. Default is None.
        order_by (list, optional): A list of column names to order the data within each partition. Default is None.
        return_index_column (bool, optional): Whether to include the row index column in the output DataFrame. Default is False.
        index_column_name (str, optional): The name of the row index column. Default is 'row_index'.

    Note:
        - If the order_by columns contain null values, it's important to specify the treatment of nulls explicitly. By default, null values are
            treated as smallest when using orderBy. To specify whether nulls should come first or last, consider using asc_nulls_last()
            or desc_nulls_first(), or a combination of asc(), desc(), nulls_first(), and nulls_last() functions.

    Returns:
        pyspark.sql.DataFrame: A new PySpark DataFrame containing the first 'n' rows for each partition.

    Example:
        >>> df = spark.createDataFrame([("A", 1), ("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("B", 6)], ["Group", "Value"])
        >>> result = first_row(df, n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'row_number')
        >>> result.show()
        +-----+-----+----------+
        |Group|Value|row_number|
        +-----+-----+----------+
        |    A|    1|         1|
        |    A|    1|         2|
        |    B|    4|         1|
        |    B|    5|         2|
        +-----+-----+----------+
    """

    df = select_top_rows(
        df, method = 'row_number', n = n, partition_by = partition_by, order_by = order_by,
        return_index_column = return_index_column, index_column_name = index_column_name
    )

    return df


def first_rank(
    df, n = 1, partition_by = None, order_by = None, return_index_column = False, index_name = 'rank_index'
) -> DataFrame:
    """
    Selects the rows corresponding to the first 'n' ranks based on specified ordering for each partition in a PySpark DataFrame, optionally 
    retaining the row indices.
    A wrapper function for select_top_rows() with method = 'rank'.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be processed.
        n (int): The number of ranks to retain for each partition.
        partition_by (list, optional): A list of column names to partition the data. Default is None.
        order_by (list, optional): A list of column names to order the data within each partition. Default is None.
        return_index_column (bool, optional): Whether to include the row index column in the output DataFrame. Default is False.
        index_column_name (str, optional): The name of the rank index column. Default is 'row_index'.

    Note:
        - If the order_by columns contain null values, it's important to specify the treatment of nulls explicitly. By default, null values are
            treated as smallest when using orderBy. To specify whether nulls should come first or last, consider using asc_nulls_last()
            or desc_nulls_first(), or a combination of asc(), desc(), nulls_first(), and nulls_last() functions.

    Returns:
        pyspark.sql.DataFrame: A new PySpark DataFrame containing the first 'n' rows for each partition.

    Example:
        >>> df = spark.createDataFrame([("A", 1), ("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("B", 6)], ["Group", "Value"])
        >>> result = first_rank(df, n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'rank_index')
        >>> result.show()
        +-----+-----+----------+
        |Group|Value|rank_index|
        +-----+-----+----------+
        |    A|    1|         1|
        |    A|    1|         1|
        |    B|    4|         1|
        |    B|    5|         2|
        +-----+-----+----------+
    """

    df = select_top_rows(
        df, method = 'rank', n = n, partition_by = partition_by, order_by = order_by,
        return_index_column = return_index_column, index_column_name = index_column_name
    )

    return df


def first_dense_rank(
    df, n = 1, partition_by = None, order_by = None, return_index_column = False, index_column_name = 'dense_rank_index'
) -> DataFrame:
    """
    Selects the rows corresponding to the first 'n' dense ranks based on specified ordering for each partition in a PySpark DataFrame, optionally 
    retaining the row indices.
    A wrapper function for select_top_rows() with method = 'dense_rank'.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be processed.
        n (int): The number of dense ranks to retain for each partition.
        partition_by (list, optional): A list of column names to partition the data. Default is None.
        order_by (list, optional): A list of column names to order the data within each partition. Default is None.
        return_index_column (bool, optional): Whether to include the row index column in the output DataFrame. Default is False.
        index_column_name (str, optional): The name of the dense rank index column. Default is 'row_index'.

    Note:
        - If the order_by columns contain null values, it's important to specify the treatment of nulls explicitly. By default, null values are
            treated as smallest when using orderBy. To specify whether nulls should come first or last, consider using asc_nulls_last()
            or desc_nulls_first(), or a combination of asc(), desc(), nulls_first(), and nulls_last() functions.

    Returns:
        pyspark.sql.DataFrame: A new PySpark DataFrame containing the first 'n' rows for each partition.

    Example:
        >>> df = spark.createDataFrame([("A", 1), ("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("B", 6)], ["Group", "Value"])
        >>> result = first_dense_rank(df, n=2, partition_by=["Group"], order_by=["Value"],
            return_index_column = True, index_column_name = 'dense_rank_index')
        >>> result.show()
        +-----+-----+----------------+
        |Group|Value|dense_rank_index|
        +-----+-----+----------------+
        |    A|    1|               1|
        |    A|    1|               1|
        |    A|    2|               2|
        |    B|    4|               1|
        |    B|    5|               2|
        +-----+-----+----------------+
    """

    df = select_top_rows(
        df, method = 'dense_rank', n = n, partition_by = partition_by, order_by = order_by,
        return_index_column = return_index_column, index_column_name = index_column_name
    )

    return df


