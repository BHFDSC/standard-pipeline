"""
Module name: cohort_construction.py

Description:
    This module provides functions for constructing and managing cohort tables. 
    It includes functionality for filtering cohorts based on specified inclusion 
    criteria and generating flowchart tables to visualize the application of these
    criteria.

Functions:
    - apply_inclusion_criteria: Filters cohort DataFrame based on inclusion criteria. Optionally creates flowchart.
    - create_inclusion_columns: Creates flag columns based on inclusion criteria
    - create_inclusion_flowchart: Logs the changes in rows and individuals for each
        step in inclusion criteria
"""

from pyspark.sql import functions as f, DataFrame, Window
from .table_management import save_table
from .environment_utils import get_spark_session

def apply_inclusion_criteria(
    cohort: DataFrame, 
    inclusion_criteria: dict[str, str], 
    flowchart_table: str = None, 
    row_id_col: str = 'row_id', 
    person_id_col: str = 'person_id', 
    drop_inclusion_flags: bool = True
) -> DataFrame:
    """
    Apply inclusion criteria to the cohort and optionally generate a flowchart table.
    
    Args:
        cohort (DataFrame): The input DataFrame containing cohort data.
        inclusion_criteria (dict[str, str]): A dictionary of inclusion criteria where keys are column names and values are SQL expressions.
        flowchart_table (str, optional): The table key of the flowchart table to save (if any). Defaults to None.
        row_id_col (str, optional): The name of the row identifier column. Defaults to 'row_id'.
        person_id_col (str, optional): The name of the person identifier column. Defaults to 'person_id'.
        drop_inclusion_flags (bool, optional): If True, drop the inclusion criteria columns after filtering. Defaults to True.
    
    Returns:
        DataFrame: The filtered cohort DataFrame.
    
    Raises:
        ValueError: If the cohort contains existing columns with names like 'criteria_*', 'include', or any of the inclusion_criteria keys.

    Example:
    >>> cohort = spark.createDataFrame(
    ...     [("1", "id_001", 16), ("2", "id_002", 47), ("3", None, 53),
    ...      ("4", "id_004", 26), ("5", None, 32), ("6", "id_006", 84)],
    ...     ["row_id", "person_id", "age"]
    ... )

    >>> inclusion_criteria = {
    ...     "valid_person_id": "person_id IS NOT NULL",
    ...     "aged_between_18_and_65": "(age >= 18) AND (age <= 65)"
    ... }

    >>> # Single step example: Apply inclusion criteria and save flowchart
    >>> cohort_filtered = apply_inclusion_criteria(cohort, inclusion_criteria, flowchart_table = 'flowchart_table')
    >>> cohort_filtered.show()
    +------+---------+---+
    |row_id|person_id|age|
    +------+---------+---+
    |     2|   id_001| 47|   
    |     4|   id_004| 26|
    +------+---------+---+

    >>> # Intermediate results: Create flagged cohort
    >>> cohort_flagged = create_inclusion_columns(cohort, inclusion_criteria)
    >>> cohort_flagged.show()
    +------+---------+---+---------------+----------------------+----------+----------+----------+-------+
    |row_id|person_id|age|valid_person_id|aged_between_18_and_65|criteria_0|criteria_1|criteria_2|include|
    +------+---------+---+---------------+----------------------+----------+----------+----------+-------+
    |     1|   id_001| 16|           true|                 false|      true|      true|     false|  false|
    |     1|   id_002| 47|           true|                  true|      true|      true|      true|   true|
    |     1|     null| 53|          false|                  true|      true|     false|     false|  false|
    |     1|   id_004| 26|           true|                  true|      true|      true|      true|   true|
    |     1|     null| 32|          false|                  true|      true|     false|     false|  false|
    |     1|   id_006| 84|           true|                 false|      true|      true|     false|  false|
    +------+---------+---+---------------+----------------------+----------+----------+----------+-------+

    >>> # Intermediate results: Generate flowchart of inclusion criteria
    >>> inclusion_flowchart = create_inclusion_flowchart(cohort_flagged)
    >>> inclusion_flowchart.show()
    +--------------+----------+--------------------+--------------------+-----+-------------+-------------+------------+
    |criteria_index|  criteria|         description|          expression|n_row|n_distinct_id|excluded_rows|excluded_ids|
    +--------------+----------+--------------------+--------------------+-----+-------------+-------------+------------+
    |             0|criteria_0|      Original table|                    |    6|            4|         null|        null|
    |             1|criteria_1|     valid_person_id|person_id IS NOT ...|    4|            4|           -2|           0|
    |             2|criteria_2|aged_between_18_a...|(age >= 18) AND (...|    2|            2|           -2|          -2|
    +--------------+----------+--------------------+--------------------+-----+-------------+-------------+------------+

    """

    # Validate inclusion criteria
    validate_inclusion_criteria(cohort, inclusion_criteria)
    
    # Check for forbidden columns (criteria_*, 'include', and inclusion_criteria keys) and 
    validate_cohort_columns(cohort, inclusion_criteria, row_id_col, person_id_col)

    # Flag cohort rows based on inclusion criteria
    cohort_flagged = create_inclusion_columns(cohort, inclusion_criteria)

    # Generate and save flowchart if flowchart_table is specified
    if flowchart_table:
        flowchart = create_inclusion_flowchart(cohort_flagged, inclusion_criteria, row_id_col, person_id_col)
        save_table(df=flowchart, table=flowchart_table)

    # Filter cohort for rows that meet all inclusion criteria
    cohort_filtered = cohort_flagged.filter(f.col('include'))

    # Optionally drop inclusion flags and criteria columns after filtering
    if drop_inclusion_flags:
        # Create list of all columns to be dropped (criteria, flags, and the 'include' column)
        columns_to_drop = [f'criteria_{i}' for i in range(len(inclusion_criteria) + 1)] + list(inclusion_criteria.keys()) + ['include']
        cohort_filtered = cohort_filtered.drop(*columns_to_drop)

    return cohort_filtered
    

def create_inclusion_columns(cohort: DataFrame, inclusion_criteria: dict[str, str]) -> DataFrame:
    """
    Create and flag inclusion criteria columns in the cohort DataFrame.

    Args:
        cohort (DataFrame): The input DataFrame containing cohort data.
        inclusion_criteria (dict[str, str]): A dictionary of inclusion criteria where keys are column names and values are SQL expressions.

    Returns:
        DataFrame: The cohort DataFrame with additional 'criteria_*' columns and a final 'include' column that flags rows.
    """

    # Apply each inclusion criteria, creating new columns in the DataFrame
    for column_name, sql_expression in inclusion_criteria.items():
        cohort = cohort.withColumn(column_name, f.expr(sql_expression))

    # Fill null values in the criteria columns with False
    cohort = cohort.fillna(False, list(inclusion_criteria.keys()))

    # Initialize the first 'criteria_0' column as True for baseline inclusion
    cohort = cohort.withColumn('criteria_0', f.lit(True))

    # Chain the criteria columns (criteria_1, criteria_2, ...) based on logical AND
    for index, column_name in enumerate(inclusion_criteria.keys(), start=1):
        cohort = cohort.withColumn(f'criteria_{index}', f.col(f'criteria_{index - 1}') & f.col(column_name))

    # The final 'include' column is based on the last criteria in the chain
    cohort_flagged = cohort.withColumn('include', f.col(f'criteria_{len(inclusion_criteria)}'))

    return cohort_flagged


def create_inclusion_flowchart(
    cohort_flagged: DataFrame,
    inclusion_criteria: dict[str, str],
    row_id_col: str = 'row_id', 
    person_id_col: str = 'person_id'
) -> DataFrame:
    """
    Create a flowchart DataFrame tracking inclusion criteria application.

    Args:
        cohort_flagged (DataFrame): The cohort DataFrame with 'criteria_*' and 'include' columns.
        inclusion_criteria (dict[str, str]): A dictionary of inclusion criteria where keys are column names and values are SQL expressions.
        row_id_col (str): The column name for row IDs.
        person_id_col (str): The column name for person IDs.

    Returns:
        DataFrame: A flowchart DataFrame showing how many rows and distinct persons passed each criteria.
    """
    
    # Criteria columns for flowchart tracking
    criteria_columns = [f'criteria_{i}' for i in range(len(inclusion_criteria) + 1)]

    # Get spark session
    spark = get_spark_session()

    # Create a DataFrame for inclusion criteria
    df_inclusion_criteria = spark.createDataFrame(
        [('criteria_0', 'Original table', '')] + 
        [(f'criteria_{i + 1}', k, v) for i, (k, v) in enumerate(inclusion_criteria.items())],
        ['criteria', 'description', 'expression']
    )

    # Define the window for lag functions
    _win = Window.orderBy('criteria_index')

    # Define identifying columns for unpivot (usually the row and person identifiers)
    id_cols = [row_id_col, person_id_col]

    # Calculate exclusion flowchart by unpivoting criteria columns and aggregating
    flowchart = (
        cohort_flagged
        .select(id_cols + criteria_columns)
        .unpivot(
            ids=id_cols, 
            values=criteria_columns,
            variableColumnName='criteria', 
            valueColumnName='value'
        )
        .groupBy('criteria')
        .agg(
            f.count(f.when(f.col('value') == True, 1)).alias('n_row'),
            f.countDistinct(f.when(f.col('value') == True, f.col(person_id_col))).alias('n_distinct_id')
        )
        .join(
            f.broadcast(df_inclusion_criteria),
            on='criteria', 
            how='left'
        )
        .withColumn('criteria_index', (f.regexp_extract('criteria', r'\d+', 0)).cast('int'))
        .withColumn('excluded_rows', (f.lag('n_row', 1).over(_win) - f.col('n_row')).cast('int'))
        .withColumn('excluded_ids', (f.lag('n_distinct_id', 1).over(_win) - f.col('n_distinct_id')).cast('int'))
        .select(
            'criteria_index', 'criteria', 'description', 'expression',
            'n_row', 'n_distinct_id', 'excluded_rows', 'excluded_ids'
        )
        .orderBy('criteria_index')
    )

    return flowchart


def validate_inclusion_criteria(cohort: DataFrame, inclusion_criteria: dict[str, str]) -> None:
    """
    Validate that the inclusion criteria are structured correctly.
    
    Args:
        cohort (DataFrame): The input DataFrame containing cohort data.
        inclusion_criteria (dict[str, str]): A dictionary of inclusion criteria where keys are column names and values are SQL expressions.
    
    Raises:
        TypeError: If inclusion_criteria is not a dictionary or if the SQL expressions are not strings.
    """
    
    if not isinstance(inclusion_criteria, dict):
        raise TypeError("The inclusion_criteria must be a dictionary where keys are criteria column names and values are SQL expressions.")

    # Ensure the values in the inclusion_criteria are valid SQL expression strings
    for key, value in inclusion_criteria.items():
        if not isinstance(value, str):
            raise TypeError(f"The SQL expression for inclusion criteria '{key}' must be a string, but got {type(value).__name__}.")


def validate_cohort_columns(
    cohort: DataFrame, 
    inclusion_criteria: dict[str, str], 
    row_id_col: str, 
    person_id_col: str
) -> None:
    """
    Validate that the required columns are present and that no conflicting columns exist in the cohort DataFrame.

    Args:
        cohort (DataFrame): The input DataFrame containing cohort data.
        inclusion_criteria (dict[str, str]): A dictionary of inclusion criteria where keys are criteria column names.
        row_id_col (str): The column name for row identifiers.
        person_id_col (str): The column name for person identifiers.

    Raises:
        ValueError: If the cohort contains columns with names that conflict with 'criteria_*', 'include', or any keys from inclusion_criteria.
        AnalysisException: If row_id_col or person_id_col are missing from the cohort.
    """
    
    # Get the set of current columns in the cohort DataFrame for efficient lookup
    cohort_columns = set(cohort.columns)

    # Define forbidden columns: any column that starts with 'criteria_' and 'include'
    forbidden_columns = {
        col for col in cohort_columns if col.startswith('criteria_')
    } | {'include'}

    # Include the keys from inclusion_criteria in the forbidden columns
    forbidden_columns |= set(inclusion_criteria.keys())

    # Check for any conflicting columns in the cohort
    conflicting_columns = forbidden_columns.intersection(cohort_columns)
    if conflicting_columns:
        raise ValueError(
            f"The cohort DataFrame contains conflicting columns: {', '.join(conflicting_columns)}"
        )

    # Verify that the required ID columns are present
    missing_columns = [col for col in (row_id_col, person_id_col) if col not in cohort_columns]
    if missing_columns:
        raise AnalysisException(
            f"Missing required columns: {', '.join(missing_columns)}"
        )
