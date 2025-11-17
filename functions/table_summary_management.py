# from .environment_utils import get_spark_session
import os
import pkg_resources
from pyspark.sql import SparkSession

def get_spark_session():
    """
    Creates or retrieves a SparkSession object.

    This function initializes a SparkSession with the specified app name if it does
    not exist; otherwise, it retrieves the existing SparkSession.

    Returns:
    - spark_session (SparkSession): A SparkSession object.

    Example:
        >>> spark = get_spark_session()
    """
    spark_session = (
        SparkSession.builder
        .appName('SparkSession')
        .getOrCreate()
    )

    return spark_session


spark = get_spark_session()
def get_current_datetime(format_str="yyyy_MM_dd_HH:mm"):
    """
    Returns the current timestamp (according to Spark) as a string
    formatted by the given pattern, e.g. "yyyy_MM_dd_HH:mm".
    """
    row = spark.sql(f"SELECT date_format(current_timestamp(), '{format_str}') AS dt").collect()[0]
    return row["dt"]


from typing import Union, List
import re
from pyspark.sql import functions as f, Row


def build_archive_table_list(all_tbls) -> list:
    """
    Given a list of live tables, group those whose names contain a 4-digit
    'fyear' into a set of *_all_years_archive, and the rest into *_archive.
    Returns a list of all such archive table names..

    Returns a list of archive table names in the SAME order as all_tbls.
    """

    fyears_regex = r"(?P<tbl_type>.*)_(?P<fyear>\d{4})_"
    db_table_list = []
    seen = set()

    for tbl in all_tbls:
        # Extract the table name string
        table_name = tbl.tableName if hasattr(tbl, "tableName") else tbl
        
        match = re.match(fyears_regex, table_name)
        if match:
            grp = match.groupdict()
            archive_name = f"{grp['tbl_type']}_all_years_archive"
        else:
            archive_name = f"{table_name}_archive"

        # Only add the name if we haven't seen it yet
        if archive_name not in seen:
            seen.add(archive_name)
            db_table_list.append(archive_name)

    return db_table_list


def strip_archived_names(
    archived_input: Union[str, List[str]]
) -> Union[str, List[str]]:
    """
    Given a list of archived table names that include suffixes like _all_years,
     _archive, or _dars_nic_391419_j3w0t, return a new list with those substrings 
    removed, revealing the table_name.
    Returns a single string if the input was a single string, otherwise
    returns a list of strings if the input was a list.
    """
    
    pattern = re.compile(r"_all_years|_archive|_dars_nic_391419_j3w9t")

    if isinstance(archived_input, str):
        # It's just one name; strip once and return a single string.
        return pattern.sub("", archived_input)
    elif isinstance(archived_input, list):
        # It's a list of names; strip each and return a new list.
        return [pattern.sub("", tbl_name) for tbl_name in archived_input]
    else:
        raise TypeError(
            f"Expected str or List[str], got {type(archived_input)} instead."
        )


def create_table_mapping(cleaned_db_table_list: list[str],
                         archive_table_list: list[str],
                         dbc: str
                         ) -> dict:
    """
    Build a dict where the key is the 'cleaned' table name
    and the value is {'dbc': <archive_table_name>}.
    We assume both input lists align by index (same length).
    """
    final_mapping = {}
    for cleaned, archived in zip(cleaned_db_table_list, archive_table_list):
        final_mapping[cleaned] = {
            "table_name": archived,
            "database": dbc
            }
    return final_mapping


def describe_table(table_name, dbc):
    """
    Run DESCRIBE DETAIL for one table and return a list of dicts
    (so that we can easily unify columns across multiple tables).
    """
    rows = spark.sql(f"DESCRIBE DETAIL {dbc}.{table_name}").collect()
    # Convert each Row to a dict, add the table_name
    return [
        {**r.asDict(), "table_name": strip_archived_names(table_name)}
        for r in rows
    ]

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, LongType, DoubleType, TimestampType, DateType
)
import datetime

def python_value_to_spark_type(values):
    """
    Given a list of Python values (ignoring None),
    pick an appropriate Spark type.
    If multiple distinct Python types appear, degrade to StringType.
    """
    non_nulls = [v for v in values if v is not None]
    if not non_nulls:
        # All were None => just default to string
        return StringType()

    distinct_types = {type(v) for v in non_nulls}

    # If exactly one type, map it directly:
    if len(distinct_types) == 1:
        t = distinct_types.pop()
        if issubclass(t, bool):
            return BooleanType()
        elif issubclass(t, int):
            return LongType()
        elif issubclass(t, float):
            return DoubleType()
        elif issubclass(t, datetime.datetime):
            return TimestampType()
        else:
            # Default to string if it’s e.g. str or something else
            return StringType()

    # If multiple Python types, see if they are all numeric (int/float).
    # Otherwise degrade to string.
    if distinct_types.issubset({int, float}):
        return DoubleType()
    return StringType()

from pyspark.sql import DataFrame
def find_changed_datasets(old_meta: DataFrame, new_meta: DataFrame) -> list:
    """
    Compare new metadata vs. old metadata (by dataset_name or table_name).
    Return a list of dataset identifiers that have changed since last time
    (e.g. lastModified is newer).
    If old_meta is None, treat all as changed.
    """
    if old_meta is None:
        return [row["table_name"] for row in new_meta.select("table_name").distinct().collect()]

    joined = (
        new_meta.alias("new")
        .join(old_meta.alias("old"), on="table_name", how="left")
        .select(
            "new.table_name",
            "new.lastModified",
            "old.lastModified"
        )
    )
    changed_rows = joined.filter(
        (f.col("old.lastModified").isNull()) |
        (f.col("new.lastModified") > f.col("old.lastModified"))
    )
    return [r["table_name"] for r in changed_rows.collect()]

def show_partitions(table, dbc):
    result = spark.sql(f"SHOW PARTITIONS {dbc}.{table}").collect()
    return [Row(table_name=strip_archived_names(table), archived_on=row.archived_on) for row in result]


def check_table_columns(table_key, table_metadata):
    """
    Checks whether table_metadata['person_id'] and table_metadata['event_date']
    exist in the actual columns of table_metadata['database.table_name'].
    Returns a dictionary if columns are missing, or None if all present.
    """
    db_table = f"{table_metadata['database']}.{table_metadata['table_name']}"
    
    try:
        # Load the schema (a metadata operation).
        df = spark.table(db_table)
        # Convert actual columns to a lowercase set
        actual_columns = {col.lower() for col in df.columns}
    except Exception as e:
        return {
            'table_key': table_key,
            'table': db_table,
            'error': str(e),
            'missing_columns': ['ALL']
        }
    
    missing_cols = []
    
    # person_id check
    person_id = table_metadata.get('person_id')
    if person_id:
        if person_id.lower() not in actual_columns:
            missing_cols.append(person_id)
    
    # event_date check
    event_date = table_metadata.get('event_date')
    if event_date:
        if event_date.lower() not in actual_columns:
            missing_cols.append(event_date)
    
    if missing_cols:
        return {
            'table_key': table_key,
            'table': db_table,
            'missing_columns': missing_cols
        }
    else:
        return None
    
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
def summarise_coverage(dataset: DataFrame, table: str = None, table_mapping: dict = None) -> DataFrame:
    
    id_column = table_mapping[table].get("person_id")
    date_column = table_mapping[table].get("event_date")

    select_exprs = [
        f"{id_column}" if id_column else "'null' as dummy_id",
        "archived_on",
        "BatchId",
    ]
    if date_column:
        select_exprs.append(f"{date_column}")

    dataset = dataset.selectExpr(*select_exprs)

    if date_column:
        dataset = (dataset
                   .withColumn(date_column, f.to_date(f.col(date_column), "yyyyMMdd"))
                   .withColumn(date_column,
                               f.when(f.length(f.col(date_column)) == 6,
                               f.to_date(f.concat(f.col(date_column), f.lit("01")), "yyyyMMdd"))
                               .when(f.length(f.col(date_column)) == 7,
                               f.to_date(f.concat(f.substring(f.col(date_column), 1, 6), f.lit("0"), f.substring(f.col(date_column), 7, 1)),"yyyyMMdd"))
                               .otherwise(f.to_date(f.col(date_column), "yyyyMMdd")))
                   .withColumn("date_ym", f.date_format(f.col(date_column), "yyyy-MM"))
                   )
    else:
        dataset = dataset.withColumn("date_ym", f.lit(None).cast("string"))

    group_cols = ["archived_on", "BatchId", "date_ym"]

    return (
            dataset
            .groupBy(*group_cols)
            .agg(
                f.count(f.lit(1)).alias("n"),
                f.count(f.col(id_column)).alias("n_id") if id_column else f.lit(0).cast("bigint").alias("n_id"),
                f.countDistinct(f.col(id_column)).alias("n_id_distinct") if id_column else f.lit(0).cast("bigint").alias("n_id_distinct")
                )
            .withColumn("table_name", f.lit(table))
            )

from pyspark.sql.utils import AnalysisException
from hds_functions import load_table, read_json_file
from hds_functions.table_management import get_archive_versions
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DateType
def compute_coverage_for_table(table: str, update_all: bool = False, table_mapping: dict = None) -> DataFrame:

    """
    Update the table for all versions or only the new versions not already in the table.

    Parameters:
        update_all (bool): If True, update all versions. If False, update only new versions.

    Returns:
        None
    """

    coverage_result_df = None

    if update_all:
        coverage_result_df = summarise_coverage(table_archive, table)

    else:
        
        try:
            coverage_df = load_table("archived_datasets_coverage")
            coverage_exists = True
        except AnalysisException:
            coverage_df = None
            coverage_exists = False
            
        # Load the archived version of the table
        table_archive = load_table(table, table_directory = table_mapping)
        # access partitions instead
        table_mapping = read_json_file(path=table_mapping)

        if coverage_exists:
            # Load the summary data for this table
            coverage_all_versions = load_table('archived_datasets_coverage').filter(f.col("table_name") == table)
            
            if coverage_all_versions.rdd.isEmpty():
                schema = StructType([
                    StructField("archived_on", DateType(), True),
                    StructField("BatchId", StringType(), True),
                    StructField("date_ym", StringType(), True),
                    StructField("n", LongType(), False),
                    StructField("n_id", LongType(), False),
                    StructField("n_id_distinct", LongType(), False),
                    StructField("table_name", StringType(), False)
                    ])
                empty_row = spark.createDataFrame([(None, None, None, 0, 0, 0, table)],schema=schema)
                coverage_result_df = empty_row
        
            # Get a list of versions from the main table and the coverage table
            all_versions = get_archive_versions(table_archive)
            existing_versions = get_archive_versions(coverage_all_versions)

            # Determine which versions need to be computed (i.e., those not in the coverage table)
            versions_to_compute = [v for v in all_versions if v not in existing_versions]

            if versions_to_compute:
                # Filter the main table to include only the versions that need to be computed
                table_subset = table_archive.filter(f.col('archived_on').isin(versions_to_compute))

                # Summarise the coverages for the filtered subset
                coverage_subset = summarise_coverage(table_subset, table, table_mapping)
            
                # Union the new summarised coverage with the existing coverage table
                coverage_updated = coverage_df.unionByName(coverage_subset)

                coverage_result_df = coverage_updated
                
        else:
            # If the table does not exist or update_all is True, summarise all versions for all tables
            coverage_result_df = summarise_coverage(table_archive, table, table_mapping)

    return coverage_result_df

from typing import List
from hds_functions import save_table
def run_sequential_coverage(table_names: List[str], table_mapping: dict = None) -> None:
    for table in table_names:
        print(f"Processing: {table}")
        df = compute_coverage_for_table(table=table, table_mapping=table_mapping)
        
        if df is not None:
            # Save result for this table immediately
            save_table(
                df=df,
                table="archived_datasets_coverage",
                partition_by="archived_on"
            )

from typing import List
from concurrent.futures import ThreadPoolExecutor

def run_parallel_coverage(table_names: List[str]) -> None:

    with ThreadPoolExecutor(max_workers=8) as executor:
        coverage_dfs = list(executor.map(compute_coverage_for_table, table_names))
        
    from pyspark.sql import DataFrame
    def union_all(dfs: List[DataFrame]) -> DataFrame:
        return reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    
    coverage_dfs = [df for df in coverage_dfs if df is not None]
    if not coverage_dfs:
        return
    
    combined_coverage_df = union_all(coverage_dfs)
    save_table(df=combined_coverage_df, table='archived_datasets_coverage', partition_by = 'archived_on')


def summarise_overall(coverage_df: DataFrame) -> DataFrame:
    
    overall_df = (
        coverage_df
        .groupBy("archived_on", "BatchId", "table_name")
        .agg(
            f.sum("n").alias("n"),
            f.sum("n_id").alias("n_id"),
            f.sum("n_id_distinct").alias("n_id_distinct")
        )
    )
    
    save_table(
        df=overall_df,
        table="archived_datasets_overall",
        partition_by="archived_on"
    )

from pyspark.sql import DataFrame, Window
from hds_functions import write_json_file
def table_all_versions_json(table_all_versions: DataFrame, path: str) -> None:
    # Define a window for each table_name, ordered by archived_on
    window_spec = Window.partitionBy('table_key').orderBy('archived_on')


    # Convert to the desired dictionary format
    result = (
        table_all_versions
        .withColumn("archived_on", f.col('archived_on').cast('string'))
        .withColumn("table_key",f.col('table_name'))
        .withColumn("archived_on_list", f.collect_list("archived_on").over(window_spec))
        .groupBy('table_key')
        .agg(
        f.max("archived_on_list").alias("archived_versions"),
        f.max("archived_on").alias("latest_archive_version")
        )
        .rdd.map(
        lambda row: {
            row["table_key"]: {
                "latest_archive_version": row["latest_archive_version"],
                "archived_versions": row["archived_versions"],

                }
            }
        )
    .collect()
    )   

    # Flatten the list of dictionaries into a single dictionary
    final_result = {k: v for d in result for k, v in d.items()}

    write_json_file(final_result, path)