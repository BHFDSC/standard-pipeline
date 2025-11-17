"""
Module name: environment_utils.py.

This module provides utilities for managing the environment including path resolution and Spark session management.

"""
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


def resolve_path(path: str, repo: str = None) -> str:
    """
    Resolve the path to a file, handling different formats and repositories.

    Args:
    - path (str): Path to the file. Can be an absolute path, a relative path with './', or a path within a repo.
    - repo (str): Name of the repo if the path is relative within a repo.

    Returns:
    - resolved_path (str): Resolved absolute path to the file.
    """
    assert (
        (os.path.isabs(path) and repo is None) or 
        (path.startswith("./") and repo is None) or 
        (repo is not None and not (path.startswith("./") or os.path.isabs(path)))
    ), "Specify either an absolute path, a relative path with './', or a path within a repo, not a combination of them."

    if path.startswith("./"):
        project_folder = os.environ.get('PROJECT_FOLDER', None)
        assert project_folder is not None, "Environment variable 'PROJECT_FOLDER' not set. Ensure './project_setup' is run at the start of the notebook."
        resolved_path = os.path.join(project_folder, path[2:])
    elif repo is not None:
        resolved_path = pkg_resources.resource_filename(repo, path)
    else:
        resolved_path = path

    return resolved_path
