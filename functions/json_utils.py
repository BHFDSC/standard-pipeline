"""
Module name: json_utils.py

Description:
    This module provides utilities for reading and writing JSON files in Python.
"""
import json
import os
from typing import Any, Dict
from .environment_utils import resolve_path

def read_json_file(path: str, repo: str = None) -> Dict[str, Any]:
    """
    Read a JSON file from the specified path and return its content as a Python dictionary. 

    This function reads a JSON file in binary mode from the specified path and returns its content as a Python dictionary. 
    It checks for duplicate keys within the JSON object and raises a ValueError if duplicates are found.

    Args:
        path (str): Path to the JSON file. Can be an absolute path, a relative path with './', or a path within a repo.
        repo (str): Name of the repo if the path is relative within a repo.

    Returns:
        dict: A Python dictionary containing the JSON content.

    Raises:
        ValueError: If the JSON file contains duplicate keys.

    Examples:
        >>> data = read_json_file('./relative/path/in/project.json')
        >>> data = read_json_file('/Workspace/absolute/path.json')
        >>> data = read_json_file(path='path/in/repo.json', repo='common_repo')
    """

    def check_json_for_duplicate_keys(ordered_pairs):
        """
        Reject duplicate keys while loading JSON content.

        Args:
            ordered_pairs: A list of key-value pairs from the JSON object.

        Returns:
            dict: A dictionary representation of the JSON object.

        Raises:
            ValueError: If duplicate keys are found.
        """
        d = {}
        for k, v in ordered_pairs:
            if k in d:
                raise ValueError(f"JSON file '{resolved_path}' contains duplicate key: {k}")
            else:
                d[k] = v
        return d

    # Resolve the path
    resolved_path = resolve_path(path, repo)
    
    # Load JSON file and check for duplicate keys
    with open(resolved_path) as json_file:
        json_dict = json.load(json_file, object_pairs_hook=check_json_for_duplicate_keys)

    return json_dict


def write_json_file(data: Dict[str, Any], path: str, repo: str = None, indent: int = 4) -> None:
    """
    Write a Python dictionary to a JSON file at the specified path.

    This function writes the contents of a Python dictionary to a JSON file at the specified path. 
    The JSON object is formatted with optional indentation for readability.

    Args:
        data (dict): The Python dictionary to be written to the JSON file.
        path (str): Path to the JSON file. Can be an absolute path, a relative path with './', or a path within a repo.
        repo (str): Name of the repo if the path is relative within a repo.
        indent (int, optional): The number of spaces used for indentation (default is 4).

    Returns:
        None

    Examples:
        >>> data = {'key1': 'value1', 'key2': 'value2'}
        >>> write_json_file(data, "./in_project_folder.json")
        >>> write_json_file(data, "/Workspace/absolute_path.json")
        >>> write_json_file(data, path="in/shared/repo.json", repo="common_repo")
    """
    # Resolve the path
    resolved_path = resolve_path(path, repo)

    # Check if the directory exists
    directory = os.path.dirname(resolved_path)
    if not os.path.exists(directory):
        raise ValueError(f"Directory '{directory}' does not exist.")

    # Write file
    with open(resolved_path, 'w') as fp:
        json.dump(data, fp, indent=indent)
