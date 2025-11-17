{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "25e15c6a-0477-46ba-bc22-43d57968eb75",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Data Checks\n",
    "\n",
    "This notebook helps you evaluate the **quality and completeness of the datasets you plan to use** by running a series of data checks across available archive versions. It supports key decisions such as selecting an appropriate study start and end date, based on data coverage, consistency, and lag.\n",
    "\n",
    "⚠️ **The archive versions you choose here will be hardcoded in the following parameters notebook and used throughout the rest of the analysis pipeline.**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "569663ad-86e7-4514-8a67-5a13a261fb50",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%run ./project_config"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d08e347d-206f-4dd1-b75f-8de00254359d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from functions.table_monitoring import data_quality_report, get_latest_archive\n",
    "from functions import read_json_file"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a3e0b2e0-de12-44d8-99fa-f4dafef44c49",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "USER INPUT"
    }
   },
   "outputs": [],
   "source": [
    "###############################################################\n",
    "# USER INPUT REQUIRED                                         #\n",
    "# Please list the datasets you will be using in your analysis #\n",
    "# Example: table_name_list = [\"hes_ae\", \"chess\"]              #\n",
    "# You can view all dataset names using:                       #\n",
    "# list((read_json_file(table_mapping_path)).keys())           #\n",
    "###############################################################\n",
    "\n",
    "table_name_list = [\"hes_ae\",\"chess\"]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "a41652b7-a2bd-4feb-be7c-edfbb8891994",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "list((read_json_file(table_mapping_path)).keys())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "6180a262-0b2a-4574-bf39-e4bf8190698e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "get_latest_archive(archive_folders)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e358754d-56c4-41d7-8532-32eb74407968",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "data_quality_report(\n",
    "    table_name_list = table_name_list,\n",
    "    outputs_folder = f'{archived_base_path}/{get_latest_archive(archive_folders)}',\n",
    "    figsize=(20, 20),\n",
    "    include_text = True\n",
    ")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "D01-data_checks",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
