{
 "cells": [
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
     "nuid": "f28ea66a-3fef-473d-b277-9d0cc7a127bf",
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
     "nuid": "21f19061-dce8-445f-8478-6a04f683f044",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%run ./parameters"
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
     "nuid": "0266001d-6572-4988-9703-f6923430575b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as f, DataFrame\n",
    "from pyspark.sql.window import Window\n",
    "from functions import load_table, save_table, read_csv_file, map_column_values\n",
    "from functools import reduce"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0b78199d-409c-4449-88c6-9f9d0f479401",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 1 Load table"
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
     "nuid": "cae4f815-3521-4954-bd11-02eb6dcd08bd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_filtered = load_table('cohort_filtered')\n",
    "display(cohort_filtered.limit(100))\n",
    "\n",
    "covid_positive = load_table('covid_positive')\n",
    "display(covid_positive.limit(100))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "091f5be2-ab6b-446a-bddc-e7a2b9dde071",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 2 Prepare datasets"
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
     "nuid": "6e5d9983-413b-4aec-aed3-badc97dffff2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "covid_positive_source_priority = {\n",
    "    'sgss': 1,\n",
    "    'pillar_2': 2,\n",
    "    'hes_apc_any_diagnosis': 3,\n",
    "    'gdppr': 4\n",
    "}\n",
    "\n",
    "covid_positive_prepared = (\n",
    "    covid_positive\n",
    "    .filter(f.col('data_source').isin(list(covid_positive_source_priority.keys())))\n",
    "    .transform(\n",
    "        map_column_values,\n",
    "        map_dict=covid_positive_source_priority,\n",
    "        column='data_source', new_column='source_priority'\n",
    "    )\n",
    ")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "db72ef0f-1f3c-493b-b372-7d93e0cbee54",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 3 Cohort dates"
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
     "nuid": "7d4c9968-a369-4587-a757-294094e34199",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_prepared = (\n",
    "    cohort_filtered\n",
    "    .select(\n",
    "        'person_id',\n",
    "        f.date_sub('index_mi_date', 14).alias('min_date'),\n",
    "        f.col('index_mi_date').alias('max_date')\n",
    "    )\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3d953f6b-a443-4dd4-a70d-070af73ec5b2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 4 Match to cohort"
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
     "nuid": "820b555d-9e1b-4b34-9017-c736becddde5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "covid_positive_all_events = (\n",
    "    covid_positive_prepared\n",
    "    .join(\n",
    "        cohort_prepared,\n",
    "        on='person_id', how='inner'\n",
    "    )\n",
    "    .filter(\"(date >= min_date) AND (date <= max_date)\")\n",
    ")\n",
    "\n",
    "save_table(covid_positive_all_events, 'covid_positive_all_events')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "de8cf5bc-ba0c-4d70-ae51-17d29b8b17ee",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 5 Aggregate"
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
     "nuid": "105b146c-1e0f-4be0-878f-8cd48f2a1b73",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "covid_positive_all_events = load_table('covid_positive_all_events')\n",
    "\n",
    "_win = Window.partitionBy('person_id').orderBy(f.col('date').desc(), 'source_priority')\n",
    "\n",
    "covid_positive_last_event = (\n",
    "    covid_positive_all_events\n",
    "    .withColumn('rank', f.row_number().over(_win))\n",
    "    .filter('rank = 1')\n",
    "    .withColumn('flag', f.lit(1))\n",
    ")\n",
    "\n",
    "covid_positive_last_event = (\n",
    "    covid_positive_last_event\n",
    "    .select(\n",
    "        'person_id',\n",
    "        f.col('flag').alias('covid_positive_flag'),\n",
    "        f.col('date').alias('covid_positive_date'),\n",
    "        f.col('data_source').alias('covid_positive_source')\n",
    "    )\n",
    ")\n",
    "\n",
    "save_table(covid_positive_last_event, 'covid_positive_last_event')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "517e12d6-aa83-4c83-9aff-1deb16253c4f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Save table"
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
     "nuid": "a4f87f51-147f-473c-88c8-1e0209d0f8b4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_filtered = load_table('cohort_filtered')\n",
    "covid_positive_last_event = load_table('covid_positive_last_event')\n",
    "\n",
    "cohort_covid_infection = (\n",
    "    cohort_filtered\n",
    "    .select('person_id')\n",
    "    .join(\n",
    "        covid_positive_last_event,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "    \n",
    "save_table(cohort_covid_infection, 'cohort_covid_infection')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a5632d6a-eb14-49ee-8357-eb8216f4c61a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 7 Display"
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
     "nuid": "a710da59-48e2-4e3f-8b2d-cd11cddde4f1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_covid_infection = load_table('cohort_covid_infection')\n",
    "display(cohort_covid_infection.limit(100))"
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
   "notebookName": "D09d-covid_infection",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
