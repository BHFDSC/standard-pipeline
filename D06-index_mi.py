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
     "nuid": "217c62c5-2081-455b-ba1b-9612aef664dc",
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
     "nuid": "d68f101b-ffba-4961-8955-28f8f84abddf",
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
     "nuid": "8fe701cd-7e83-442b-a676-78cce7c13834",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as f, DataFrame\n",
    "from pyspark.sql.window import Window\n",
    "from functions import load_table, save_table, read_csv_file\n",
    "from functools import reduce"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "db058032-10d6-4621-8afe-86c754a526cc",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 1 Load tables"
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
     "nuid": "d661da4d-31ae-4cfb-8bdc-362d5e27a073",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_prior_mi = load_table('cohort_prior_mi')\n",
    "hes_apc_diagnosis = load_table('hes_apc_diagnosis')\n",
    "gdppr = load_table('gdppr', method = 'gdppr')\n",
    "\n",
    "display(cohort_prior_mi.limit(50))\n",
    "display(hes_apc_diagnosis.limit(50))\n",
    "display(gdppr.limit(50))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "92b5ef3a-2148-4aeb-bb36-eb037efd5ab4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 2 Prepare Codelists\n",
    "\n",
    "Codelists, lists of labels from coding systems like snomed or icd10 used to describe a disease or event, are generally agreed upon outside the SDE. They are then copied into a seperate file (see codelists folder). The below reads in these prepared codelists as they'll be needed for this curation notebook."
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
     "nuid": "faefb5c4-028f-4197-a9f8-a333b336671a",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "snomed"
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_snomed = {\n",
    "    \"index_mi\": \"./codelists/incident_myocardial_infarction_snomed.csv\",\n",
    "}\n",
    "\n",
    "list_codelists_snomed = [\n",
    "    read_csv_file(codelist_path)\n",
    "    .withColumn('phenotype', f.lit(phenotype))\n",
    "    for phenotype, codelist_path in dict_codelists_snomed.items()\n",
    "]\n",
    "\n",
    "codelist_snomed = reduce(DataFrame.unionByName, list_codelists_snomed)\n",
    "\n",
    "display(codelist_snomed)\n"
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
     "nuid": "d1a3fdc2-6405-40d1-ad1f-0bc9c975b1de",
     "showTitle": true,
     "tableResultSettingsMap": {},
     "title": "icd10"
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_icd10 = {\n",
    "    \"index_mi\": \"./codelists/incident_myocardial_infarction_icd10.csv\",\n",
    "}\n",
    "\n",
    "list_codelists_icd10 = [\n",
    "    read_csv_file(codelist_path)\n",
    "    .withColumn('phenotype', f.lit(phenotype))\n",
    "    for phenotype, codelist_path in dict_codelists_icd10.items()\n",
    "]\n",
    "\n",
    "codelist_icd10 = reduce(DataFrame.unionByName, list_codelists_icd10)\n",
    "\n",
    "display(codelist_icd10)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9bba22da-116a-4f71-9e90-0c079929802e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 3 Prepare dataset"
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
     "nuid": "5e5c75e5-3800-4539-94bc-4a450ab240c8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# GDPPR can be a very long set so limiting to only the cols needed here is sensible. We'll also be sourcing data from other datasets and harmonising, so preparing flags to show where the records have come from helps with this.\n",
    "\n",
    "gdppr_prepared = (\n",
    "    gdppr\n",
    "    .select(\n",
    "        'person_id', 'date', 'code',\n",
    "        f.lit('gdppr').alias('data_source'),\n",
    "        f.lit(1).alias('source_priority')\n",
    "    )\n",
    ")"
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
     "nuid": "9b49d8f3-63a3-46d2-a421-10d4cba8df5e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Similar for hospital data from hes, we need the columns to have the same names before appending with gdppr, and we want to know which source each record has come from so we prepare that col in advance.\n",
    "\n",
    "hes_apc_prepared = (\n",
    "    hes_apc_diagnosis\n",
    "    .filter(\"diag_position = 1\")\n",
    "    .select(\n",
    "        'person_id', 'code',\n",
    "        f.col('epistart').alias('date'),\n",
    "        f.lit('hes_apc').alias('data_source'),\n",
    "        f.lit(2).alias('source_priority')\n",
    "    )\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "34d99506-975a-4475-968b-7a3156680a19",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 4 Prepare cohort dates"
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
     "nuid": "4414899c-189b-4b10-b900-2620cd2970d5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# The study window will vary between projects and will be started in the parameters of a project ease of use, reference and update. Limiting the data to the eligible window early on in the code helps the code run quicker.\n",
    "\n",
    "cohort_prepared = (\n",
    "    cohort_prior_mi\n",
    "    .select(\n",
    "        'person_id',\n",
    "        f.col('cohort_entry_start_date').alias('min_date'),\n",
    "        f.col('cohort_entry_end_date').alias('max_date')\n",
    "    )\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b68891dc-c1fd-41ba-9f5f-e3ca75261f5d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 5 Perform matching"
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
     "nuid": "e90e18f3-8a70-47a9-8839-d904e53a281f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Here we take the codelists of codes we are interested in for this study, find only the records in primary care (gdppr) that match, join this to the cohort we've already defined thus far and a limit to only the study window.\n",
    "\n",
    "gdppr_matched = (\n",
    "    gdppr_prepared\n",
    "    .join(\n",
    "        f.broadcast(codelist_snomed),\n",
    "        on='code', how='inner'\n",
    "    )\n",
    "    .join(\n",
    "        cohort_prepared,\n",
    "        on='person_id', how='inner'\n",
    "    )\n",
    "    .filter(\"(date >= min_date) AND (date <= max_date)\")\n",
    ")"
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
     "nuid": "05659766-3c3e-4219-aad9-c17454a94078",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Similar for HES as above but with icd10 codes rather than snomed\n",
    "\n",
    "hes_apc_matched = (\n",
    "    hes_apc_prepared\n",
    "    .join(\n",
    "        f.broadcast(codelist_icd10),\n",
    "        on='code', how='inner'\n",
    "    )\n",
    "    .join(\n",
    "        cohort_prepared,\n",
    "        on='person_id', how='inner'\n",
    "    )\n",
    "    .filter(\"(date >= min_date) AND (date <= max_date)\")\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9d89ca45-dce3-47b5-a0b6-262ebcd4c3b8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Combine"
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
     "nuid": "91146166-1dde-4e89-af86-2182d5ac6d71",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# We can then append the records from both sources, so we can see the chronology of MI events when collated from both primary care and secondary care sources. Note this will now be at event level (1 row per event), rather than patient level, so we may see multiple records per patient.\n",
    "\n",
    "index_mi_events = (\n",
    "    gdppr_matched\n",
    "    .unionByName(hes_apc_matched)\n",
    ")\n",
    "\n",
    "save_table(index_mi_events, 'index_mi_events')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9bcc9517-41f5-42ba-bdcd-de762f7bbd36",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 7 Aggregate"
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
     "nuid": "3e6fb836-f9e6-460a-8e9f-7cdfd76ea2f1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# We're interested in the earliest MI event, so for each patient we need to sort their records choronologically and then take the first record. In normal computing this would be a groupBy, but with distributed computed we nee the 'window.partitionBy' function, thus making sure one patients record is not handled by more than one computer.\n",
    "\n",
    "index_mi_events = load_table('index_mi_events')\n",
    "\n",
    "_win = Window.partitionBy('person_id').orderBy('date', 'source_priority')\n",
    "\n",
    "index_mi_earliest = (\n",
    "    index_mi_events\n",
    "    .withColumn('rank', f.row_number().over(_win))\n",
    "    .filter('rank = 1')\n",
    "    .withColumn('flag', f.lit(1))\n",
    ")\n",
    "\n",
    "index_mi_earliest = (\n",
    "    index_mi_earliest\n",
    "    .groupBy('person_id')\n",
    "    .pivot('phenotype')\n",
    "    .agg(\n",
    "        f.first('flag').alias('flag'),\n",
    "        f.first('date').alias('date'),\n",
    "        f.first('code').alias('code'),\n",
    "        f.first('data_source').alias('source')\n",
    "    )\n",
    ")\n",
    "\n",
    "save_table(index_mi_earliest, 'index_mi_earliest')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "60f4a1d7-8822-45e1-8dd7-2a37917e9eef",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 8 Append to cohort"
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
     "nuid": "04b7bd9b-71e8-4a60-8910-5107be7d94ee",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_prior_mi = load_table('cohort_prior_mi')\n",
    "index_mi_earliest = load_table('index_mi_earliest')\n",
    "\n",
    "cohort_index_mi = (\n",
    "    cohort_prior_mi\n",
    "    .join(\n",
    "        index_mi_earliest,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "    \n",
    "save_table(cohort_index_mi, 'cohort_index_mi')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "95d0aa5e-e286-47a1-aca6-bc94c6ede5ea",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 9 Display"
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
     "nuid": "77e7a4eb-ffa1-4eed-b7f5-e7d7508f0b79",
     "showTitle": false,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{},\"columnVisibility\":{}},\"settings\":{\"columns\":{}},\"syncTimestamp\":1752074766100}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 0
      }
     },
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_index_mi = load_table('cohort_index_mi')\n",
    "display(cohort_index_mi.limit(500))"
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
   "notebookName": "D06-index_mi",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
