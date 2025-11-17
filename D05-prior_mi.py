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
    "# 1 Load tables\n",
    "\n",
    "Refer to Table Directory notebook to understand the shorthand names for the tables being loaded."
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
    "# This notebook establishes the prior history of a diagnosis/medical event - so begins with hes and gdppr tables where medical history is most complete\n",
    "\n",
    "cohort_demographics = load_table('cohort_demographics')\n",
    "hes_apc_diagnosis = load_table('hes_apc_diagnosis')\n",
    "gdppr = load_table('gdppr', method = 'gdppr')\n",
    "\n",
    "display(cohort_demographics.limit(50))\n",
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
    "A csv style file (or notebook that can be converted into csv) should exist for this in the codelists folder. It should be a list of codes (or labels) in a given coding system (icd10, snomed, etc) that define a concept of interest - eg myocardial infarction aka MI. This section reads them in and reformats them for further use."
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
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_snomed = {\n",
    "    \"prior_mi\": \"./codelists/prior_myocardial_infarction_snomed.csv\",\n",
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
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_icd10 = {\n",
    "    \"prior_mi\": \"./codelists/prior_myocardial_infarction_icd10.csv\",\n",
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
    "# 3 Prepare dataset\n",
    "\n",
    "Here we're looking at medical history from two sources, so we need to make sure the source table is labelled, and the format is compatible to combine the tables together."
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
    "hes_apc_prepared = (\n",
    "    hes_apc_diagnosis\n",
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
    "# This notebook is looking at history of a condition BEFORE the study start. Their history can only occur between being born and the study start date. This code prepares that criteria to be applied later\n",
    "\n",
    "cohort_prepared = (\n",
    "    cohort_demographics\n",
    "    .select(\n",
    "        'person_id',\n",
    "        f.col('date_of_birth').alias('min_date'),\n",
    "        f.date_sub('cohort_entry_start_date', 1).alias('max_date')\n",
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
    "# Broadcast joins are joins used in situations where on very short list is being compared/joined with another very long list. Codelists are typically shorter (<< 1k items) and primary care sets are typically very long (in the billions), so this is the most efficient approach.\n",
    "\n",
    "# Here the code is retrieving (inner matching) only the primary care records with the relevant disease/event of interest (eg MI)\n",
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
    ")\n",
    "\n",
    "# The filtering is to make sure the record are stricly 'prior' to the study start date."
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
    "# And a similar approach is taken for records sourced from hospital care rather than primary care\n",
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
    "# Records from both primary and hospital care are combined (appended) here into one longer table\n",
    "\n",
    "prior_mi_events = (\n",
    "    gdppr_matched\n",
    "    .unionByName(hes_apc_matched)\n",
    ")\n",
    "\n",
    "# This previous steps, especially as they involve two very large datasets, can be compute-intensive so this is a good point to save to the database to avoid having to recompute them with each visit to the notebook\n",
    "save_table(prior_mi_events, 'prior_mi_events')"
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
    "prior_mi_events = load_table('prior_mi_events')\n",
    "\n",
    "# We have previously defined primary care sourced records as priority '1' and hospital as priority '2'\n",
    "_win = Window.partitionBy('person_id').orderBy('date', 'source_priority')\n",
    "\n",
    "# Our starting table here might have multiple records per patient, but as with most projects we will eventually need this at patient level (one row per patient). We therefore prioritise the earliest, and if multiple still, those from gdppr\n",
    "\n",
    "prior_mi_earliest = (\n",
    "    prior_mi_events\n",
    "    .withColumn('rank', f.row_number().over(_win))\n",
    "    .filter('rank = 1')\n",
    "    .withColumn('flag', f.lit(1))\n",
    ")\n",
    "\n",
    "prior_mi_earliest = (\n",
    "    prior_mi_earliest\n",
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
    "save_table(prior_mi_earliest, 'prior_mi_earliest')"
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
    "# 8 Match with cohort"
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
    "# We now retrieve the cohort from earlier to add the relevant prior_MI information columns to the respective individuals\n",
    "\n",
    "cohort_demographics = load_table('cohort_demographics')\n",
    "prior_mi_earliest = load_table('prior_mi_earliest')\n",
    "\n",
    "cohort_prior_mi = (\n",
    "    cohort_demographics\n",
    "    .join(\n",
    "        prior_mi_earliest,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "    \n",
    "save_table(cohort_prior_mi, 'cohort_prior_mi')"
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
     "nuid": "e1788e16-5332-4639-89f2-d8e49777451e",
     "showTitle": false,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{},\"columnVisibility\":{}},\"settings\":{\"columns\":{}},\"syncTimestamp\":1752071695256}",
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
    "cohort_prior_mi = load_table('cohort_prior_mi')\n",
    "display(cohort_prior_mi.limit(250))"
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
   "notebookName": "D05-prior_mi",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
