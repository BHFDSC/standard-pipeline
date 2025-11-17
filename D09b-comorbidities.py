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
    "from functions.functions import union_dataframe_list"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8a08a5be-a64f-45e1-81d7-4be0f6341c6e",
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
    "gdppr = load_table('gdppr', method='gdppr')\n",
    "display(gdppr.limit(100))\n",
    "\n",
    "hes_apc_diagnosis = load_table('hes_apc_diagnosis')\n",
    "display(hes_apc_diagnosis.limit(100))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b876ddfa-6a74-4a1b-a6de-a21b7c91b4cb",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 2 Codelists"
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
     "nuid": "9e6f95c5-b7cc-4094-954c-803562f8fcbe",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_snomed = {\n",
    "    \"diabetes\": \"./codelists/diabetes_snomed.csv\",\n",
    "    \"hypertension\": \"./codelists/hypertension_snomed.csv\",\n",
    "}\n",
    "\n",
    "list_codelists_snomed = [\n",
    "    read_csv_file(codelist_path)\n",
    "    .withColumn('phenotype', f.lit(phenotype))\n",
    "    for phenotype, codelist_path in dict_codelists_snomed.items()\n",
    "]\n",
    "\n",
    "codelist_snomed = spark.createDataFrame(union_dataframe_list(list_codelists_snomed))\n",
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
     "nuid": "2e639a9c-3fac-431e-8564-977502622f74",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_icd10 = {\n",
    "    \"diabetes\": \"./codelists/diabetes_icd10.csv\",\n",
    "    \"hypertension\": \"./codelists/hypertension_icd10.csv\",\n",
    "}\n",
    "\n",
    "list_codelists_icd10 = [\n",
    "    read_csv_file(codelist_path)\n",
    "    .withColumn('phenotype', f.lit(phenotype))\n",
    "    for phenotype, codelist_path in dict_codelists_icd10.items()\n",
    "]\n",
    "\n",
    "codelist_icd10 = spark.createDataFrame(union_dataframe_list(list_codelists_icd10))\n",
    "\n",
    "display(codelist_icd10)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3963b037-ca42-4cea-9a88-25a6a51bcb24",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 3 Prepare datasets"
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
     "nuid": "1f8a7bae-ad03-4eff-b52e-55cb864554d3",
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
     "nuid": "0070fb72-0287-4e6c-a501-c0e5ca49eb23",
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
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1f85eb76-b72e-4ec6-b1cb-7ded6d105604",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 4 Cohort dates"
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
     "nuid": "02736eaf-b937-4531-925c-83029430bfca",
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
    "        f.col('date_of_birth').alias('min_date'),\n",
    "        f.date_sub('index_mi_date', 1).alias('max_date')\n",
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
     "nuid": "ca2eb23c-4b9a-4132-ba63-0afec81aa957",
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
     "nuid": "21c855ec-d47a-426c-8242-ebf20cb35c58",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
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
     "nuid": "5a9f4d99-32f3-438b-a190-d2c708e45240",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
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
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2d9b06f7-1dd7-4da7-ac3b-c1a8c257d0d8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Combine matches"
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
     "nuid": "3cd8886e-ed46-4c34-af44-a6921ef95538",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "comorbs_all_events = (\n",
    "    gdppr_matched\n",
    "    .unionByName(hes_apc_matched)\n",
    ")\n",
    "\n",
    "save_table(comorbs_all_events, 'comorbs_all_events')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "693ae71c-65e6-4dbb-afb8-557cdd01b400",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 7 Aggregate and pivot wide"
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
    "comorbs_all_events = load_table('comorbs_all_events')\n",
    "\n",
    "_win = Window.partitionBy('person_id', 'phenotype').orderBy(f.col('date').desc(), 'source_priority')\n",
    "\n",
    "comorbs_last_event = (\n",
    "    comorbs_all_events\n",
    "    .withColumn('rank', f.row_number().over(_win))\n",
    "    .filter('rank = 1')\n",
    "    .withColumn('flag', f.lit(1))\n",
    ")\n",
    "\n",
    "comorbs_last_event = (\n",
    "    comorbs_last_event\n",
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
    "save_table(comorbs_last_event, 'comorbs_last_event')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "88bb2660-15f9-4188-a7ce-f1b49206e646",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 8 Save table"
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
    "comorbs_last_event = load_table('comorbs_last_event')\n",
    "\n",
    "cohort_comorbs = (\n",
    "    cohort_filtered\n",
    "    .select('person_id')\n",
    "    .join(\n",
    "        comorbs_last_event,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "    \n",
    "save_table(cohort_comorbs, 'cohort_comorbs')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8937036b-1d3b-4d3f-ba05-88afd6da8130",
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
     "nuid": "a710da59-48e2-4e3f-8b2d-cd11cddde4f1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_comorbs = load_table('cohort_comorbs')\n",
    "display(cohort_comorbs.limit(100))"
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
   "notebookName": "D09b-comorbidities",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
