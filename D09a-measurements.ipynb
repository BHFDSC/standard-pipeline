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
     "nuid": "268a0a2d-abbf-46dd-9daa-176a23a828f1",
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
    "from functions import load_table, save_table, read_csv_file, apply_inclusion_criteria, map_column_values\n",
    "from functions.functions import union_dataframe_list\n"
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
     "nuid": "fe66ccaf-b336-4fc5-8ae0-27225af069ad",
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
     "nuid": "cc0481ca-a1af-4d3b-b437-0b27affc26ee",
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
    "display(gdppr.limit(100))"
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
     "nuid": "d88cec6e-a247-4434-9e3a-e5cd0be6686e",
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
     "nuid": "86e5d98d-6d54-4c83-9f6d-b5726578dc4b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_snomed = {\n",
    "    \"bmi\": \"./codelists/bmi_snomed.csv\",\n",
    "    \"height\": \"./codelists/height_snomed.csv\",\n",
    "    \"weight\": \"./codelists/weight_snomed.csv\",\n",
    "}\n",
    "\n",
    "dict_min_values = {\n",
    "    \"bmi\": 10,\n",
    "    \"height\": 50,\n",
    "    \"weight\": 25\n",
    "}\n",
    "\n",
    "dict_max_values = {\n",
    "    \"bmi\": 270,\n",
    "    \"height\": 275,\n",
    "    \"weight\": 650\n",
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
    "codelist_snomed = (\n",
    "    codelist_snomed\n",
    "    .transform(\n",
    "        map_column_values,\n",
    "        map_dict=dict_min_values, column='phenotype', new_column='min_value'\n",
    "    )\n",
    "    .transform(\n",
    "        map_column_values,\n",
    "        map_dict=dict_max_values, column='phenotype', new_column='max_value'\n",
    "    )\n",
    ")\n",
    "\n",
    "display(codelist_snomed)\n"
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
     "nuid": "db78ba5c-3b4f-4f8d-af03-4125230801ab",
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
     "nuid": "9e65e255-81df-4d00-b796-2dc832cadc3a",
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
    "        f.col('value1_condition').alias('value')\n",
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
     "nuid": "4a4595d5-6b72-4c21-82b6-e061557079a6",
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
     "nuid": "f359bda8-32d1-4a1b-a253-19035a7d6aff",
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
    "        f.date_sub('index_mi_date', 2*365).alias('min_date'),\n",
    "        f.date_add('index_mi_date', 3*30).alias('max_date'),\n",
    "        f.col('index_mi_date').alias('target_date')\n",
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
     "nuid": "0277ac7a-f38c-4c4d-a742-121547d98f78",
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
     "nuid": "e68f8d7c-5c1b-4e37-86b8-707586fb381b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "measures_code_matched = (\n",
    "    gdppr_prepared\n",
    "    .join(\n",
    "        f.broadcast(codelist_snomed),\n",
    "        on='code', how='inner'\n",
    "    )\n",
    "    .filter(f.col('value').between(f.col('min_value'), f.col('max_value')))\n",
    ")\n",
    "\n",
    "measures_cohort_matched = (\n",
    "    measures_code_matched\n",
    "    .join(\n",
    "        cohort_prepared,\n",
    "        on='person_id', how='inner'\n",
    "    )\n",
    "    .filter(f.col('date').between(f.col('min_date'), f.col('max_date')))\n",
    ")\n"
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
     "nuid": "894194dc-8d9d-4c12-a0d3-8921d4009e9c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Aggregate and pivot wide"
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
     "nuid": "07569bb6-88a4-4f75-b6b6-ec7ebeb917dd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "_window = Window.partitionBy('person_id', 'phenotype').orderBy(f.col('abs_date_diff'))\n",
    "\n",
    "measures_aggregated = (\n",
    "    measures_cohort_matched\n",
    "    .withColumn('abs_date_diff', f.abs(f.datediff('date', 'target_date')))\n",
    "    .withColumn('rank', f.rank().over(_window))\n",
    "    .filter(f.col('rank') == 1)\n",
    ")\n",
    "\n",
    "measures_wide = (\n",
    "    measures_aggregated\n",
    "    .groupBy('person_id')\n",
    "    .pivot('phenotype')\n",
    "    .agg(\n",
    "        f.first('value').alias('value'),\n",
    "        f.first('date').alias('date')\n",
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
     "nuid": "96b4b946-dcd4-455a-9fbc-870dfe6a276e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 7 Save table"
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
     "nuid": "a1a6bccc-9d07-4af6-a78e-822844a76e65",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_measures = (\n",
    "    cohort_filtered\n",
    "    .select('person_id')\n",
    "    .join(measures_wide, on='person_id', how='left')\n",
    ")\n",
    "\n",
    "save_table(cohort_measures, 'cohort_measures')"
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
     "nuid": "0bf6a3b5-0d8d-4c98-98b9-a71489eb67db",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 8 Display"
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
     "nuid": "c2b69b41-f221-4bbc-8fc0-da8a586d5d2d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_measures = load_table('cohort_measures')\n",
    "display(cohort_measures.limit(100))"
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
   "notebookName": "D09a-measurements",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
