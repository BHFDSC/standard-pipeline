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
     "nuid": "c5299322-a359-4b7b-8a68-d35ba8e261cf",
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
    "hes_apc_procedure = load_table('hes_apc_procedure')\n",
    "display(hes_apc_procedure.limit(100))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1911b7c3-b109-4699-ae18-b84d7b72db8e",
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
     "nuid": "80d80f4f-fbb9-46c5-bf80-e63c54da153e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dict_codelists_opcs = {\n",
    "    \"post_mi_pci\": \"./codelists/percutaneous_coronary_intervention_opcs4.csv\",\n",
    "    \"post_mi_cabg\": \"./codelists/coronary_artery_bypass_grafts_opcs4.csv\"\n",
    "}\n",
    "\n",
    "list_codelists_opcs = [\n",
    "    read_csv_file(codelist_path)\n",
    "    .withColumn('phenotype', f.lit(phenotype))\n",
    "    for phenotype, codelist_path in dict_codelists_opcs.items()\n",
    "]\n",
    "\n",
    "codelist_opcs = spark.createDataFrame(union_dataframe_list(list_codelists_opcs))\n",
    "\n",
    "display(codelist_opcs)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f3e26513-b953-47ec-9cd7-e5d33f70b6f2",
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
     "nuid": "6e5d9983-413b-4aec-aed3-badc97dffff2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "hes_apc_procedure_prepared = (\n",
    "    hes_apc_procedure\n",
    "    .select(\n",
    "        'person_id', 'code',\n",
    "        f.col('procedure_date').alias('date'),\n",
    "        f.lit('hes_apc_procedure').alias('data_source'),\n",
    "        f.lit(1).alias('source_priority')\n",
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
     "nuid": "7fba0453-3971-4c50-87ba-f610de2ed557",
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
    "        f.date_add('index_mi_date', 1).alias('min_date'),\n",
    "        f.least(f.col('date_of_death'), f.col('follow_up_end_date')).alias('max_date')\n",
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
     "nuid": "80f41a89-0fb9-4ca9-8ac8-91e8f71605c7",
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
     "nuid": "820b555d-9e1b-4b34-9017-c736becddde5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "hes_apc_procedure_matched = (\n",
    "    hes_apc_procedure_prepared\n",
    "    .join(\n",
    "        f.broadcast(codelist_opcs),\n",
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
     "nuid": "3cd8886e-ed46-4c34-af44-a6921ef95538",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "outcomes_all_events = (\n",
    "    hes_apc_procedure_matched\n",
    ")\n",
    "\n",
    "save_table(outcomes_all_events, 'outcomes_all_events')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b24fff31-b3d2-4ade-9bec-e9333398c702",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Aggregate"
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
    "outcomes_all_events = load_table('outcomes_all_events')\n",
    "\n",
    "_win = Window.partitionBy('person_id', 'phenotype').orderBy(f.col('date').asc(), 'source_priority')\n",
    "\n",
    "outcomes_first_event = (\n",
    "    outcomes_all_events\n",
    "    .withColumn('rank', f.row_number().over(_win))\n",
    "    .filter('rank = 1')\n",
    "    .withColumn('flag', f.lit(1))\n",
    ")\n",
    "\n",
    "outcomes_first_event = (\n",
    "    outcomes_first_event\n",
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
    "save_table(outcomes_first_event, 'outcomes_first_event')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "fab5e9fc-717a-4f39-88c6-2b3b45868e30",
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
     "nuid": "a4f87f51-147f-473c-88c8-1e0209d0f8b4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_filtered = load_table('cohort_filtered')\n",
    "outcomes_first_event = load_table('outcomes_first_event')\n",
    "\n",
    "cohort_outcomes = (\n",
    "    cohort_filtered\n",
    "    .select('person_id')\n",
    "    .join(\n",
    "        outcomes_first_event,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "    \n",
    "save_table(cohort_outcomes, 'cohort_outcomes')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3fd7e221-d68b-49fd-ace2-0e73da8a3bed",
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
     "nuid": "a710da59-48e2-4e3f-8b2d-cd11cddde4f1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_outcomes = load_table('cohort_outcomes')\n",
    "display(cohort_outcomes.limit(100))"
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
   "notebookName": "D10-outcomes",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
