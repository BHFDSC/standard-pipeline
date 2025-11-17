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
     "nuid": "8b6751fc-6d6e-472a-b1c7-f01fa5b6f9bc",
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
     "nuid": "57ac30dd-57d2-4a58-a669-aaae38136a86",
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
     "nuid": "eced1af3-4af5-48a0-8d82-2b84225ef773",
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
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "95f3ebc0-24ce-4302-a8ee-97618ca092d9",
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
     "nuid": "c1aed98f-34c0-47e2-b19c-552c37960174",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_index_mi = load_table('cohort_index_mi')\n",
    "lsoa_multisource = load_table('lsoa_multisource')\n",
    "\n",
    "display(cohort_index_mi.limit(50))\n",
    "display(lsoa_multisource.limit(50))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "37d71303-3cd5-4770-bd59-0bd0d121bf29",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 2 Load mapping files"
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
     "nuid": "b3bac873-a865-4aef-a628-d644f525307c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "mapping_lsoa_imd = (\n",
    "    read_csv_file('./mapping_files/lsoa11_imd19_OSGB1936.csv')\n",
    "    .select(\n",
    "        f.col('lsoa11cd').alias('lsoa'),\n",
    "        f.col('IMDDecil').alias('imd_decile')\n",
    "    )\n",
    ")\n",
    "\n",
    "display(mapping_lsoa_imd)"
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
     "nuid": "425f7aff-dc4c-45bc-895a-65babefbce62",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "mapping_lsoa_region = (\n",
    "    read_csv_file('./mapping_files/lsoa11_buasd11_bua11_rgn11_best_fit_lookup_england_wales.csv')\n",
    "    .select(\n",
    "        f.col('LSOA11CD').alias('lsoa'),\n",
    "        f.col('RGN11NM').alias('region')\n",
    "    )\n",
    ")\n",
    "\n",
    "display(mapping_lsoa_region)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2fc20e87-a71c-4265-bc8d-4eda6836e7a3",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 3 Prepare LSOA multisource"
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
     "nuid": "3dd00178-186e-4477-aa07-684658aac1b2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Needed in case sources from around the same time disagree on the residence of the individual, then \n",
    "lsoa_source_priority = {\n",
    "    'gdppr': 1,\n",
    "    'hes_apc': 2,\n",
    "    'hes_op': 3,\n",
    "    'hes_ae': 4\n",
    "}\n",
    "\n",
    "lsoa_prepared = (\n",
    "    lsoa_multisource\n",
    "    .filter(f.col('data_source').isin(list(lsoa_source_priority.keys())))\n",
    "    .transform(\n",
    "        map_column_values,\n",
    "        map_dict=lsoa_source_priority,\n",
    "        column='data_source', new_column='source_priority'\n",
    "    )\n",
    ")\n",
    "\n",
    "display(lsoa_prepared.limit(100))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b41a9d26-ab96-425f-9e67-26f0cc2ee32e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 4 Prepare Cohort Dates"
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
     "nuid": "aa279675-a75c-4b72-9bc2-d095066d94f3",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_prepared = (\n",
    "    cohort_index_mi\n",
    "    .select(\n",
    "        'person_id',\n",
    "        f.col('index_mi_date').alias('target_date'),\n",
    "        f.date_sub('index_mi_date', 365).alias('min_date'),\n",
    "        f.date_add('index_mi_date', 3*30).alias('max_date')\n",
    "    )\n",
    "    .filter(\"target_date IS NOT NULL\")\n",
    ")\n",
    "\n",
    "display(cohort_prepared.limit(100))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0eb16b26-529f-4a0e-b5c0-3c40496ed61b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 5 Join, filter and aggregate"
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
     "nuid": "6d36a46a-6207-4723-bb4c-5ad08a0afc9b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "lsoa_joined = (\n",
    "    cohort_prepared\n",
    "    .join(\n",
    "        lsoa_prepared,\n",
    "        on='person_id', how='inner'\n",
    "    )\n",
    ")\n",
    "\n",
    "lsoa_filtered = (\n",
    "    lsoa_joined\n",
    "    .filter(f.col('record_date').between(f.col('min_date'), f.col('max_date')))\n",
    ")\n",
    "\n",
    "_window = Window.partitionBy('person_id').orderBy('abs_date_diff', 'source_priority')\n",
    "\n",
    "lsoa_closest = (\n",
    "    lsoa_filtered\n",
    "    .withColumn('date_diff', f.datediff('record_date', 'target_date'))\n",
    "    .withColumn('abs_date_diff', f.abs(f.col('date_diff')))\n",
    "    .withColumn('rank', f.rank().over(_window))\n",
    "    .filter(f.col('rank') == 1)\n",
    ")\n",
    "\n",
    "lsoa_selected = (\n",
    "    lsoa_closest\n",
    "    .select(\n",
    "        'person_id', 'lsoa',\n",
    "        f.col('data_source').alias('lsoa_source'),\n",
    "        f.col('record_date').alias('lsoa_date')\n",
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
     "nuid": "105b0531-a1c1-41ee-87d6-727476ab8c16",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 6 Join IMD and Region"
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
     "nuid": "dcf73cfd-706c-459a-acdc-96eea8efec19",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "lsoa_mapped = (\n",
    "    lsoa_selected\n",
    "    .join(\n",
    "        f.broadcast(mapping_lsoa_imd),\n",
    "        on='lsoa', how='left' \n",
    "    )\n",
    "    .join(\n",
    "        f.broadcast(mapping_lsoa_region),\n",
    "        on='lsoa', how='left' \n",
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
     "nuid": "57402713-c30c-4719-a660-9b5263b3a10d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 7 Append to cohort"
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
     "nuid": "b838439a-437e-496c-8d18-94d95f195d93",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "cohort_lsoa_imd_region = (\n",
    "    cohort_index_mi\n",
    "    .join(\n",
    "        lsoa_mapped,\n",
    "        on='person_id', how='left'\n",
    "    )\n",
    ")\n",
    "\n",
    "save_table(cohort_lsoa_imd_region, 'cohort_lsoa_imd_region')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "6163daf0-7bda-41d6-89b1-e5a9f891e189",
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
     "nuid": "8ae97d88-ad11-4985-9073-130232b811a4",
     "showTitle": false,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{},\"columnVisibility\":{}},\"settings\":{\"columns\":{}},\"syncTimestamp\":1752071899839}",
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
    "cohort_lsoa_imd_region = load_table('cohort_lsoa_imd_region')\n",
    "display(cohort_lsoa_imd_region.limit(1000))"
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
   "notebookName": "D07-lsoa_region_imd",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
