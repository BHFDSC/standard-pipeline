# Health Data Science - Standard Pipeline

This repo contains a standardized data curation and cohort construction pipeline for health research using Databricks/Spark. 
This pipeline replicates processes within the NHSE Secure Data Environment for healthcare data to build epidemiological cohorts for research studies.

## Overview

This project demonstrates a template for building a myocardial infarction (MI) cohort by extracting and combining data from multiple NHS data sources including HES (Hospital Episode Statistics), GDPPR (General Practice data), and other clinical databases.

The pipeline implements best practices for:
- Reproducible cohort construction
- Data quality monitoring
- Codelist-based medical concept identification
- Multi-source data integration

## Features

- **Modular Pipeline Structure**: Step-by-step notebooks for each phase of cohort construction
- **Medical Codelist Integration**: CSV-based SNOMED-CT, ICD-10, OPCS-4, and BNF codelists
- **Data Quality Checks**: Built-in monitoring and validation across archive versions
- **Multi-Source Integration**: Combines HES, GDPPR, CHESS, SGSS, and other NHS datasets
- **Privacy Controls**: Disclosure control and count rounding utilities
- **Reusable Functions**: Comprehensive library of data processing utilities
- **Archive Management**: Consistent data snapshot selection across tables

## Project Structure

```
test-standard-pipeline/
├── parameters.ipynb                      # Study parameters and dates
├── project_config.ipynb                  # Databricks environment setup
├── D03-create_table_directory.py         # Table configuration mapping
├── D01-data_checks.ipynb                 # Data quality evaluation
├── D04-person_id_and_demographics.ipynb  # Cohort and demographics
├── D05-prior_mi.ipynb                    # Prior MI identification
├── D06-index_mi.ipynb                    # Index MI identification
├── D07-lsoa_region_imd.ipynb             # Geographic/socioeconomic data
├── D09a-measurements.ipynb               # BMI, height, weight extraction
├── D09b-comorbidities.ipynb              # Comorbidity identification
├── D09c-medications.ipynb                # Medication history
├── D09d-covid_infection.ipynb            # COVID-19 infection identification
├── D10-outcomes.ipynb                    # Post-MI outcomes (PCI, CABG)
├── D11-combine_tables.ipynb              # Final cohort assembly
│
├── functions/                            # Reusable Python modules
│   ├── functions.py                      # Core utility functions
│   ├── table_management.py               # Table operations
│   ├── table_monitoring.py               # Data quality reporting
│   ├── cohort_construction.py            # Inclusion/exclusion criteria
│   ├── data_aggregation.py               # Aggregation functions
│   ├── data_wrangling.py                 # Data transformation
│   ├── data_privacy.py                   # Disclosure control
│   ├── csv_utils.py                      # CSV operations
│   ├── json_utils.py                     # JSON operations
│   ├── date_functions.py                 # Date utilities
│   ├── environment_utils.py              # Environment setup
│   └── table_summary_management.py       # Summary statistics
│
└── codelists/                            # Medical code lists
    ├── diabetes_snomed.csv
    ├── hypertension_icd10.csv
    ├── prior_myocardial_infarction_snomed.csv
    ├── incident_myocardial_infarction_icd10.csv
    ├── antihypertensives_bnf.csv
    └── [additional codelists...]
```

## Getting Started

### Prerequisites

- Databricks workspace with access to NHS SDE data sources
- Python 3.x
- Apache Spark / PySpark
- Access to required databases:
  - `dars_nic_391419_j3w9t` (raw data)
  - `dars_nic_391419_j3w9t_collab` (collaboration workspace)
  - `dsa_391419_j3w9t_collab` (curated assets)
  - `dss_corporate` (corporate reference data)

### Installation

1. Clone this repository to your Databricks workspace
2. Update database names in `project_config.ipynb` to match your environment
3. Set study parameters in `parameters.ipynb`:
   - `cohort_entry_start_date`
   - `cohort_entry_end_date`
   - `follow_up_end_date`
   - `production_date`
   - `archive_date`

### Configuration

Edit `parameters.ipynb` to set your study parameters:

```python
cohort_entry_start_date = '2020-01-01'
cohort_entry_end_date = '2020-12-31'
follow_up_end_date = '2021-12-31'
production_date = '2025-07-07'
archive_date = '2024-10-24'
```

Update `project_config.ipynb` with your database configurations:

```python
db = 'your_raw_data_database'
dbc = 'your_collab_database'
dsa = 'your_curated_assets_database'
dss = 'your_reference_database'
```

## Usage

### Pipeline Execution Order

Run the notebooks in the following sequence:

1. **Setup**
   - `parameters.ipynb` - Set study parameters
   - `project_config.ipynb` - Configure environment
   - `D03-create_table_directory.py` - Create table mappings

2. **Data Quality**
   - `D01-data_checks.ipynb` - Evaluate data quality and select appropriate archive dates

3. **Cohort Construction**
   - `D04-person_id_and_demographics.ipynb` - Load base cohort and demographics
   - `D05-prior_mi.ipynb` - Identify prior MI events
   - `D06-index_mi.ipynb` - Identify index MI events
   - `D07-lsoa_region_imd.ipynb` - Add geographic/socioeconomic data

4. **Feature Extraction**
   - `D09a-measurements.ipynb` - Extract measurements (BMI, height, weight)
   - `D09b-comorbidities.ipynb` - Identify comorbidities
   - `D09c-medications.ipynb` - Extract medication history
   - `D09d-covid_infection.ipynb` - Identify COVID-19 infections

5. **Outcomes and Assembly**
   - `D10-outcomes.ipynb` - Identify post-MI outcomes
   - `D11-combine_tables.ipynb` - Combine all tables into final cohort

### Using the Function Library

Import functions in your notebooks:

```python
from functions.table_management import load_table, save_table
from functions.cohort_construction import apply_inclusion_criteria
from functions.data_privacy import round_counts
from functions.csv_utils import load_codelist
```

### Working with Codelists

Codelists are stored as CSV files in the `codelists/` directory:

```python
from functions.csv_utils import load_codelist

# Load a codelist
diabetes_codes = load_codelist('codelists/diabetes_snomed.csv')

# Filter data using codelist
diabetes_patients = df.join(diabetes_codes, on='code', how='inner')
```

## Data Sources

The pipeline integrates with these NHS datasets:

- **GDPPR** - General Practice Extraction Service data
- **HES-APC** - Hospital Episode Statistics (Admitted Patient Care)
- **HES-AE** - Hospital Episode Statistics (Accident & Emergency)
- **HES-OP** - Hospital Episode Statistics (Outpatient)
- **SSNAP** - Sentinel Stroke National Audit Programme
- **CHESS** - COVID-19 Hospitalisation in England Surveillance System
- **SGSS** - Second Generation Surveillance System (COVID-19 tests)
- **Deaths** - ONS mortality data
- **Primary Care Meds** - Medication prescribing records
- **Vaccine Status** - COVID-19 vaccination records

## Key Dependencies

- **PySpark** - Distributed data processing
- **Apache Spark SQL** - SQL operations on DataFrames
- Standard Python libraries: `os`, `sys`, `json`, `functools`

## Privacy and Disclosure Control

This pipeline includes privacy-preserving features:

- Count rounding functions in `functions/data_privacy.py`
- Suppression of small counts
- Controlled aggregation methods

Always ensure compliance with NHS Digital data governance and disclosure control policies.

## Data Quality Monitoring

The pipeline includes comprehensive data quality checks:

- Archive version monitoring
- Missing data analysis
- Count validation across sources
- Temporal coverage assessment

Run `D01-data_checks.ipynb` to generate data quality reports.

## Customization

To adapt this pipeline for other cohorts:

1. Update codelists in `codelists/` directory
2. Modify inclusion/exclusion criteria in cohort notebooks
3. Add or remove feature extraction notebooks (D09 series)
4. Update study parameters in `parameters.ipynb`
5. Customize outcomes in `D10-outcomes.ipynb`

## Contributing

When contributing to this pipeline:

1. Follow the existing naming conventions (D##-descriptive_name.ipynb)
2. Add reusable functions to appropriate modules in `functions/`
3. Document code with clear comments
4. Test notebooks end-to-end before committing
5. Update this README with any structural changes

## License

This project is intended for use within NHS Digital's Trusted Research Environment. Ensure compliance with data access agreements and information governance policies.

## How to cite

Please cite all uses of this pipeline, e.g.
“Data curation was conducted using the BHF Data Science Centre standard pipeline (British Heart Foundation Data Science Centre at Health Data Research UK), available at https://github.com/BHFDSC/standard-pipeline, accessed 29 July 2025.”
For additional information see our Publication and Dissemination Policy (https://bhfdatasciencecentre.org/publication-and-dissemination-policy/)

## Support

For questions or issues:

1. Check existing notebook documentation
2. Review function docstrings in `functions/` modules
4. Consult Health data science team
5. Refer to NHS Digital data documentation

## Acknowledgments

This pipeline follows Health Data Science best practices and builds upon:

- NHS Digital curated data assets
- BHF Data Science Centre methodologies
- Databricks health analytics frameworks

---

**Note**: This is a template pipeline. Always adapt parameters, codelists, and criteria to match your specific research question and study protocol.
