# MAGIC %md
# MAGIC ## Create table directory
# MAGIC
# MAGIC In your curation work you'll often refer to locations of datasets needed, and locations in which your outputs should be saved.
# MAGIC
# MAGIC This notebook creates a handy address book so that your code can simply use the shorthands. It reduces the chances of error, and avoids a lot of duplication and clutter.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./parameters

# COMMAND ----------

import json
import os
from functions.json_utils import write_json_file

# Database names
# These database names are used in the notebook to access the tables, in order to retrieve the input data, and save output tables. Given each notebook might refer to these multiple times we parameterise the name into a shorthand. Be advised the consortium operates under the same agreement number, but other agreements would likely have their own ref.

db = 'dars_nic_391419_j3w9t'        # database where raw data if often found
dbc = f'{db}_collab'                # older database where raw and curated data can be found, but no longer written to
dsa = 'dsa_391419_j3w9t_collab'     # database where curated assets are stored, and you will save data to
dss = 'dss_corporate'               # often used to store reference lists

# Project name
# Output tables from your curation pipeline should start with your project name i.e. your CCU number. We as standard name the project folder by the project name anyway, so the below code retrieves it from there.  

project_name = os.environ['PROJECT_NAME']

# Table directory

# As new raw data comes in intermittently, it means the tables in the database exist in different batches. One batch will contain the newest data at the time was created and all the previous iterations. It is therefore important to specify which batch to used based on the archived_on date. New batches will typically result in datasets being updated, but some datasets experience delays and fall out of sync. You can manually change this in the table directory below should this occur.
archive_date = '2024-10-24'

archive_date_underscore = archive_date.replace("-", "_")

table_directory = {
    # --------------------------------------------------------------------------
    # Provisioned datasets
    # --------------------------------------------------------------------------
    # For the purposes of the curation work, these sets are the raw data you might start with - provisioned by NHSE. The below is to have an easy catalogue that your curation notebooks can call upon, a bit like an address book. You might not need all of them so can comment out the ones not of use.
    
    'gdppr': {
        'database': dbc,
        'table_name': 'gdppr_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'gdppr_all_versions': {
        'database': dbc,
        'table_name': 'gdppr_dars_nic_391419_j3w9t_archive',
    },
    'hes_apc': {
        'database': dbc,
        'table_name': 'hes_apc_all_years_archive',
        'archive_date': archive_date
    },
    'hes_op': {
        'database': dbc,
        'table_name': 'hes_op_all_years_archive',
        'archive_date': archive_date
    },
    'hes_ae': {
        'database': dbc,
        'table_name': 'hes_ae_all_years_archive',
        'archive_date': archive_date
    },
    'ssnap': {
        'database': dbc,
        'table_name': 'ssnap_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'vaccine_status':{
        'database': dbc,
        'table_name': 'vaccine_status_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'sgss':{
        'database': dbc,
        'table_name': 'sgss_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'pillar_2':{
        'database': dbc,
        'table_name': 'covid_antigen_testing_pillar2_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'sus':{
        'database': dbc,
        'table_name': 'sus_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'chess':{
        'database': dbc,
        'table_name': 'chess_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'deaths': {
        'database': dbc,
        'table_name': 'deaths_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'primary_care_meds':{
        'database': dbc,
        'table_name': 'primary_care_meds_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'token_pseudo_id_lookup':{
        'database': dbc,
        'table_name': 'token_pseudo_id_lookup_archive',
        'archive_date': archive_date
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - Demographics
    # --------------------------------------------------------------------------
    # These tables are pre-curated or semi-curated by the BHFDSC Health Data Science team. In our experience most projects will benefit from some of them - hence the 'common' - but it's unlikely an project will require all of them so again can simply comment out the ones not of use.

    'gdppr_demographics_all_versions': {
        'database': dsa,
        'table_name': f'hds_curated_assets__gdppr_demographics',
    },
    'gdppr_demographics': {
        'database': dsa,
        'table_name': f'hds_curated_assets__gdppr_demographics',
        'max_archive_date': archive_date
    },
    'date_of_birth_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__date_of_birth_multisource_{archive_date_underscore}',
    },
    'date_of_birth_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__date_of_birth_individual_{archive_date_underscore}',
    },
    'sex_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__sex_multisource_{archive_date_underscore}',
    },
    'sex_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__sex_individual_{archive_date_underscore}',
    },
    'ethnicity_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__ethnicity_multisource_{archive_date_underscore}',
    },
    'ethnicity_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__ethnicity_individual_{archive_date_underscore}',
    },
    'lsoa_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__lsoa_multisource_{archive_date_underscore}',
    },
    'lsoa_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__lsoa_individual_{archive_date_underscore}',
    },
    'demographics': {
        'database': dsa,
        'table_name': f'hds_curated_assets__demographics_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - HES-APC 
    # --------------------------------------------------------------------------
    'hes_apc_cips_episodes': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_cips_episodes_{archive_date_underscore}',
    },
    'hes_apc_provider_spells': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_provider_spells_{archive_date_underscore}',
    },
    'hes_apc_cip_spells': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_cip_spells_{archive_date_underscore}',
    },
    'hes_apc_diagnosis': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_diagnosis_{archive_date_underscore}',
    },
    'hes_apc_procedure': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_procedure_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - ONS Deaths
    # --------------------------------------------------------------------------
    'deaths_single': {
        'database': dsa,
        'table_name': f'hds_curated_assets__deaths_single_{archive_date_underscore}',
    },
    'deaths_cause_of_death': {
        'database': dsa,
        'table_name': f'hds_curated_assets__deaths_cause_of_death_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - Covid-19
    # --------------------------------------------------------------------------
    'covid_positive': {
        'database': dsa,
        'table_name': f'hds_curated_assets__covid_positive_{archive_date_underscore}',
    },
    
    # --------------------------------------------------------------------------
    # Project tables
    # --------------------------------------------------------------------------
    # The above tables are datasets that will only but inputted into the pipeline - you will not output to them. The below are the tables names and locations that are outputted by your pipeline. For this demonstration pipeline, the following catalogue makes sense, however for repurposing this pipeline to other projects, you will likely want to change the names. We strongly recommend keeping the naming conventions though.

    # D04-person_id_and_demographics

    'cohort_demographics': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_demographics',
    },

    # D05-prior_mi

    'prior_mi_events': {
        'database': dsa,
        'table_name': f'{project_name}__prior_mi_events'
    },

    'prior_mi_earliest': {
        'database': dsa,
        'table_name': f'{project_name}__prior_mi_earliest'
    },

    'cohort_prior_mi': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_prior_mi'
    },

    # D06-index_mi

    'index_mi_events': {
        'database': dsa,
        'table_name': f'{project_name}__index_mi_events'
    },

    'index_mi_earliest': {
        'database': dsa,
        'table_name': f'{project_name}__index_mi_earliest'
    },

    'cohort_index_mi': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_index_mi'
    },

    # D07-lsoa_imd_region

    'cohort_lsoa_imd_region': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_lsoa_imd_region'
    },

    # D08-cohort_inclusion

    'cohort_filtered': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_filtered'
    },

    'inclusion_flowchart': {
        'database': dsa,
        'table_name': f'{project_name}__inclusion_flowchart'
    },

    # D09a-measurements

    'cohort_measures': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_measures'
    },

    # D09b-comorbidities

    'comorbs_all_events': {
        'database': dsa,
        'table_name': f'{project_name}__comorbs_all_events'
    },  

    'comorbs_last_event': {
        'database': dsa,
        'table_name': f'{project_name}__comorbs_last_event'
    },

    'cohort_comorbs': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_comorbs'
    },

    # D09c-medications

    'medications_all_events': {
        'database': dsa,
        'table_name': f'{project_name}__medications_all_events'
    },

    'medications_last_event': {
        'database': dsa,
        'table_name': f'{project_name}__medications_last_event'
    },

    'cohort_medications': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_medications'
    },

    # D09d-covid_infection

    'covid_positive_all_events': {
        'database': dsa,
        'table_name': f'{project_name}__covid_positive_all_events'
    },

    'covid_positive_last_event': {
        'database': dsa,
        'table_name': f'{project_name}__covid_positive_last_event'
    },

    'cohort_covid_infection': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_covid_infection'
    },

    # D10-outcomes

    'outcomes_all_events': {
        'database': dsa,
        'table_name': f'{project_name}__outcomes_all_events'
    },

    'outcomes_first_event': {
        'database': dsa,
        'table_name': f'{project_name}__outcomes_first_event'
    },

    'cohort_outcomes': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_outcomes'
    },

    # D11-combine_tables

    'cohort_final': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_final'
    }

}

# Write table_directory
write_json_file(table_directory, path = './config/table_directory.json')
