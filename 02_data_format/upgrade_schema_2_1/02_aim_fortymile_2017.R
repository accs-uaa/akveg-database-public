# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# AIM Fortymile environment and soils schema 2.1 update
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2023-03-26
# Usage: Script should be executed in R 4.0.0+.
# Description: "AIM Fortymile environment and soils schema 2.1 update" updates the AIM Fortymile environment and soils data from schema version 2.0 to 2.1 and includes missing data.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')

# Define project
aim_2017_folder = '07_aim_fortymile_2017'

# Define input files
site_visit_2017_file = paste(data_folder,
                             aim_2017_folder,
                             '03_sitevisit_aimfortymile2017.csv',
                             sep = '/')
environment_file = paste(data_folder,
                         aim_2017_folder,
                         'schema_2_1',
                         '12_environment_aimfortymile2017.xlsx',
                         sep = '/')
soil_metrics_file = paste(data_folder,
                          aim_2017_folder,
                          'schema_2_1',
                          '13_soilmetrics_aimfortymile2017.xlsx',
                          sep = '/')
soil_horizons_file = paste(data_folder,
                           aim_2017_folder,
                           'schema_2_1',
                           '14_soilhorizons_aimfortymile2017.xlsx',
                           sep = '/')

# Define output data files
environment_2017_output = paste(data_folder,
                                aim_2017_folder,
                                '12_environment_aimfortymile2017.csv',
                                sep = '/')
metrics_2017_output = paste(data_folder,
                            aim_2017_folder,
                            '13_soilmetrics_aimfortymile2017.csv',
                            sep = '/')
horizons_2017_output = paste(data_folder,
                             aim_2017_folder,
                             '14_soilhorizons_aimfortymile2017.csv',
                             sep = '/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input site visit data into data frames
site_visit_data = read.csv(site_visit_2017_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code, project_code)

# Read environment, soil metrics, and soil horizons
environment_data = read_excel(environment_file, sheet = 'environment')
soil_metrics_data = read_excel(soil_metrics_file, sheet = 'soil_metrics') %>%
  left_join(site_visit_data, by = 'site_code') %>%
  select(-site_code, -project_code)
soil_horizons_data = read_excel(soil_horizons_file, sheet = 'horizons') %>%
  left_join(site_visit_data, by = 'site_code') %>%
  select(-site_code, -project_code)

# Create export lists
output_list = list(environment_2017_output,
                   metrics_2017_output,
                   horizons_2017_output)
table_list = list(environment_data,
                  soil_metrics_data,
                  soil_horizons_data)

# Export output tables to csv
for (output in output_list) {
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = output, fileEncoding = 'UTF-8', row.names = FALSE)
}