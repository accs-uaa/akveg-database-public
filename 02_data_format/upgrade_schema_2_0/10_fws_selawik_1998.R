# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse data to new format
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-06
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse data to new format" parses all data tables per project from schema 1.0 to schema 2.0.
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
project_folder = '10_fws_selawik_1998'

# Define input data files
site_input = 'usfwsSelawikLC_1998_Site.xlsx'
cover_input = 'usfwsSelawikLC_1998_Cover.xlsx'

# Define output data files
site_output = '02_site_fwsselawik1998'
site_visit_output = '03_sitevisit_fwsselawik1998'
cover_output = '05_vegetationcover_fwsselawik1998'
output_list = c(site_output, site_visit_output, cover_output)

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input files into data frames
site_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', site_input, sep = '/')
                           , sheet = 'site')
cover_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', cover_input, sep = '/')
                            , sheet = 'cover')

# Create list of sites with vegetation cover data
site_list = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code)

# Parse new site table
site_data = site_original %>%
  # Select sites that have vegetation cover observations
  inner_join(site_list, by = 'site_code') %>%
  # Rename fields
  rename(establishing_project = initial_project,
         plot_dimensions_m = plot_dimensions,
         h_datum = datum,
         latitude_dd = latitude,
         longitude_dd = longitude,
         h_error_m = error) %>%
  # Assign no data values to empty fields
  mutate(positional_accuracy = 'image interpretation') %>%
  mutate(location_type = 'targeted') %>%
  # Correct project code
  mutate(establishing_project = 'fws_selawik_1998') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code, veg_observe_date, veg_observer, veg_recorder) %>%
  # Join site and environment tables
  left_join(site_original, by = 'site_code') %>%
  # Rename fields
  rename(observe_date = veg_observe_date) %>%
  # Assign no data value to empty fields
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = 'TRUE') %>%
  mutate(data_tier = 'map development & verification') %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  # Correct erroneous values
  mutate(project_code = 'fws_selawik_2005') %>%
  mutate(veg_recorder = 'NULL') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(observe_date, '-', ''),
                               sep = '_')) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen)

# Parse new cover table
cover_data = cover_original %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(veg_observe_date, '-', ''),
                               sep = '_')) %>%
  # Rename fields
  rename(cover_percent = cover) %>%
  # Assign no data value to empty fields
  mutate(dead_status = 'FALSE') %>%
  # Correct field values
  mutate(cover_type = 'absolute cover') %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Control for duplicate cover entries
final_cover = cover_data %>%
  group_by(site_visit_id, name_original) %>%
  summarize(cover_percent = sum(cover_percent))
cover_metadata = cover_data %>%
  distinct(site_visit_id, cover_type)
name_join = cover_data %>%
  distinct(name_original, name_adjudicated)
cover_join = final_cover %>%
  left_join(cover_metadata, by = 'site_visit_id') %>%
  left_join(name_join, by = 'name_original') %>%
  mutate(dead_status = 'FALSE') %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_join)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}