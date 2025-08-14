# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse NPS Aniakchak 2009 upload from schema 1.0 to 2.0
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse NPS Aniakchak 2009 data from schema 1.0 to 2.0" updates the data from the 1.0 to 2.0 AKVEG Database schema format. New fields are added as necessary to match VTWG Minimum Standards per data tier and NODATA values are enforced. Duplicate vegetation cover entries are aggregated as the sum.
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
project_folder = '15_nps_aniakchak_2009'

# Define input data files
site_input = 'npsAniakchakLC_2009_Site.xlsx'
cover_input = 'npsAniakchakLC_2009_Cover.xlsx'
environment_input = 'npsAniakchakLC_2009_Environment.xlsx'

# Define output data files
site_output = '02_site_npsaniakchak2009'
site_visit_output = '03_sitevisit_npsaniakchak2009'
cover_output = '05_vegetationcover_npsaniakchak2009'
environment_output = '12_environment_npsaniakchak2009'
soils_output = '13_soils_npsaniakchak2009'
output_list = c(site_output, site_visit_output, cover_output, environment_output, soils_output)

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
environment_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', environment_input,
                                        sep = '/')
                                  , sheet = 'environment')

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
  mutate(positional_accuracy = 'consumer grade GPS') %>%
  mutate(location_type = 'targeted') %>%
  # Correct project code
  mutate(establishing_project = 'nps_aniakchak_2009') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code, veg_observe_date, veg_observer, veg_recorder) %>%
  # Join site and environment tables
  left_join(site_original, by = 'site_code') %>%
  left_join(environment_original, by = 'site_code') %>%
  # Rename fields
  rename(observe_date = veg_observe_date,
         soils_observer = soil_observer,
         project_code = project) %>%
  # Assign no data value to empty fields
  mutate(soils_observer = case_when(soil_ph_10 > 0 | soil_ph_30 > 0 | water_ph > 0 ~ soils_observer,
                                    TRUE ~ 'NULL')) %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = 'TRUE') %>%
  mutate(data_tier = 'legacy classification') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  mutate(env_observer = replace_na(env_observer, 'NULL')) %>%
  mutate(soils_observer = replace_na(soils_observer, 'NULL')) %>%
  # Correct erroneous data
  mutate(project_code = 'nps_aniakchak_2009') %>%
  mutate(scope_bryophyte = 'partial') %>%
  mutate(scope_lichen = 'partial') %>%
  mutate(veg_observer = case_when(veg_observer == 'Sue Ives' ~ 'Susan Ives',
                                  TRUE ~ veg_observer)) %>%
  mutate(soils_observer = case_when(soils_observer == 'Sue Ives' ~ 'Susan Ives',
                                    TRUE ~ soils_observer)) %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(observe_date, '-', ''),
                               sep = '_')) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

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
  # Correct name adjudicated
  mutate(name_adjudicated = case_when(name_adjudicated == 'Silene acaulis var. acaulis' ~ 'Silene acaulis',
                                      name_adjudicated == 'Forb' ~ 'forb',
                                      name_adjudicated == 'Astragalus alpinus var. alpinus' ~
                                        'Astragalus alpinus',
                                      name_adjudicated == 'Crustose Lichen' ~ 'crustose lichen',
                                      name_adjudicated == 'Liverwort' ~ 'liverwort',
                                      name_adjudicated == 'Moss' ~ 'moss',
                                      name_adjudicated == 'Algae' ~ 'algae',
                                      name_adjudicated == 'Fungus' ~ 'fungus',
                                      name_adjudicated == 'Graminoid' ~ 'graminoid',
                                      name_adjudicated == 'Astragalus alpinus ssp. alpinus' ~
                                        'Astragalus alpinus',
                                      name_adjudicated == 'Lichen' ~ 'lichen',
                                      name_adjudicated == 'Cryptobiotic Crust' ~ 'biotic soil crust',
                                      name_adjudicated == 'Foliose Lichen' ~ 'foliose lichen',
                                      name_adjudicated == 'Fruticose Lichen' ~ 'fruticose lichen',
                                      name_adjudicated == 'Eriophorum vaginatum var. vaginatum' ~
                                        'Eriophorum vaginatum',
                                      name_adjudicated == 'Parrya nudicaulis ssp. nudicaulis' ~
                                        'Parrya nudicaulis',
                                      name_adjudicated == 'Solidago multiradiata var. multiradiata' ~
                                        'Solidago multiradiata',
                                      name_adjudicated ==
                                        'Artemisia campestris ssp. borealis var. borealis' ~
                                        'Artemisia campestris ssp. borealis',
                                      name_adjudicated == 'Cyanobacteria' ~ 'cyanobacteria',
                                      name_adjudicated == 'Shrub' ~ 'shrub',
                                      name_adjudicated == 'Fern' ~ 'fern',
                                      name_adjudicated == 'Lycophyte' ~ 'lycophyte',
                                      name_adjudicated == 'Artemisia cana' ~ 'Artemisia',
                                      name_adjudicated == 'Eriophorum vaginatum ssp. vaginatum' ~
                                        'Eriophorum vaginatum',
                                      name_adjudicated == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                      TRUE ~ name_adjudicated)) %>%
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

# Parse new environment table
environment_data = environment_original %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(env_observe_date, '-', ''),
                               sep = '_')) %>%
  # Limit to site visits
  inner_join(site_visit_data, by = 'site_visit_id') %>%
  # Rename fields
  rename(microrelief_cm = microrelief,
         depth_water_cm = depth_water,
         depth_moss_duff_cm = depth_moss_duff,
         depth_restrictive_layer_cm = depth_restrictive_layer) %>%
  # Assign no data value to empty fields
  mutate(surface_water = 'NULL') %>%
  mutate(disturbance_severity = 'NULL') %>%
  mutate(disturbance_time_y = -999) %>%
  mutate(drainage = 'NULL') %>%
  mutate(microrelief_cm = -999) %>%
  mutate(depth_moss_duff_cm = -999) %>%
  # Replace na values
  mutate(physiography = replace_na(physiography, 'NULL')) %>%
  mutate(geomorphology = replace_na(geomorphology, 'NULL')) %>%
  mutate(macrotopography = replace_na(macrotopography, 'NULL')) %>%
  mutate(microtopography = replace_na(microtopography, 'NULL')) %>%
  mutate(moisture = replace_na(moisture, 'NULL')) %>%
  mutate(drainage = replace_na(drainage, 'NULL')) %>%
  mutate(disturbance = replace_na(disturbance, 'NULL')) %>%
  mutate(depth_water_cm = replace_na(depth_water_cm, -999)) %>%
  mutate(depth_moss_duff_cm = replace_na(depth_moss_duff_cm, -999)) %>%
  mutate(depth_restrictive_layer_cm = replace_na(depth_restrictive_layer_cm, -999)) %>%
  mutate(restrictive_type = replace_na(restrictive_type, 'NULL')) %>%
  mutate(microrelief_cm = replace_na(microrelief_cm, -999)) %>%
  # Select final fields
  select(site_visit_id, physiography, geomorphology, macrotopography, microtopography,
         moisture, drainage, disturbance, disturbance_severity, disturbance_time_y,
         depth_water_cm, depth_moss_duff_cm, depth_restrictive_layer_cm, restrictive_type,
         microrelief_cm, surface_water)

# Parse new soils table
soils_data = environment_original %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(env_observe_date, '-', ''),
                               sep = '_')) %>%
  # Limit to site visits
  inner_join(site_visit_data, by = 'site_visit_id') %>%
  # Assign no data value to empty fields
  mutate(soil_measure_depth_cm = -999) %>%
  mutate(conductivity_10 = -999) %>%
  mutate(conductivity_30 = -999) %>%
  mutate(water_conductivity = -999) %>%
  mutate(temperature_10 = -999) %>%
  mutate(temperature_30 = -999) %>%
  mutate(water_temperature = -999) %>%
  mutate(cryoturbation = 'NULL') %>%
  mutate(dominant_texture_40_cm = 'NULL') %>%
  mutate(depth_15percent_rock_cm = -999) %>%
  mutate(soil_class = 'NULL') %>%
  # Replace na values
  mutate(soil_ph_10 = replace_na(soil_ph_10, -999)) %>%
  mutate(soil_ph_30 = replace_na(soil_ph_30, -999)) %>%
  mutate(conductivity_10 = replace_na(conductivity_10, -999)) %>%
  mutate(conductivity_30 = replace_na(conductivity_30, -999)) %>%
  mutate(water_conductivity = replace_na(water_conductivity, -999)) %>%
  mutate(temperature_10 = replace_na(temperature_10, -999)) %>%
  mutate(temperature_30 = replace_na(temperature_30, -999)) %>%
  mutate(water_temperature = replace_na(water_temperature, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL')) %>%
  # Parse ph, conductivity, and temperature
  mutate(soil_measure_depth_cm = case_when(soil_ph_10 != -999 ~ 10,
                                           soil_ph_10 == -999 & soil_ph_30 != -999 ~ 30,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 &
                                             water_ph != -999 ~ 0,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 != -999 ~ 10,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 == -999 & conductivity_30 != -999 ~ 30,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 == -999 & conductivity_30 == -999 &
                                             water_conductivity != -999 ~ 0,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 == -999 & conductivity_30 == -999 &
                                             water_conductivity == -999 & temperature_10 != -999 ~ 10,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 == -999 & conductivity_30 == -999 &
                                             water_conductivity == -999 & temperature_10 == -999 &
                                             temperature_30 != -999 ~ 30,
                                           soil_ph_10 == -999 & soil_ph_30 == -999 & water_ph == -999 &
                                             conductivity_10 == -999 & conductivity_30 == -999 &
                                             water_conductivity == -999 & temperature_10 == -999 &
                                             temperature_30 == -999 & water_temperature != -999 ~ 0,
                                           TRUE ~ -999)) %>%
  mutate(soil_ph = case_when(soil_ph_10 != -999 ~ soil_ph_10,
                             soil_ph_10 == -999 & soil_ph_30 != -999 ~ soil_ph_30,
                             soil_ph_10 == -999 & soil_ph_30 == -999 &
                               water_ph != -999 ~ water_ph,
                             TRUE ~ -999)) %>%
  mutate(soil_conductivity_mus = case_when(conductivity_10 != -999 ~ conductivity_10,
                             conductivity_10 == -999 & conductivity_30 != -999 ~ conductivity_30,
                             conductivity_10 == -999 & conductivity_30 == -999 &
                               water_conductivity != -999 ~ water_conductivity,
                             TRUE ~ -999)) %>%
  mutate(soil_temperature_deg_c = case_when(temperature_10 != -999 ~ temperature_10,
                             temperature_10 == -999 & temperature_30 != -999 ~ temperature_30,
                             temperature_10 == -999 & temperature_30 == -999 &
                               water_temperature != -999 ~ water_temperature,
                             TRUE ~ -999)) %>%
  # Remove rows with no data
  filter(soil_measure_depth_cm != -999) %>%
  # Select final fields
  select(site_visit_id, soil_measure_depth_cm, soil_ph,
         soil_conductivity_mus, soil_temperature_deg_c,
         cryoturbation, dominant_texture_40_cm, depth_15percent_rock_cm, soil_class)

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_join, environment_data, soils_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}