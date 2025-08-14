# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ABR Various 2022 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ABR Various 2022 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the ABR Various 2022 dataset. Unique rows are enforced.
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
project_folder = '28_abr_various_2022'

# Define input data files
project_input = '01_project_abrvarious2022.xlsx'
site_input = '02_site_abrvarious2022.xlsx'
site_visit_input = '03_sitevisit_abrvarious2022.xlsx'
cover_input = '05_vegetationcover_abrvarious2022.xlsx'

# Define output data files
site_output = '02_site_abrvarious2022'
site_visit_output = '03_sitevisit_abrvarious2022'
cover_output = '05_vegetationcover_abrvarious2022'
output_list = c(site_output, site_visit_output, cover_output)

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tibble)
library(tidyr)

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connectDatabasePostGreSQL.R',
                          sep = '/')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = paste(drive,
                       root_folder,
                       'Administrative/Credentials/accs-postgresql/authentication_akveg.csv',
                       sep = '/')
database_connection = connect_database_postgresql(authentication)

# Read constraints into data frames from AKVEG
query_taxon = 'SELECT * FROM taxon_all'
taxon_data = as_tibble(dbGetQuery(database_connection, query_taxon))

# Read input files into data frames
project_original = read_excel(paste(data_folder, project_folder, 'excel', project_input, sep = '/'),
                              sheet = 'project')
site_original = read_excel(paste(data_folder, project_folder, 'excel', site_input, sep = '/'),
                           sheet = 'site')
site_visit_original = read_excel(paste(data_folder, project_folder, 'excel', site_visit_input, sep = '/'),
                                 sheet = 'site_visit') %>%
  # Remove arctic refuge data
  filter(project_code != 'usfws_anwr_2019')
cover_original = read_excel(paste(data_folder, project_folder, 'excel', cover_input, sep = '/'),
                            sheet = 'cover')

# Create project code table
project_codes = site_visit_original %>%
  distinct(project_code) %>%
  mutate(code_new = case_when(project_code == 'cpai_meltwater_2000' ~ 'abr_meltwater_2000',
                              project_code == 'cpai_drill_site_3s_2001' ~ 'abr_drillsite3s_2001',
                              project_code == 'cpai_npra_els_2001_2002' ~ 'abr_npra_2003',
                              project_code == 'cpai_npra_els_2003' ~ 'abr_npra_2003',
                              project_code == 'cpai_kuparuk_els_2006' ~ 'abr_kuparuk_2006',
                              project_code == 'hilcorp_milne_point_els_2008' ~ 'abr_milnepoint_2008',
                              project_code == 'cpai_nuna_els_2010' ~ 'abr_nuna_2010',
                              project_code == 'cpai_news_els_2011' ~ 'abr_news_2011',
                              project_code == 'aea_susitna_2012' ~ 'abr_susitna_2013',
                              project_code == 'aea_susitna_2013' ~ 'abr_susitna_2013',
                              project_code == 'cpai_cd5_habitat_2016' ~ 'abr_cd5_2016',
                              project_code == 'cpai_willow_itu_2017' ~ 'abr_willow_2018',
                              project_code == 'cpai_willow_itu_2018' ~ 'abr_willow_2018',
                              project_code == 'cpai_stony_hill_itu_2018' ~ 'abr_stonyhill_2018',
                              project_code == 'cpai_colville_els_1996' ~ 'abr_colville_1996',
                              project_code == 'cpai_colville_seismic_1997_1998' ~ 'abr_colville_1998',
                              project_code == 'cpai_tarn_1997' ~ 'abr_tarn_1997',
                              TRUE ~ project_code))

# Parse new site table
site_data = site_original %>%
  # Join site visit data
  inner_join(site_visit_original, by = 'site_visit_id_v2') %>%
  # Add missing data
  mutate(perspective = 'ground') %>%
  # Join corrected project codes
  left_join(project_codes, by = 'project_code') %>%
  rename(establishing_project = code_new) %>%
  # Correct erroneous data
  mutate(plot_dimensions_m = case_when(plot_dimensions_m == '1' ~ '1 radius',
                                       plot_dimensions_m == '5' ~ '5 radius',
                                       plot_dimensions_m == '10' ~ '10 radius',
                                       plot_dimensions_m == '15' ~ '15 radius',
                                       plot_dimensions_m == '23' ~ '23 radius',
                                       plot_dimensions_m == 'unknown' ~ '10 radius',
                                       TRUE ~ plot_dimensions_m)) %>%
  mutate(location_type = 'targeted') %>%
  mutate(cover_method = 'semi-quantitative visual estimate') %>%
  # Replace site codes
  rename(site_code = site_code_v2.x) %>%
  # Select final fields
  distinct(site_code, establishing_project, perspective, cover_method, h_datum,
           latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type) %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

check_site = site_data %>%
  distinct(site_code)

# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Join corrected project codes
  left_join(project_codes, by = 'project_code') %>%
  # Correct erroneous data
  mutate(data_tier = 'ecological land classification') %>%
  mutate(homogenous = 'TRUE') %>%
  # Replace NA values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  mutate(env_observer = replace_na(env_observer, 'NULL')) %>%
  mutate(soils_observer = replace_na(soils_observer, 'NULL')) %>%
  # Rename project code
  select(-project_code) %>%
  rename(project_code = code_new) %>%
  # Replace site code and site visit id
  select(-site_code, -site_visit_id) %>%
  rename(site_code = site_code_v2.x) %>%
  rename(site_visit_id = site_visit_id_v2.x) %>%
  # Select final fields
  distinct(site_visit_id, project_code, site_code, data_tier, observe_date,
           veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
           scope_vascular, scope_bryophyte, scope_lichen, homogenous) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  # Replace site visit id
  left_join(site_visit_original, by = c('site_visit_id' = 'site_visit_id_v1')) %>%
  select(-site_visit_id) %>%
  rename(site_visit_id = site_visit_id_v2) %>%
  # Limit observations to site visits
  inner_join(site_visit_data, by = 'site_visit_id') %>%
  # Correct original name
  mutate(name_original = case_when(name_original == 'Luzula 1' ~ 'Luzula',
                                   name_original == 'Pedicularis 1' ~ 'Pedicularis',
                                   name_original == 'Sphagnum (green)' ~ 'Sphagnum',
                                   name_original == 'Sphagnum 1' ~ 'Sphagnum',
                                   name_original == 'Sphagnum 2' ~ 'Sphagnum',
                                   name_original == 'Carex 1' ~ 'Carex',
                                   name_original == 'Sphagnum (brown)' ~ 'Sphagnum',
                                   name_original == 'Sphagnum (red)' ~ 'Sphagnum',
                                   name_original == 'Festuca 1' ~ 'Festuca',
                                   name_original == 'Salix 1' ~ 'Salix',
                                   TRUE ~ name_original)) %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Correct adjudicated names
  mutate(name_adjudicated = case_when(name_original == 'Alopecurus magellanicus' ~ 'Alopecurus borealis',
                                      name_original == 'Hedysarum mackenzii' ~ 'Hedysarum mackenziei',
                                      name_original == 'Pedicularis sudetica' ~ 'Pedicularis',
                                      name_original == 'Androsace chamaejasme ssp. lehmannia' ~
                                        'Androsace chamaejasme ssp. andersonii',
                                      name_original == 'Minuartia' ~ 'forb',
                                      name_original == 'Melandrium apetalum' ~
                                        'Silene uralensis ssp. arctica',
                                      name_original == 'Lagotis glauca ssp. minor' ~ 'Lagotis glauca',
                                      name_original == 'Myriophyllum spicatum' ~ 'Myriophyllum sibiricum',
                                      name_original == 'Potentilla uniflora' ~ 'Potentilla vulcanicola',
                                      name_original == 'Saxifraga bronchialis' ~ 'Saxifraga funstonii',
                                      name_original == 'Ranunculus gmelinii ssp. gmelini' ~
                                        'Ranunculus gmelinii ssp. gmelinii',
                                      name_original == 'Festuca vivipara' ~ 'Festuca viviparoidea',
                                      name_original == 'Astragalus eucosmus ssp. eucosmus' ~
                                        'Astragalus eucosmus',
                                      name_original == 'Luzula wahlenbergii ssp. wahlenbergii' ~
                                        'Luzula wahlenbergii',
                                      name_original == 'Chrysanthemum' ~ 'Arctanthemum',
                                      name_original == 'Carex amblyorhyncha' ~ 'Carex amblyorrhyncha',
                                      name_original == 'Gentiana propinqua ssp. propinqua' ~
                                        'Gentianella propinqua ssp. propinqua',
                                      name_original == 'Populus balsamifera x trichocarpa' ~
                                        'Populus balsamifera',
                                      name_original == 'Pyrola secunda ssp. secunda' ~
                                        'Pyrola secunda',
                                      name_original == 'Brachythecium velutinum' ~
                                        'Brachythecium',
                                      name_original == 'Brachythecium rutabulum' ~
                                        'Brachythecium',
                                      name_original == 'Corallorrhiza trifida' ~ 'Corallorhiza trifida',
                                      name_original == 'Solidago multiradiata var. multiradiata' ~
                                        'Solidago multiradiata',
                                      name_original == 'Arnica alpina' ~ 'Arnica',
                                      name_original == 'Nostoc pruniforme' ~ 'algae',
                                      name_original == 'Draba hirta' ~ 'Draba',
                                      name_original == 'Draba alpina' ~ 'Draba',
                                      name_original == 'Cerastium jenisejense' ~ 'Cerastium regelii',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Check if any problem taxa remain
problem_list = cover_data %>%
  filter(is.na(name_adjudicated)) %>%
  distinct(name_original, name_adjudicated)

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}