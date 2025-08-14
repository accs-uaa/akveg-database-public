# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the NPS CAKN 2021 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the NPS CAKN 2021 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the NPS CAKN 2021 dataset.
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
project_folder = '26_nps_cakn_2021'

# Define input data files
project_input = '01_project_npscakn2021.xlsx'
site_input = '02_site_npscakn2021.xlsx'
site_visit_input = '03_sitevisit_npscakn2021.xlsx'
cover_input = '05_vegetationcover_npscakn2021.xlsx'
plants_input = 'speciesPlants_03062018.xlsx'

# Define output data files
site_output = '02_site_npscakn2021'
site_visit_output = '03_sitevisit_npscakn2021'
cover_output = '05_vegetationcover_npscakn2021'
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
site_original = read_excel(paste(data_folder, project_folder, 'excel', site_input, sep = '/')
                           , sheet = 'site')
site_visit_original = read_excel(paste(data_folder, project_folder, 'excel', site_visit_input, sep = '/')
                                 , sheet = 'site_visit')
cover_original = read_excel(paste(data_folder, project_folder, 'excel', cover_input, sep = '/')
                                 , sheet = 'cover')
plants_data = read_excel(paste(drive, root_folder,
                               'Projects/VegetationEcology/AKVEG_Database',
                               'Data/Tables_Taxonomy/USDA_Plants',
                               plants_input, sep = '/'),
                         sheet = 'speciesPlants')

# Parse new site table
site_data = site_original %>%
  # Join site visit data
  inner_join(site_visit_original, by = 'site_code') %>%
  # Rename columns
  rename(establishing_project = project_code) %>%
  # Add missing data
  mutate(perspective = 'ground') %>%
  # Correct erroneous data
  mutate(establishing_project = 'nps_cakn_2021') %>%
  mutate(plot_dimensions_m = '8 radius') %>%
  # Select final fields
  distinct(site_code, establishing_project, perspective, cover_method, h_datum,
           latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Correct erroneous data
  mutate(veg_observer = 'Unknown') %>%
  mutate(veg_recorder = 'Unknown') %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  mutate(project_code = 'nps_cakn_2021') %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = 'TRUE') %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  # Replace subspecies label
  mutate_if(is.character,
            str_replace_all, pattern = 'subsp.', replacement = 'ssp.') %>%
  # Adjust name original
  mutate(name_original = case_when(name_original == 'Lichen' ~ 'lichen',
                                   name_original == 'Moss' ~ 'moss',
                                   name_original == 'PEOC6' ~ 'Peltigera occidentalis',
                                   name_original == 'PERE11' ~ 'Peltigera retifoveata',
                                   name_original == 'CLUM4' ~ 'Cladonia umbricola',
                                   name_original == 'CLARB' ~ 'Cladina arbuscula ssp. beringiana',
                                   name_original == 'CLPR2' ~ 'Cladonia prolifica',
                                   name_original == 'CLAS5' ~ 'Cladonia asahinae',
                                   name_original == 'CLDI11' ~ 'Cladonia dimorpha',
                                   name_original == 'CLHO3' ~ 'Cladonia homosekikaica',
                                   name_original == 'CLHU' ~ 'Cladonia humilis',
                                   name_original == 'CLNO3' ~ 'Cladonia novochlorophaea',
                                   name_original == 'CLRE60' ~ 'Cladonia rei',
                                   name_original == 'CLVE4' ~ 'Cladonia verruculosa',
                                   name_original == 'LEIM5' ~ 'Leptogium imbricatum',
                                   name_original == 'Marchantia polymorpha L.' ~ 'Marchantia polymorpha',
                                   name_original == 'MESU61' ~ 'Melanelia subolivacea',
                                   name_original == 'PASK' ~ 'Parmelia skultii',
                                   name_original == 'PATR10' ~ 'Parmeliella triptophylla',
                                   name_original == 'Phegopteris connectilis (Michx.) Watt' ~ 'Phegopteris connectilis',
                                   name_original == 'Rubus pedatus Sm.' ~ 'Rubus pedatus',
                                   name_original == 'STSA9' ~ 'Stereocaulon sasakii',
                                   name_original == 'STST10' ~ 'Stereocaulon sterile',
                                   name_original == 'Trisetum sibiricum Rupr.' ~ 'Trisetum sibiricum',
                                   name_original == 'Unidentified liverwort' ~ 'liverwort',
                                   name_original == 'Vaccinium caespitosum Michx.' ~ 'Vaccinium caespitosum',
                                   name_original == 'Veratrum viride Ait.' ~ 'Veratrum viride', 
                                   TRUE ~ name_original)) %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Join USDA Plants names
  left_join(plants_data, by = c('name_original' = 'codePLANTS'), keep = TRUE) %>%
  mutate(name_original = case_when(is.na(name_adjudicated) & !is.na(namePLANTS) ~ namePLANTS,
                                   TRUE ~ name_original)) %>%
  # Select interim fields
  select(site_visit_id, name_original, cover_type, dead_status, cover_percent) %>%
  # Repeat join for name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Adjust name adjudicated
  mutate(name_adjudicated = case_when(name_original == 'Astragalus adsurgens' ~ 'Astragalus adsurgens var. tananaicus',
                                      name_original == 'Dryas octopetala' ~ 'Dryas ajanensis ssp. beringensis',
                                      name_original == 'Polygonum bistorta' ~ 'Bistorta plumosa',
                                      name_original == 'Deschampsia caespitosa' ~ 'Deschampsia cespitosa',
                                      name_original == 'Lagotis glauca ssp. minor' ~ 'Lagotis glauca',
                                      name_original == 'Penstemon gormani' ~ 'Penstemon gormanii',
                                      name_original == 'Potentilla uniflora' ~ 'Potentilla vulcanicola',
                                      name_original == 'Rumex acetosa ssp. alpestris' ~ 'Rumex lapponicus',
                                      name_original == 'Saxifraga bronchialis' ~ 'Saxifraga funstonii',
                                      name_original == 'Saxifraga hieracifolia' ~ 'Saxifraga hieraciifolia',
                                      TRUE ~ name_adjudicated)) %>%
  filter(name_original != 'Burned moss' &
           name_original != 'burned into dead moss' &
           name_original != 'Burned graminoid' &
           name_original != 'FLAMIN') %>%
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