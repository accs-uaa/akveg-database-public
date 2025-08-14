# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for ABR Various 2019 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-09-24
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for ABR Various 2019 data" enters and formats project-level information for ingestion into the AKVEG Database. The script standardizes project names, extracts project start/end dates, adds project descriptions, and populates fields required for ingestion into the AKVEG database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
library(dplyr)
library(fs)
library(lubridate)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data')
plot_folder = path(project_folder, 'Data_Plots', '27_abr_various_2019')
source_folder = path(plot_folder, 'source')
working_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data_Entry")

# Define inputs ----
project_input = path(source_folder, 'deliverable_tnawrocki_ref_project.xlsx')
site_input = path(source_folder, 'deliverable_tnawrocki_els.xlsx')
template_input = path(template_folder,"01_Project.xlsx")

# Define outputs ----
project_output = path(plot_folder, '01_project_abrvarious2019.csv')
crosswalk_output = path(working_folder, 'xwalk_projectid_akvegcode.csv')

# Read in data ----
project_original = read_xlsx(path=project_input)
site_original = read_xlsx(path=site_input)
template = names(read_xlsx(path=template_input, col_types = "text"))

# Prepare Project data ----

# Ensure all projects in the project table are in the site table and vice-versa
which(!(unique(site_original$project_id) %in% project_original$project_id))
which(!(project_original$project_id %in% unique(site_original$project_id)))

# Generate project codes
# Combine both Shell ONES habitat mapping project
project_codes = project_original %>% 
  select(project_id, title) %>% 
  arrange(title) %>% 
  mutate(project_code = c('fws_selawik_2008',
                          'nps_arcn_2008',
                          'nps_aniakchak_2014',
                          'nps_alagnak_2014', 
                          'nps_katmai_2017',
                          'nps_kenai_2013',
                          'nps_lakeclark_2011',
                          'shell_ones_habitat_2012',
                          'shell_ones_habitat_2012',
                          'shell_ones_remote_2012',
                          'nps_wrangell_2006',
                          'nps_wrangell_yc_2012'),
         place_name = c('Selawik Wildlife Refuge',
                        'Arctic Network National Parks',
                        'Aniakchak National Monument and Preserve',
                        'Alagnak Wild River',
                        'Katmai National Park and Preserve',
                        'Kenai Fjords National Park and Preserve',
                        'Lake Clark National Park and Preserve',
                        'Shell',
                        'Shell',
                        'Shell',
                        'Wrangell-St. Elias National Park and Preserve',
                        'Wrangell-St. Elias National Park and Preserve and Yukon-Charley Rivers National Preserve')) %>% 
  select(-title)

# Populate table ----
project_data = site_original %>% 
  filter(!is.na(env_field_start_ts)) %>% 
  group_by(project_id) %>%  
  summarize(year_start = min(year(env_field_start_ts)), # Extract project start/end dates
            year_end = max(year(env_field_start_ts))) %>% 
  right_join(project_original, by='project_id') %>% 
  mutate(project_name = str_replace(title, 'ELS', 'Ecological Land Survey'),
         project_name = str_replace(project_name, 'NPP', 'National Park and Preserve'),
         project_name = str_replace(project_name, 'NP', 'National Preserve'),
         project_name = str_remove(project_name, '^An '),
         project_name = str_replace(project_name, 'mapping', 'Mapping'),
         project_name = str_replace(project_name, 'Map ', 'Mapping '),
         project_name = str_remove(project_name, ', Alaska, 2014')) %>% 
  mutate(project_name = case_when(grepl('Arctic Network', project_name) ~ 'Arctic Network National Parks Ecological Land Survey and Land Cover Mapping',
                                  grepl('Selawik', project_name) ~ 'Selawik National Wildlife Refuge Ecological Land Survey and Land Cover Mapping',
                                  grepl('Alagnak', project_name) ~ 'Alagnak Wild River Ecological Land Survey and Soil Landscapes Mapping',
                                  grepl('Aniakchak', project_name) ~ 'Aniakchak National Monument and Preserve Ecological Land Survey and Soil Landscapes Mapping',
                                  .default = project_name)) %>% 
  left_join(project_codes, by = 'project_id') %>% 
  mutate(originator = 'ABR') %>%
  mutate(funder = str_to_upper(client)) %>% 
  mutate(manager = case_when(grepl('Selawik|Arctic Network', project_name) ~ 'Torre Jorgenson',
  .default = 'Aaron Wells')) %>% 
  mutate(completion = 'finished') %>% 
  mutate(project_description = case_when(funder == 'SHELL' ~ 'Vegetation composition data collected for Shell as part of the ONES monitoring.',
                                         grepl('Permafrost Studies', title) ~ str_c('Vegetation composition data collected for permafrost studies of ', place_name, '.'),
                                         .default = str_c('Vegetation composition data collected for an ecological land survey of ', place_name, '.'))) %>% 
  mutate(private = "FALSE") %>% 
  select(project_id, all_of(template))

# Create crosswalk ----
# So that ABR project ids can be linked to AKVEG project codes
crosswalk = project_data %>% 
  select(project_id, project_code)

# Create final table ----
# Combine Shell habitat projects
project_final = project_data %>% 
  filter(project_id != '12-258.6.1') %>% 
  mutate(project_name = case_when(grepl('Shell ONES Habitat', project_name) ~ 'Shell ONES Habitat Mapping',
                                  .default = project_name),
         year_end = case_when(project_name == 'Shell ONES Habitat Mapping' ~ 2012,
                              .default = year_end)) %>% 
  select(all_of(template))

# Export CSV ----
write_csv(crosswalk, crosswalk_output)
write_csv(project_final, project_output)

# Clear workspace ----
rm(list=ls())
