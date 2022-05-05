# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Prepare taxonomic codes
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-02-12
# Usage: Script should be executed in R 4.0.0+.
# Description: "Prepare taxonomic codes" merges taxonomy tables for vascular plants, bryophytes, and lichens and adds unique database codes and USDA Plants Database codes (where they exist).
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Tables_Taxonomy',
                    sep = '/')

# Identify taxonomy tables
vascular_file = paste(data_folder,
                      'vascular_plants.xlsx',
                      sep='/')
bryophyte_file = paste(data_folder,
                       'bryophytes.xlsx',
                       sep='/')
lichen_file = paste(data_folder,
                    'lichens.xlsx',
                    sep='/')

# Define output file
output_taxonomy = paste(data_folder,
                        'taxonomy.xlsx',
                        sep = '/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)
library(writexl)

# Read taxonomy tables into dataframes
vascular_data = read_excel(vascular_file, sheet = 'taxonomy')
bryophyte_data = read_excel(bryophyte_file, sheet = 'taxonomy')
lichen_data = read_excel(lichen_file, sheet = 'taxonomy')

# Merge taxonomy tables to single dataframe
taxonomy_data = bind_rows(vascular_data, bryophyte_data, lichen_data)

# Add unique codes
codes_added = taxonomy_data %>%
  mutate(name_split = name_adjudicated) %>%
  separate(name_split, c('genus', 'species', 'infralabel', 'infraspecies'), sep = ' ', extra = 'drop') %>%
  mutate(code = case_when(is.na(species) ~ str_sub(genus, 1, 6),
                          species == '×' ~ paste(str_sub(genus, 1, 3),
                                                 str_sub(species, 1, 1),
                                                 str_sub(infralabel, 1, 3),
                                                 sep = ''),
                          is.na(infralabel) ~ paste(str_sub(genus, 1, 3),
                                                    str_sub(species, 1, 3),
                                                    sep = ''),
                          infralabel == '×' ~ paste(str_sub(genus, 1, 3),
                                                    str_sub(species, 1, 1),
                                                    str_sub(infralabel, 1, 1),
                                                    str_sub(infraspecies, 1, 1),
                                                    sep = ''),
                          infralabel == 'f.' ~ paste(str_sub(genus, 1, 3),
                                                     str_sub(species, 1, 3),
                                                     str_sub(infralabel, 1, 1),
                                                     str_sub(infraspecies, 1, 3),
                                                     sep = ''),
                          infralabel == 'ssp.' ~ paste(str_sub(genus, 1, 3),
                                                       str_sub(species, 1, 3),
                                                       str_sub(infralabel, 1, 1),
                                                       str_sub(infraspecies, 1, 3),
                                                       sep = ''),
                          infralabel == 'var.' ~ paste(str_sub(genus, 1, 3),
                                                       str_sub(species, 1, 3),
                                                       str_sub(infralabel, 1, 1),
                                                       str_sub(infraspecies, 1, 3),
                                                       sep = ''),
                          TRUE ~ 'not_gen')) %>%
  mutate(code = tolower(code)) %>%
  mutate(status = case_when(status_adjudicated == 'accepted' |
                              status_adjudicated == 'adjacent Yukon' |
                              status_adjudicated == 'taxonomy unresolved' |
                              status_adjudicated == 'historic' |
                              status_adjudicated == 'location unresolved' ~ 1,
                            status_adjudicated == 'microspecies' |
                              status_adjudicated == 'name misapplied' |
                              status_adjudicated == 'possible hybrid origin' |
                              status_adjudicated == 'spelling variant' |
                              status_adjudicated == 'synonym' ~ 2,
                            TRUE ~ 0)) %>%
  mutate(org = case_when(category == 'Lichen' ~ 3,
                         category == 'Hornwort' |
                           category == 'Liverwort' |
                           category == 'Moss' ~ 2,
                         category == 'Eudicot' |
                           category == 'Fern' |
                           category == 'Forb' |
                           category == 'Gymnosperm' |
                           category == 'Horsetail' |
                           category == 'Lycophyte' |
                           category == 'Monocot' ~ 1,
                         TRUE ~ 0)) %>%
  arrange(code, status)

# Add an alternative code for duplicates
codes_added = codes_added %>%
  group_by(code) %>%
  mutate(duplicate_id = row_number()) %>%
  mutate(number = n()) %>%
  ungroup() %>%
  mutate(alt_code = paste(code, duplicate_id, sep = '')) %>%
  mutate(code = case_when(status == 2 &
                            number > 1 ~ alt_code,
                          status == 1 |
                            number == 1 ~ code,
                          TRUE ~ '1_error')) %>%
  select(-duplicate_id, -alt_code, -number)

# Add an alternative code for remaining (accepted) duplicates
codes_added = codes_added %>%
  group_by(code) %>%
  mutate(number = n()) %>%
  ungroup() %>%
  mutate(alt_code = case_when(level == 'genus' ~ str_sub(genus, 1, 8),
                              level == 'species' ~ paste(str_sub(genus, 1, 3),
                                                         str_sub(species, 1, 4),
                                                         sep = ''),
                              infralabel == 'f.' ~ paste(str_sub(genus, 1, 3),
                                                         str_sub(species, 1, 3),
                                                         str_sub(infralabel, 1, 1),
                                                         str_sub(infraspecies, 1, 3),
                                                         sep = ''),
                              infralabel == 'ssp.' ~ paste(str_sub(genus, 1, 3),
                                                           str_sub(species, 1, 3),
                                                           str_sub(infralabel, 1, 1),
                                                           str_sub(infraspecies, 1, 3),
                                                           sep = ''),
                              infralabel == 'var.' ~ paste(str_sub(genus, 1, 3),
                                                           str_sub(species, 1, 3),
                                                           str_sub(infralabel, 1, 1),
                                                           str_sub(infraspecies, 1, 3),
                                                           sep = ''),
                              TRUE ~ '1_error')) %>%
  mutate(code = case_when(status == 1 &
                            number > 1 ~ alt_code,
                          status == 2 |
                            number == 1 ~ code,
                          TRUE ~ '1_error')) %>%
  select(-alt_code, -number) %>%
  mutate(code = tolower(code))

# Remove unnecessary fields
codes_added = codes_added %>%
  select(-genus, -species, -infralabel, -infraspecies, -status)

# Export merged taxonomy table
write_xlsx(codes_added, path = output_taxonomy, col_names = TRUE, format_headers = FALSE)