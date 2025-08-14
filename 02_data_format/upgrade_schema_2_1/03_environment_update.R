# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Environment and soils schema 2.1 update
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2023-03-26
# Usage: Script should be executed in R 4.0.0+.
# Description: "Environment and soils schema 2.1 update" updates the environment and soils data from schema version 2.0 to 2.1, except for the AIM dataset (scripted independently).
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
gates_1998_folder = '04_nps_gates_1998'
yukon_2003_folder = '06_nps_yukon_2003'
denali_1999_folder = '12_nps_denali_1999'
alagnak_2010_folder = '13_nps_alagnak_2010'
katmai_2000_folder = '14_nps_katmai_2000'
aniakchak_2009_folder = '15_nps_aniakchak_2009'
klondike_2011_folder = '21_nps_klondike_2011'
mulchatna_2022_folder = '30_accs_mulchatna_2022'
alphabet_2021_folder = '33_accs_alphabethills_2021'

# Define site visit files
gates_site_file = paste(data_folder,
                        gates_1998_folder,
                        '03_sitevisit_npsgates1998.csv',
                        sep = '/')
yukon_site_file = paste(data_folder,
                        yukon_2003_folder,
                        '03_sitevisit_npsyukon2003.csv',
                        sep = '/')
denali_site_file = paste(data_folder,
                         denali_1999_folder,
                         '03_sitevisit_npsdenali1999.csv',
                         sep = '/')
alagnak_site_file = paste(data_folder,
                          alagnak_2010_folder,
                          '03_sitevisit_npsalagnak2010.csv',
                          sep = '/')
katmai_site_file = paste(data_folder,
                         katmai_2000_folder,
                         '03_sitevisit_npskatmai2000.csv',
                         sep = '/')
aniakchak_site_file = paste(data_folder,
                            aniakchak_2009_folder,
                            '03_sitevisit_npsaniakchak2009.csv',
                            sep = '/')

# Define input files
gates_environment_file = paste(data_folder,
                               gates_1998_folder,
                               'schema_2_0',
                               '12_environment_npsgates1998.csv',
                               sep = '/')
gates_soils_file = paste(data_folder,
                         gates_1998_folder,
                         'schema_2_0',
                         '13_soils_npsgates1998.csv',
                         sep = '/')
gates_metrics_file = paste(data_folder,
                           gates_1998_folder,
                           'schema_2_1',
                           '13_soilmetrics_npsgates1998.xlsx',
                           sep = '/')
yukon_environment_file = paste(data_folder,
                               yukon_2003_folder,
                               'schema_2_0',
                               '12_environment_npsyukon2003.csv',
                               sep = '/')
yukon_soils_file = paste(data_folder,
                         yukon_2003_folder,
                         'schema_2_0',
                         '13_soils_npsyukon2003.csv',
                         sep = '/')
yukon_metrics_file = paste(data_folder,
                           yukon_2003_folder,
                           'schema_2_1',
                           '13_soilmetrics_npsyukon2003.xlsx',
                           sep = '/')
denali_environment_file = paste(data_folder,
                                denali_1999_folder,
                                'schema_2_0',
                                '12_environment_npsdenali1999.csv',
                                sep = '/')
denali_soils_file = paste(data_folder,
                          denali_1999_folder,
                          'schema_2_0',
                          '13_soils_npsdenali1999.csv',
                          sep = '/')
denali_metrics_file = paste(data_folder,
                            denali_1999_folder,
                            'schema_2_1',
                            '13_soilmetrics_npsdenali1999.xlsx',
                            sep = '/')
alagnak_environment_file = paste(data_folder,
                                 alagnak_2010_folder,
                                 'schema_2_0',
                                 '12_environment_npsalagnak2010.csv',
                                 sep = '/')
alagnak_soils_file = paste(data_folder,
                           alagnak_2010_folder,
                           'schema_2_0',
                           '13_soils_npsalagnak2010.csv',
                           sep = '/')
alagnak_metrics_file = paste(data_folder,
                             alagnak_2010_folder,
                             'schema_2_1',
                             '13_soilmetrics_npsalagnak2010.xlsx',
                             sep = '/')
katmai_environment_file = paste(data_folder,
                                katmai_2000_folder,
                                'schema_2_0',
                                '12_environment_npskatmai2000.csv',
                                sep = '/')
katmai_soils_file = paste(data_folder,
                          katmai_2000_folder,
                          'schema_2_0',
                          '13_soils_npskatmai2000.csv',
                          sep = '/')
katmai_metrics_file = paste(data_folder,
                            katmai_2000_folder,
                            'schema_2_1',
                            '13_soilmetrics_npskatmai2000.xlsx',
                            sep = '/')
aniakchak_environment_file = paste(data_folder,
                                   aniakchak_2009_folder,
                                   'schema_2_0',
                                   '12_environment_npsaniakchak2009.csv',
                                   sep = '/')
aniakchak_soils_file = paste(data_folder,
                             aniakchak_2009_folder,
                             'schema_2_0',
                             '13_soils_npsaniakchak2009.csv',
                             sep = '/')
aniakchak_metrics_file = paste(data_folder,
                               aniakchak_2009_folder,
                               'schema_2_1',
                               '13_soilmetrics_npsaniakchak2009.xlsx',
                               sep = '/')
klondike_environment_file = paste(data_folder,
                                  klondike_2011_folder,
                                  'schema_2_0',
                                  '12_environment_npsklondike2011.csv',
                                  sep = '/')
mulchatna_environment_file = paste(data_folder,
                                   mulchatna_2022_folder,
                                   'schema_2_0',
                                   '12_environment_accsmulchatna2022.csv',
                                   sep = '/')
mulchatna_soils_file = paste(data_folder,
                             mulchatna_2022_folder,
                             'schema_2_0',
                             '13_soils_accsmulchatna2022.csv',
                             sep = '/')
mulchatna_metrics_file = paste(data_folder,
                               mulchatna_2022_folder,
                               'schema_2_1',
                               '13_soilmetrics_accsmulchatna2022.xlsx',
                               sep = '/')
alphabet_environment_file = paste(data_folder,
                                  alphabet_2021_folder,
                                  'schema_2_0',
                                  '12_environment_accsalphabethills2021.csv',
                                  sep = '/')


# Define output data files
gates_environment_output = paste(data_folder,
                                 gates_1998_folder,
                                 '12_environment_npsgates1998.csv',
                                 sep = '/')
gates_metrics_output = paste(data_folder,
                             gates_1998_folder,
                             '13_soilmetrics_npsgates1998.csv',
                             sep = '/')
yukon_environment_output = paste(data_folder,
                                 yukon_2003_folder,
                                 '12_environment_npsyukon2003.csv',
                                 sep = '/')
yukon_metrics_output = paste(data_folder,
                             yukon_2003_folder,
                             '13_soilmetrics_npsyukon2003.csv',
                             sep = '/')
denali_environment_output = paste(data_folder,
                                  denali_1999_folder,
                                  '12_environment_npsdenali1999.csv',
                                  sep = '/')
denali_metrics_output = paste(data_folder,
                              denali_1999_folder,
                              '13_soilmetrics_npsdenali1999.csv',
                              sep = '/')
alagnak_environment_output = paste(data_folder,
                                   alagnak_2010_folder,
                                   '12_environment_npsalagnak2010.csv',
                                   sep = '/')
alagnak_metrics_output = paste(data_folder,
                               alagnak_2010_folder,
                               '13_soilmetrics_npsalagnak2010.csv',
                               sep = '/')
katmai_environment_output = paste(data_folder,
                                  katmai_2000_folder,
                                  '12_environment_npskatmai2000.csv',
                                  sep = '/')
katmai_metrics_output = paste(data_folder,
                              katmai_2000_folder,
                              '13_soilmetrics_npskatmai_2000.csv',
                              sep = '/')
aniakchak_environment_output = paste(data_folder,
                                     aniakchak_2009_folder,
                                     '12_environment_npsaniakchak2009.csv',
                                     sep = '/')
aniakchak_metrics_output = paste(data_folder,
                                 aniakchak_2009_folder,
                                 '13_soilmetrics_npsaniakchak2009.csv',
                                 sep = '/')
klondike_environment_output = paste(data_folder,
                                    klondike_2011_folder,
                                    '12_environment_npsklondike2011.csv',
                                    sep = '/')
mulchatna_environment_output = paste(data_folder,
                                     mulchatna_2022_folder,
                                     '12_environment_accsmulchatna2022.csv',
                                     sep = '/')
mulchatna_metrics_output = paste(data_folder,
                                 mulchatna_2022_folder,
                                 '13_soilmetrics_accsmulchatna2022.csv',
                                 sep = '/')
alphabet_environment_output = paste(data_folder,
                                    alphabet_2021_folder,
                                    '12_environment_accsalphabethills2021.csv',
                                    sep = '/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input site data
gates_site_data = read_csv(gates_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)
yukon_site_data = read_csv(yukon_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)
denali_site_data = read_csv(denali_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)
alagnak_site_data = read_csv(alagnak_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)
katmai_site_data = read_csv(katmai_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)
aniakchak_site_data = read_csv(aniakchak_site_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code)

# Define rename fields
rename_fields = c(site_visit_code = 'site_visit_id',
                  moisture_regime = 'moisture',
                  depth_15_percent_coarse_fragments_cm = 'depth_15percent_rock_cm')

# Process gates data
gates_soils_data = read_csv(gates_soils_file) %>%
  rename(any_of(rename_fields))
gates_environment_data = read_csv(gates_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(gates_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
gates_metrics_data = read_excel(gates_metrics_file, sheet = 'soil_metrics') %>%
  left_join(gates_site_data, by = 'site_code') %>%
  select(-site_code)

# Process yukon data
yukon_soils_data = read_csv(yukon_soils_file) %>%
  rename(any_of(rename_fields))
yukon_environment_data = read_csv(yukon_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(yukon_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
yukon_metrics_data = read_excel(yukon_metrics_file, sheet = 'soil_metrics') %>%
  left_join(yukon_site_data, by = 'site_code') %>%
  select(-site_code)

# Process denali data
denali_soils_data = read_csv(denali_soils_file) %>%
  rename(any_of(rename_fields))
denali_environment_data = read_csv(denali_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(denali_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
denali_metrics_data = read_excel(denali_metrics_file, sheet = 'soil_metrics') %>%
  left_join(denali_site_data, by = 'site_code') %>%
  select(-site_code)

# Process alagnak data
alagnak_soils_data = read_csv(alagnak_soils_file) %>%
  rename(any_of(rename_fields))
alagnak_environment_data = read_csv(alagnak_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(alagnak_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
alagnak_metrics_data = read_excel(alagnak_metrics_file, sheet = 'soil_metrics') %>%
  left_join(alagnak_site_data, by = 'site_code') %>%
  select(-site_code)

# Process katmai data
katmai_soils_data = read_csv(katmai_soils_file) %>%
  rename(any_of(rename_fields))
katmai_environment_data = read_csv(katmai_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(katmai_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
katmai_metrics_data = read_excel(katmai_metrics_file, sheet = 'soil_metrics') %>%
  left_join(katmai_site_data, by = 'site_code') %>%
  select(-site_code)

# Process aniakchak data
aniakchak_soils_data = read_csv(aniakchak_soils_file) %>%
  rename(any_of(rename_fields))
aniakchak_environment_data = read_csv(aniakchak_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(aniakchak_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c) %>%
  mutate(cryoturbation = replace_na(cryoturbation, 'NULL')) %>%
  mutate(dominant_texture_40_cm = replace_na(dominant_texture_40_cm, 'NULL')) %>%
  mutate(depth_15_percent_coarse_fragments_cm
         = replace_na(depth_15_percent_coarse_fragments_cm, -999)) %>%
  mutate(soil_class = replace_na(soil_class, 'NULL'))
aniakchak_metrics_data = read_excel(aniakchak_metrics_file, sheet = 'soil_metrics') %>%
  left_join(aniakchak_site_data, by = 'site_code') %>%
  select(-site_code)

# Process klondike data
klondike_environment_data = read_csv(klondike_environment_file) %>%
  mutate(cryoturbation = 'NULL') %>%
  mutate(dominant_texture_40_cm = 'NULL') %>%
  mutate(depth_15_percent_coarse_fragments_cm = -999) %>%
  mutate(soil_class = 'NULL') %>%
  rename(any_of(rename_fields))

# Process mulchatna data
mulchatna_soils_data = read_csv(mulchatna_soils_file) %>%
  rename(any_of(rename_fields))
mulchatna_environment_data = read_csv(mulchatna_environment_file) %>%
  rename(any_of(rename_fields)) %>%
  left_join(mulchatna_soils_data, by = 'site_visit_code') %>%
  select(-soil_measure_depth_cm, -soil_ph, -soil_conductivity_mus,
         -soil_temperature_deg_c)
mulchatna_metrics_data = read_excel(mulchatna_metrics_file, sheet = 'soil_metrics')

# Process alphabet data
alphabet_environment_data = read_csv(alphabet_environment_file) %>%
  mutate(cryoturbation = 'NULL') %>%
  mutate(dominant_texture_40_cm = 'NULL') %>%
  mutate(depth_15_percent_coarse_fragments_cm = -999) %>%
  mutate(soil_class = 'NULL') %>%
  rename(any_of(rename_fields))

# Create export lists
output_list = list(gates_environment_output,
                   gates_metrics_output,
                   yukon_environment_output,
                   yukon_metrics_output,
                   denali_environment_output,
                   denali_metrics_output,
                   alagnak_environment_output,
                   alagnak_metrics_output,
                   katmai_environment_output,
                   katmai_metrics_output,
                   aniakchak_environment_output,
                   aniakchak_metrics_output,
                   klondike_environment_output,
                   mulchatna_environment_output,
                   mulchatna_metrics_output,
                   alphabet_environment_output)
table_list = list(gates_environment_data,
                  gates_metrics_data,
                  yukon_environment_data,
                  yukon_metrics_data,
                  denali_environment_data,
                  denali_metrics_data,
                  alagnak_environment_data,
                  alagnak_metrics_data,
                  katmai_environment_data,
                  katmai_metrics_data,
                  aniakchak_environment_data,
                  aniakchak_metrics_data,
                  klondike_environment_data,
                  mulchatna_environment_data,
                  mulchatna_metrics_data,
                  alphabet_environment_data)

# Export output tables to csv
for (output in output_list) {
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = output, fileEncoding = 'UTF-8', row.names = FALSE)
}