# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# AIM NPR-A environment and soils data update
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2023-03-26
# Usage: Script should be executed in R 4.0.0+.
# Description: "AIM NPR-A environment and soils data update" updates the AIM NPR-A environment and soils data from schema version 2.0 to 2.1 and includes missing data.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')
soils_folder = paste(drive,
                     root_folder,
                     'Projects/VegetationEcology/BLM_AIM/Soils_Working',
                     sep = '/')

# Define project
aim_2017_folder = '01_aim_npra_2017'
aim_2019_folder = '16_aim_gmt2_2019'
aim_2021_folder = '32_aim_various_2021'

# Define input files
vegetation_2017_file = paste(data_folder,
                             aim_2017_folder,
                             'schema_2_0',
                             '05_vegetationcover_aimnpra2017.csv',
                             sep = '/')
site_visit_2017_file = paste(data_folder,
                             aim_2017_folder,
                             '03_sitevisit_aimnpra2017.csv',
                             sep = '/')
site_visit_2019_file = paste(data_folder,
                             aim_2019_folder,
                             '03_sitevisit_aimgmt22019.csv',
                             sep = '/')
site_visit_2021_file = paste(data_folder,
                             aim_2021_folder,
                             '03_sitevisit_aimvarious2021.csv',
                             sep = '/')
soil_metrics_file = paste(soils_folder,
                          '13_soil_metrics_npra_all.xlsx',
                          sep = '/')
soil_horizons_file = paste(soils_folder,
                           '14_soils_horizons_npra_all_v3.xlsx',
                           sep = '/')
environment_file = paste(soils_folder,
                         '12_environment_npra_all.xlsx',
                         sep = '/')
site_2021_file = paste(data_folder,
                       aim_2021_folder,
                       'schema_2_0',
                       '02_site_aimvarious2021.csv',
                       sep = '/')
vegetation_2021_file = paste(data_folder,
                             aim_2021_folder,
                             'schema_2_0',
                             '05_vegetationcover_aimvarious2021.csv',
                             sep = '/')

# Define output data files
vegetation_2017_output = paste(data_folder,
                               aim_2017_folder,
                               '05_vegetationcover_aimnpra2017.csv',
                               sep = '/')
environment_2017_output = paste(data_folder,
                                aim_2017_folder,
                                '12_environment_aimnpra2017.csv',
                                sep = '/')
environment_2019_output = paste(data_folder,
                                aim_2019_folder,
                                '12_environment_aimgmt22019.csv',
                                sep = '/')
environment_2021_output = paste(data_folder,
                                aim_2021_folder,
                                '12_environment_aimvarious2021.csv',
                                sep = '/')
metrics_2017_output = paste(data_folder,
                            aim_2017_folder,
                            '13_soilmetrics_aimnpra2017.csv',
                            sep = '/')
metrics_2019_output = paste(data_folder,
                            aim_2019_folder,
                            '13_soilmetrics_aimgmt22019.csv',
                            sep = '/')
metrics_2021_output = paste(data_folder,
                            aim_2021_folder,
                            '13_soilmetrics_aimvarious2021.csv',
                            sep = '/')
horizons_2017_output = paste(data_folder,
                             aim_2017_folder,
                             '14_soilhorizons_aimnpra2017.csv',
                             sep = '/')
horizons_2019_output = paste(data_folder,
                             aim_2019_folder,
                             '14_soilhorizons_aimgmt22019.csv',
                             sep = '/')
horizons_2021_output = paste(data_folder,
                             aim_2021_folder,
                             '14_soilhorizons_aimvarious2021.csv',
                             sep = '/')
site_2021_output = paste(data_folder,
                         aim_2021_folder,
                         '02_site_aimvarious2021.csv',
                         sep = '/')
vegetation_2021_output = paste(data_folder,
                               aim_2021_folder,
                               '05_vegetationcover_aimvarious2021.csv',
                               sep = '/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read and correct vegetation cover site codes
vegetation_2017_data = read_csv(vegetation_2017_file) %>%
  rename(site_visit_code = site_visit_id) %>%
  mutate(site_visit_code = case_when(
    site_visit_code == 'ADST-71_20170729' ~ 'ADST-71_20170727',
    site_visit_code == 'ADST-76_20170729' ~ 'ADST-76_20170722',
    site_visit_code == 'ADST-79_20170729' ~ 'ADST-79_20170722',
    site_visit_code == 'AFS-71_20170729' ~ 'AFS-71_20170727',
    site_visit_code == 'AFS-78_20170728' ~ 'AFS-78_20170725',
    site_visit_code == 'CPBWM-73_20170726' ~ 'CPBWM-73_20170723',
    site_visit_code == 'CPHCP-72_20170729' ~'CPHCP-72_20170725',
    site_visit_code == 'CPHCP-78_20170723' ~ 'CPHCP-78_20170721',
    site_visit_code == 'FLST-71_20170729' ~ 'FLST-71_20170727',
    site_visit_code == 'FLST-80_20170729' ~ 'FLST-80_20170726',
    site_visit_code == 'GMT2-046_20190804' ~ 'GMT2-046_20190808',
    site_visit_code == 'GMT2-085_20210726' ~ 'GMT2-085_20210802',
    site_visit_code == 'GMT2-114_20210726' ~ 'GMT2-114_20210802',
    TRUE ~ site_visit_code))

# Read input site visit data into data frames
site_visit_2017 = read_csv(site_visit_2017_file)
site_visit_2019 = read_csv(site_visit_2019_file)
site_visit_2021 = read_csv(site_visit_2021_file)

# Bind site visit rows to single data frame
site_visit_data = rbind(site_visit_2017, site_visit_2019, site_visit_2021) %>%
  filter(project_code != 'aim_campbell_2018' &
           project_code != 'aim_kobuknortheast_2021' &
           project_code != 'aim_kobukwest_2021') %>%
  rename(site_visit_code = site_visit_id) %>%
  select(site_code, site_visit_code, project_code) %>%
  # Correct site visit codes
  mutate(site_visit_code = case_when(
    site_visit_code == 'ADST-71_20170729' ~ 'ADST-71_20170727',
    site_visit_code == 'ADST-76_20170729' ~ 'ADST-76_20170722',
    site_visit_code == 'ADST-79_20170729' ~ 'ADST-79_20170722',
    site_visit_code == 'AFS-71_20170729' ~ 'AFS-71_20170727',
    site_visit_code == 'AFS-78_20170728' ~ 'AFS-78_20170725',
    site_visit_code == 'CPBWM-73_20170726' ~ 'CPBWM-73_20170723',
    site_visit_code == 'CPHCP-72_20170729' ~'CPHCP-72_20170725',
    site_visit_code == 'CPHCP-78_20170723' ~ 'CPHCP-78_20170721',
    site_visit_code == 'FLST-71_20170729' ~ 'FLST-71_20170727',
    site_visit_code == 'FLST-80_20170729' ~ 'FLST-80_20170726',
    site_visit_code == 'GMT2-046_20190804' ~ 'GMT2-046_20190808',
    site_visit_code == 'GMT2-085_20210726' ~ 'GMT2-085_20210802',
    site_visit_code == 'GMT2-114_20210726' ~ 'GMT2-114_20210802',
    TRUE ~ site_visit_code))

# Read environment, soil metrics, and soil horizons
environment_data = read_excel(environment_file, sheet = 'environment') %>%
  rename(site_visit_code = site_visit_id) %>%
  left_join(site_visit_data, by = 'site_visit_code')
soil_metrics_data = read_excel(soil_metrics_file, sheet = 'soil_metrics') %>%
  rename(site_visit_code = site_visit_id) %>%
  left_join(site_visit_data, by = 'site_visit_code')
soil_horizons_data = read_excel(soil_horizons_file, sheet = 'horizons') %>%
  left_join(site_visit_data, by = 'site_code')

# Parse environment and soils data
environment_2017_data = environment_data %>%
  filter(project_code == 'aim_npra_2017') %>%
  select(-site_code, -project_code)
environment_2019_data = environment_data %>%
  filter(project_code == 'aim_gmt2_2019') %>%
  select(-site_code, -project_code)
environment_2021_data = environment_data %>%
  filter(project_code == 'aim_gmt2_2021') %>%
  select(-site_code, -project_code)
metrics_2017_data = soil_metrics_data %>%
  filter(project_code == 'aim_npra_2017') %>%
  select(-site_code, -project_code)
metrics_2019_data = soil_metrics_data %>%
  filter(project_code == 'aim_gmt2_2019') %>%
  select(-site_code, -project_code)
metrics_2021_data = soil_metrics_data %>%
  filter(project_code == 'aim_gmt2_2021') %>%
  select(-site_code, -project_code)
horizons_2017_data = soil_horizons_data %>%
  filter(project_code == 'aim_npra_2017') %>%
  select(-site_code, -project_code)
horizons_2019_data = soil_horizons_data %>%
  filter(project_code == 'aim_gmt2_2019') %>%
  select(-site_code, -project_code)
horizons_2021_data = soil_horizons_data %>%
  filter(project_code == 'aim_gmt2_2021') %>%
  select(-site_code, -project_code)

# Correct site codes and site visit codes for 2021 GMT-2 data
site_2021_data = read_csv(site_2021_file) %>%
  mutate(site_code = case_when(site_code == 'GMT2-51' ~ 'GMT2-051',
                               site_code == 'GMT2-34' ~ 'GMT2-034',
                               site_code == 'GMT2-94' ~ 'GMT2-094',
                               site_code == 'GMT2-88' ~ 'GMT2-088',
                               site_code == 'GMT2-87' ~ 'GMT2-087',
                               site_code == 'GMT2-22' ~ 'GMT2-022',
                               site_code == 'GMT2-85' ~ 'GMT2-085',
                               site_code == 'GMT2-38' ~ 'GMT2-038',
                               site_code == 'GMT2-42' ~ 'GMT2-042',
                               site_code == 'GMT2-17' ~ 'GMT2-017',
                               site_code == 'GMT2-70' ~ 'GMT2-070',
                               site_code == 'GMT2-58' ~ 'GMT2-058',
                               site_code == 'GMT2-98' ~ 'GMT2-098',
                               site_code == 'GMT2-49' ~ 'GMT2-049',
                               site_code == 'GMT2-99' ~ 'GMT2-099',
                               site_code == 'GMT2-53' ~ 'GMT2-053',
                               site_code == 'GMT2-56' ~ 'GMT2-056',
                               site_code == 'GMT2-86' ~ 'GMT2-086',
                               site_code == 'GMT2-80' ~ 'GMT2-080',
                               site_code == 'GMT2-89' ~ 'GMT2-089',
                               TRUE ~ site_code))
vegetation_2021_data = read_csv(vegetation_2021_file) %>%
  mutate(site_visit_id = case_when(site_visit_id == 'GMT2-17_20210727' ~ 'GMT2-017_20210727',
                                   site_visit_id == 'GMT2-22_20210801' ~ 'GMT2-022_20210801',
                                   site_visit_id == 'GMT2-34_20210731' ~ 'GMT2-034_20210731',
                                   site_visit_id == 'GMT2-38_20210731' ~ 'GMT2-038_20210731',
                                   site_visit_id == 'GMT2-42_20210801' ~ 'GMT2-042_20210801',
                                   site_visit_id == 'GMT2-49_20210801' ~ 'GMT2-049_20210801',
                                   site_visit_id == 'GMT2-51_20210728' ~ 'GMT2-051_20210728',
                                   site_visit_id == 'GMT2-53_20210729' ~ 'GMT2-053_20210729',
                                   site_visit_id == 'GMT2-56_20210729' ~ 'GMT2-056_20210729',
                                   site_visit_id == 'GMT2-58_20210727' ~ 'GMT2-058_20210727',
                                   site_visit_id == 'GMT2-70_20210801' ~ 'GMT2-070_20210801',
                                   site_visit_id == 'GMT2-80_20210730' ~ 'GMT2-080_20210730',
                                   site_visit_id == 'GMT2-85_20210802' ~ 'GMT2-085_20210802',
                                   site_visit_id == 'GMT2-86_20210725' ~ 'GMT2-086_20210725',
                                   site_visit_id == 'GMT2-87_20210726' ~ 'GMT2-087_20210726',
                                   site_visit_id == 'GMT2-88_20210726' ~ 'GMT2-088_20210726',
                                   site_visit_id == 'GMT2-89_20210726' ~ 'GMT2-089_20210726',
                                   site_visit_id == 'GMT2-94_20210730' ~ 'GMT2-094_20210730',
                                   site_visit_id == 'GMT2-98_20210730' ~ 'GMT2-098_20210730',
                                   site_visit_id == 'GMT2-99_20210728' ~ 'GMT2-099_20210728',
                                   TRUE ~ site_visit_id))

# Create export lists
output_list = list(vegetation_2017_output,
                   environment_2017_output,
                   environment_2019_output,
                   environment_2021_output,
                   metrics_2017_output,
                   metrics_2019_output,
                   metrics_2021_output,
                   horizons_2017_output,
                   horizons_2019_output,
                   horizons_2021_output,
                   site_2021_output,
                   vegetation_2021_output)
table_list = list(vegetation_2017_data,
                  environment_2017_data,
                  environment_2019_data,
                  environment_2021_data,
                  metrics_2017_data,
                  metrics_2019_data,
                  metrics_2021_data,
                  horizons_2017_data,
                  horizons_2019_data,
                  horizons_2021_data,
                  site_2021_data,
                  vegetation_2021_data)

# Export output tables to csv
for (output in output_list) {
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = output, fileEncoding = 'UTF-8', row.names = FALSE)
}