# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Build personnel list
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2024-11-19
# Usage: Script should be executed in R 4.0.0+.
# Description: "Build personnel list" exports a csv table of observer and recorder names from the cover and environment tables to enter into the database dictionary.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(rjson)
library(RPostgres)
library(stringr)
library(tibble)
library(tidyr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database'

# Define input folders
data_folder = path(drive,
                   root_folder,
                   'Data/Data_Plots')
repository_folder = path(drive,
                         'ACCS_Work/Repositories/akveg-database')
credential_folder = path(drive, root_folder, 'Credentials')

# Define input file
project_list_path = path(repository_folder, 
                         '03_data_insertion', 
                         '00_e_List_Projects.json')

# Define output files
output_csv = paste(data_folder,
                   'processed',
                   'personnel_unique.csv',
                   sep = '/'
)

# Read in list of projects
target_paths = fromJSON(file = project_list_path)
target_paths = target_paths$projects

# Set target pattern
project_pattern = '^01_project.*csv'
visit_pattern = '^03_sitevisit.*csv'

# Create empty list to store files
project_list = list()
visit_list = list()

# Populate project file list
for (target_path in target_paths) {
  # Create full path
  full_path = paste(data_folder, target_path, sep = '/')
  # Find file and append to file list
  files = list.files(full_path, pattern = project_pattern)
  if (!is.na(files[1])) {
    project_list = append(project_list, paste(full_path, files[1], sep = '/'))
  }
}

# Populate visit file list
for (target_path in target_paths) {
  # Create full path
  full_path = paste(data_folder, target_path, sep = '/')
  # Find file and append to file list
  files = list.files(full_path, pattern = visit_pattern)
  if (!is.na(files[1])) {
    visit_list = append(visit_list, paste(full_path, files[1], sep = '/'))
  }
}

# Create read function
read_data_site_visit_rename = function (data_file) {
  rename_fields = c(site_visit_code = 'site_visit_id', homogeneous = 'homogenous')
  input_data = read_csv(data_file) %>%
    rename(any_of(rename_fields)) %>%
    mutate(veg_observer = case_when(veg_observer == 'Jess Grunblatt' ~ 'Jesse Grunblatt',
                                    veg_observer == 'NULL' ~ 'none',
                                    veg_observer == 'Unknown' ~ 'unknown',
                                    veg_observer == 'None' ~  'none',
                                    .default = veg_observer)) %>%
    mutate(veg_recorder = case_when(veg_recorder == 'Jess Grunblatt' ~ 'Jesse Grunblatt',
                                    veg_recorder == 'NULL' ~ 'none',
                                    veg_recorder == 'Unknown' ~ 'unknown',
                                    veg_recorder == 'None' ~  'none',
                                    project_code == 'accs_ribdon_2019' & is.na(veg_recorder) ~ 'Timm Nawrocki',
                                    .default = veg_recorder)) %>%
    mutate(env_observer = case_when(env_observer == 'Jess Grunblatt' ~ 'Jesse Grunblatt',
                                    env_observer == 'NULL' ~ 'none',
                                    env_observer == 'Unknown' ~ 'unknown',
                                    env_observer == 'None' ~  'none',
                                    .default = env_observer)) %>%
    mutate(soils_observer = case_when(soils_observer == 'Jess Grunblatt' ~ 'Jesse Grunblatt',
                                    soils_observer == 'NULL' ~ 'none',
                                    soils_observer == 'Unknown' ~ 'unknown',
                                    soils_observer == 'None' ~  'none',
                                    .default = soils_observer)) %>%
    mutate(project_code = case_when(project_code == 'accs_nelchina_2022' ~ 'accs_nelchina_2023',
                                    TRUE ~ project_code))
  return(input_data)
}

# Read files into list
project_list = lapply(project_list, read_csv, show_col_types = FALSE)
visit_list = lapply(visit_list, read_data_site_visit_rename)

# Combine site list into data frame
project_data = do.call(rbind, project_list)
site_visit_data = do.call(rbind, visit_list)

# Subset unique values from each name column
manager = project_data %>%
  distinct(manager) %>%
  rename(personnel = manager) %>%
  drop_na()
veg_observer = site_visit_data %>%
  distinct(veg_observer) %>%
  rename(personnel = veg_observer) %>%
  drop_na()
veg_recorder = site_visit_data %>%
  distinct(veg_recorder) %>%
  rename(personnel = veg_recorder) %>%
  drop_na()
env_observer = site_visit_data %>%
  distinct(env_observer) %>%
  rename(personnel = env_observer) %>%
  drop_na()
soil_observer = site_visit_data %>%
  distinct(soils_observer) %>%
  rename(personnel = soils_observer) %>%
  drop_na()

# Combine personnel into a single dataframe of unique values
personnel_data = rbind(manager,
                       veg_observer,
                       veg_recorder,
                       env_observer,
                       soil_observer)
personnel_unique = personnel_data %>%
  distinct(personnel) %>%
  filter(personnel != 'none' & personnel != 'unknown')

# Export the joined table as a csv file
write.csv(personnel_unique, file = output_csv, fileEncoding = 'UTF-8', row.names = FALSE)
