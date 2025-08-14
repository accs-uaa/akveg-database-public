# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Remove 'nps_bering_2003' project"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-19
# Usage: Must be executed in R version 4.5.1+.
# Description: "Remove 'nps_bering_2003' project" finds which plot folder contains the 'nps_bering_2003' project code and removes the project and associated sites and site visits from all tables in which it is included. The data in this project code are already included in other project codes; keeping data from this project code creates duplicate entries that we wish to avoid.
# ---------------------------------------------------------------------------

# Load libraries ----
library(purrr)
library(dplyr)
library(readr)
library(fs)
library(stringr)
library(glue)

# Define functions ----

## Function 1: Detect which folder contains the project code I'm looking for
find_project_code = function(project_list, search_code = 'nps_bering_2003') {
  found_folder = purrr::detect(project_list, .f = function(input_folder) {
    input_file = dir_ls(input_folder, regexp = "01_project")
    
    if (length(input_file) > 0) {
      project_data = read_csv(input_file, 
                              col_select = "project_code", 
                              show_col_types = FALSE)
      
      if ("project_code" %in% names(project_data) && any(project_data$project_code == search_code)) {
        cat(glue("Found project in ",
                 str_extract(input_folder, pattern="\\d{2}_\\w+_\\d{4}"),
                 "\n"))
        return(TRUE)
      }
    }
    return(FALSE)
  })
  
  return(found_folder)
}

## Function 2: Discard entries is nested list that contain specific project_code
discard_project_code = function(df, search_code = 'nps_bering_2003', 
                                column_names = c('project_code', 'establishing_project')) {
  
  # Identify column name that exists in the df
  existing_cols = intersect(column_names, names(df))
  
  if (length(existing_cols) == 0) {
    return(df)
  }
  
  filtered_df = df %>%
    filter(!if_any(
      .cols = all_of(existing_cols), 
      .fns = ~ .x == search_code
    ))
  
  return(filtered_df)
}

# Define directories ----
# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 
                   'ACCS_Teams', 'Vegetation', 'AKVEG_Database', 'Data', 
                   'Data_Plots')

# List project folders
project_list = dir_ls(path=data_folder, regexp = "\\d{2}_")

# Find project folder ----
project_folder = find_project_code(project_list, search_code = 'nps_bering_2003')

# Read in all files in project folder ----
file_list = dir_ls(project_folder, recurse=FALSE, glob="*csv")
data_list = map(file_list, ~ read_csv(.x, show_col_types = FALSE))

# Add project code column to vegetation cover table ----
## This is the only table that doesn't have a project code column
visit_codes = pluck(data_list, 3)
visit_codes = visit_codes %>% 
  select(project_code, site_visit_id)

data_list[[4]] = left_join(data_list[[4]], visit_codes, by = "site_visit_id")

# Remove nps_bering_2003 from data frames ----
filtered_data_list = map(data_list, discard_project_code)

## Drop project_code column from vegetation cover table
filtered_data_list[[4]] = select(filtered_data_list[[4]], -project_code)

# Rename columns to match new database schema ----
columns_lookup = c(site_visit_code = "site_visit_id", 
                   establishing_project_code = "establishing_project",
                   homogeneous = "homogenous")

renamed_data_list = map(filtered_data_list, ~ rename(.x, any_of(columns_lookup)))
walk(renamed_data_list, ~ print(names(.x)))

# Update constrained values to match database dictionary ----
renamed_data_list[[4]] = renamed_data_list[[4]] %>% 
  mutate(cover_type = case_when(cover_type == 'top cover' ~ 'top foliar cover',
                                cover_type == 'absolute cover' ~ 'absolute foliar cover'))
  
renamed_data_list[[4]] %>% distinct(cover_type)

# Export as CSV ----
file_names = map(file_list, ~ str_extract(.x, pattern="\\d{2}_\\w+_\\w+\\d{4}.csv"))

## Copy original data frames into a new folder
output_copy = map(file_names, ~ path(project_folder, "archive", "nps_bering_2003", .x))
walk2(data_list, output_copy, ~ write_csv(.x, file = .y))

## Output new files
output_new = map(file_names, ~ path(project_folder, .x))
walk2(renamed_data_list, output_new, ~ write_csv(.x, file = .y))

# Clear workspace ----
rm(list=ls())
