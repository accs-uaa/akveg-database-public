# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Visit Data
# Author: Timm Nawrocki and Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-20
# Usage: Script should be executed in R 4.1.2+.
# Description: "Format Site Visit Data" extracts survey-level data from the FIA database and creates additional, descriptive fields to match minimum standards required by the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(tidyverse)
library(readxl)
library(RSQLite)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "35_fia_various_2021")

# Define inputs ----
FIA_database = file.path(project_folder,
                     'source',
                     'Public_AK_SQlite.db')

# Define template for AKVEG table
input_template <- file.path(root_folder, "Data/Data_Entry","03_Site_Visit.xlsx")

# Define outputs ----
output_visit = file.path(project_folder,
                    'temp_files',
                    '03_usfs_fia.csv')

# Read in tabular data ----
template <- colnames(read_xlsx(path=input_template))

# Connect to FIA SQLite Database ----
fia_connection = dbConnect(drv = SQLite(), dbname = FIA_database)

# Extract and format data ----

# Define plot query for FIA database to select all plot numbers and survey dates
query_plot = 'SELECT PLOT as plot_number
    , MEASDAY as day
    , MEASMON as month
    , MEASYEAR as year
    FROM PLOT
ORDER BY plot_number;'

# Get and define plot metadata
visit_fia = as_tibble(dbGetQuery(fia_connection, query_plot)) %>%
  mutate(project_code = 'FIA_Interior',
         observe_date = if_else(day < 10,
                                    paste(year, '-0', month, '-0', day, sep =''),
                                    paste(year, '-0', month, '-', day, sep = '')),
         date_string = if_else(day < 10,
                               paste0(year, '0', month, '0', day),
                               paste0(year, '0', month, '', day)),
         site_code = case_when(nchar(as.integer(plot_number)) == 1 ~
                                 paste('FIAINT_', '0000', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 2 ~
                                 paste('FIAINT_', '000', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 3 ~
                                 paste('FIAINT_', '00', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 4 ~
                                 paste('FIAINT_', '0', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 5 ~
                                 paste('FIAINT_', plot_number, sep = ''),
                               TRUE ~ 'none'), 
         site_visit_id = paste(site_code,date_string, sep = "_"),
         data_tier = "map development & verification",
         veg_observer = "unknown",
         veg_recorder = "unknown",
         env_observer = "unknown",
         soils_observer = "unknown",
         structural_class = "n/assess",
         scope_vascular = 'partial',
         scope_bryophyte = 'functional group or life form',
         scope_lichen = 'functional group or life form') %>% 
  select(all_of(template))

# QA/QC ----
# Ensure that no site codes are labeled as 'none'
visit_fia %>% filter(site_code == "none")

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(visit_fia, is.na)
    , sum)
)

# Export data ----
write_csv(visit_fia, file = output_visit)