# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Data
# Author: Timm Nawrocki and Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-20
# Usage: Script should be executed in R 4.1.2+.
# Description: "Format Site Data" extracts plot-level data from the FIA database and creates additional, descriptive fields to match minimum standards required by the AKVEG database.
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
input_template <- file.path(root_folder, "Data/Data_Entry","02_Site.xlsx")

# Define outputs ----
output_site = file.path(project_folder,
                    'temp_files',
                    '02_usfs_fia.csv')

# Read in tabular data ----
template <- colnames(read_xlsx(path=input_template))

# Connect to FIA SQLite Database ----
fia_connection = dbConnect(drv = SQLite(), dbname = FIA_database)

# Extract and format data ----

# Define plot query for FIA database to select all plot numbers and coordinates
query_plot = 'SELECT PLOT as plot_number
    , LAT as latitude_dd
    , LON as longitude_dd
    FROM PLOT
ORDER BY plot_number;'

# Get and define plot metadata
site_fia = as_tibble(dbGetQuery(fia_connection, query_plot)) %>%
  mutate(site_code = case_when(nchar(as.integer(plot_number)) == 1 ~
                                 paste('FIAINT_', '0000', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 2 ~
                                 paste('FIAINT_', '000', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 3 ~
                                 paste('FIAINT_', '00', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 4 ~
                                 paste('FIAINT_', '0', plot_number, sep = ''),
                               nchar(as.integer(plot_number)) == 5 ~
                                 paste('FIAINT_', plot_number, sep = ''),
                               TRUE ~ 'none')) %>%
  mutate(establishing_project_code = 'FIA_Interior') %>%
  mutate(perspective = 'ground') %>%
  mutate(cover_method = 'semi-quantitative visual estimate') %>%
  mutate(h_datum = 'NAD83') %>%
  mutate(h_error_m = 804) %>%
  mutate(positional_accuracy = "") %>% 
  mutate(plot_dimensions_m = '804 radius') %>%
  mutate(location_type = "") %>% 
  select(all_of(template))

# QA/QC ----
# Ensure that no site codes are labeled as 'none'
site_fia %>% filter(site_code == "none")

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_fia, is.na)
    , sum)
)

# Export data ----
write_csv(site_fia, file = output_site)