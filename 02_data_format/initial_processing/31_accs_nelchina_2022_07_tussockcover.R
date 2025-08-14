# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-28
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Whole Tussock Cover for ACCS Nelchina data" appends unique site visit codes, fills in missing values, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(tidyr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
tussock_cover_input = path(source_folder, "07_accs_nelchina.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "07_Whole_Tussock_Cover.xlsx")

# Define outputs ----
tussock_cover_output = path(plot_folder, "07_wholetussockcover_accsnelchina2022.csv")

# Read in data ----
tussock_cover_original = read_xlsx(path=tussock_cover_input)
site_visit_original = read_csv(site_visit_input, col_select=c('site_visit_id', 'site_code'))
template = colnames(read_xlsx(path=template_input))

# Format data ----

# Append site visit id
# Correct sites with missing data
# Keep only required columns
tussock_cover = tussock_cover_original %>%
  right_join(site_visit_original, by="site_code") %>%
  mutate(cover_type = replace_na(cover_type, "absolute foliar cover"),
         tussock_percent_cover = case_when(site_visit_id == 'NLN4_996_20220715' ~ 20,
                                           site_visit_id == 'NLS3_338_20220706' ~ 3,
                                           is.na(tussock_percent_cover) ~ 0,
                                           .default = tussock_percent_cover)) %>% 
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(tussock_cover, is.na)
    , sum)
)

# Ensure there is an entry for each site visit
length(unique(tussock_cover$site_visit_code)) == nrow(site_visit_original)

# Ensure percent cover values are between 0 and 100
summary(tussock_cover$tussock_percent_cover)

# Export data ----
write_csv(tussock_cover,file=tussock_cover_output)

# Clear workspace ----
rm(list=ls())
