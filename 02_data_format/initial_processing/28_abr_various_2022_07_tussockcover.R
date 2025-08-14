# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for ABR Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-05
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Vegetation Cover for Whole Tussock Cover data" appends unique site visit identifiers, performs QA/QC checks, and enforces formatting to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts=FALSE)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '28_abr_various_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_private_read')

# Define datasets ----

# Define input datasets
cover_input = path(source_folder, 'tnawrocki_deliverable_two_veg.txt')
site_visit_input = path(plot_folder, '03_sitevisit_abrvarious2022.csv')
template_input = path(template_folder, "07_Whole_Tussock_Cover.xlsx")

# Define output dataset
tussock_cover_output = path(plot_folder, '07_wholetussockcover_abrvarious2022.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input, col_select=c('site_code',
                                                              'site_visit_id'))
cover_original = read_delim(cover_input, delim = '|', 
                                col_select=c('plot_id', 'whole_tussocks_cover'))
template = colnames(read_xlsx(path=template_input))

# Append site visit code ----
# Format plot id column to match site code
tussock_cover = cover_original %>%
  mutate(observe_year = case_when(str_starts(plot_id, 'SUWA_') ~ str_extract(plot_id, pattern='\\d{4}'),
                                  .default = NA),
         observe_year = case_when(!is.na(observe_year) ~ str_sub(observe_year, start=-2),
                                  .default = NA)) %>% 
  mutate(site_code = case_when(str_ends(plot_id, pattern="_\\d{4}") ~ str_remove(plot_id, pattern="_\\d{4}"),
                               str_ends(plot_id, pattern='-veg') ~ str_remove(plot_id, pattern='-veg'),
                               .default = plot_id)) %>% 
  mutate(site_code = case_when(str_starts(plot_id, 'SUWA_') ~ str_c(site_code,
                                                                    observe_year, 
                                                                    sep = ""),
                               .default = site_code)) %>% 
  right_join(site_visit_original, by='site_code') %>%  # Use right join to drop excluded sites
  rename(site_visit_code = site_visit_id)

## Ensure all entries have a site visit code
tussock_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Format dataframe ----
tussock_final = tussock_cover %>% 
  mutate(tussock_percent_cover = case_when(whole_tussocks_cover == '0E-16' ~ '0',
                                           .default = whole_tussocks_cover),
         tussock_percent_cover = signif(as.numeric(tussock_percent_cover), digits = 3)) %>%
  filter(tussock_percent_cover != -999) %>%   # Drop sites with no data
  mutate(cover_type = 'absolute foliar cover') %>% 
  select(all_of(template)) %>% 
  arrange(site_visit_code)

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(tussock_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(tussock_final$tussock_percent_cover)

# Export data ----
write_csv(tussock_final, tussock_cover_output)

# Clear workspace ----
rm(list=ls())
