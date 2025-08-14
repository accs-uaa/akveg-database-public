# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for ABR 2019 Arctic Refuge data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-25
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Whole Tussock Cover for ABR 2019 Arctic Refuge data" combines data from aerial and ground plots, appends unique site visit codes, fills in missing values, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '29_abr_arcticrefuge_2019')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
cover_aerial_input = path(source_folder, "abr_anwr_ns_lc_veg_aerial_deliverable_part1_longer.csv")
cover_ground_input = path(source_folder, "abr_anwr_ns_lc_veg_deliverable.csv")
site_input = path(plot_folder, "02_site_abrarcticrefuge2019.csv")
site_visit_input = path(plot_folder, "03_sitevisit_abrarcticrefuge2019.csv")
template_input = path(template_folder, "07_Whole_Tussock_Cover.xlsx")

# Define outputs ----
tussock_cover_output = path(plot_folder, "07_wholetussockcover_abrarcticrefuge2019.csv")

# Read in data ----
aerial_original = read_csv(file=cover_aerial_input)
ground_original = read_csv(file=cover_ground_input, col_select = c('plot_id', 'whole_tussocks_cover'))
site_original = read_csv(site_input, col_select = c('site_code', 'perspective'))
site_visit_original = read_csv(site_visit_input, col_select = c('site_code', 'site_visit_code'))
template = colnames(read_xlsx(path=template_input))

# Format site data ----
site_visit = site_visit_original %>% 
  mutate(original_code = str_c(site_code, "2019", sep="_")) %>% 
  left_join(site_original, by='site_code')

# Combine aerial and ground plots ----
# The same sites are included in both aerial and ground plots, with different values for tussock cover. Use data from site table to exclude sites based on perspective.

# Format aerial plots
aerial_cover = aerial_original %>% 
  filter(cover_type == 'whole_tussocks_top_cover') %>% 
  select(plot_id, percent_cover) %>% 
  right_join(site_visit, join_by('plot_id'=='original_code')) %>% 
  filter(perspective == 'aerial')

ground_cover = ground_original %>% 
  right_join(site_visit, join_by('plot_id'=='original_code')) %>% 
  filter(perspective == 'ground') %>% 
  rename(percent_cover = whole_tussocks_cover)

tussock_cover = ground_cover %>% 
  bind_rows(aerial_cover)

# Populate remaining columns ----
tussock_final = tussock_cover %>% 
  mutate(cover_type = case_when(perspective == 'aerial' ~ 'top foliar cover',
                                perspective == 'ground' ~ 'absolute foliar cover'),
         tussock_percent_cover = signif(percent_cover, digits = 3)) %>%
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

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_code)) == length(unique(tussock_final$site_visit_code))

# Export data ----
write_csv(tussock_final, tussock_cover_output)
