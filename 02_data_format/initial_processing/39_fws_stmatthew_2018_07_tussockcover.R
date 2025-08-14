# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover Table for USFWS St Matthew 2018 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-24
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Whole Tussock Cover for USFWS St Matthew 2018 data" formats whole tussock cover data. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/39_fws_stmatthew_2018')
source_folder = path(plot_folder, 'source', 'FieldData', 'Originals')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
tussock_cover_input = path(source_folder, 'stmatthew_data_veg_structure.csv')
site_visit_input = path(plot_folder, '03_sitevisit_fwsstmatthew2018.csv')
template_input = path(template_folder, "07_whole_tussock_cover.xlsx")

# Define output datasets
tussock_cover_output = path(plot_folder, '07_wholetussockcover_fwsstmatthew2018.csv')

# Read in data ----
tussock_cover_original = read_csv(tussock_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

tussock_cover = tussock_cover_original %>% 
  mutate(site_code = str_remove(plot_id, "_2018")) %>% 
  right_join(site_visit_data, join_by("site_code")) # Use right join to drop specimen collection plots

# Ensure all entries have a site visit code
tussock_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Format tussock cover ----
tussock_cover = tussock_cover %>% 
  rename(tussock_percent_cover = whole_tussocks_cover) %>% 
  mutate(cover_type = "absolute foliar cover") %>% 
  select(all_of(template))

# QA/QC ----
# Ensure reasonable range of values
summary(tussock_cover$tussock_percent_cover) # All values in this dataset are zero

# Export as CSV ----
write_csv(tussock_cover, tussock_cover_output)

# Clear workspace ----
rm(list=ls())
