# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-23
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Whole Tussock Cover for USFWS Pribilof Islands 2022 data" formats whole tussock cover data. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
tussock_cover_input = path(source_folder,'pribilofs2022.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_fwspribilof2022.csv')
template_input = path(template_folder, "07_whole_tussock_cover.xlsx")

# Define output datasets
tussock_cover_output = path(plot_folder, '07_wholetussockcover_fwspribilof2022.csv')

# Read in data ----
tussock_cover_original = read_xlsx(tussock_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

tussock_cover = tussock_cover_original %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_")) %>% # Convert dashes to underscore
  right_join(site_visit_data, by = "site_code") # Use right join to drop 6 anthropogenic sites

# Ensure all entries have a site visit code
tussock_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Format tussock cover ----
tussock_cover = tussock_cover %>% 
  rename(tussock_percent_cover = Cov_wtuss) %>% 
  mutate(cover_type = "absolute canopy cover") %>% 
  select(all_of(template))

# QA/QC ----
# Ensure reasonable range of values
summary(tussock_cover$tussock_percent_cover) # All values in this dataset are zero

# Export as CSV ----
write_csv(tussock_cover, tussock_cover_output)

# Clear workspace ----
rm(list=ls())