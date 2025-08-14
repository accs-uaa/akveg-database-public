# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover Table for Harvard University Alder Data"
# Author: Calvin Heslop, Harvard University
# Last Updated: 2024-08-07
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Vegetation Cover Table for Harvard University Alder Data" formats vegetation cover data collected and entered by Calvin Heslop for ingestion into the AKVEG Database. The script standardizes taxonomic names, ensures values are within reasonable ranges, and adds required metadata fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readxl)
library(sf)
library(stringr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/50_harvard_alder_2023')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input dataset
veg_cover_input = path(source_folder, 'Model_validation_points.shp')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_harvardalder2023.csv')

# Read in data ----
cover_Heslop_original = read_sf(veg_cover_input)
template = colnames(read_xlsx(path=template_input))

# Remove incomplete plots ----

# Remove rows with missing cover estimates
cover_heslop = subset(cover_Heslop_original, !is.na(alder_cove))

# Remove ArtPos sites (n=2)
cover_heslop = cover_heslop %>% 
  filter(!(grepl(pattern = "ArtPos", name)))

# Fill in missing dates ----
# Seven sites with no dates, arbitrarily assign a date to them (I chose the last date of the survey season)
cover_heslop = cover_heslop %>% 
  mutate(time = case_when(is.na(time) ~ max(time, na.rm=TRUE),
                          .default = time))

# Re-code zero values ----
# In AKVEG, values that indicate absences (listed as 0 in this dataset) are coded as -999
cover_heslop = cover_heslop %>% 
  mutate(cover_percent = case_when(alder_cove == 0 ~ -999,
                                   .default = alder_cove))

# Ensure all sites listed as having no alder (alder_bina = 0) have a percent value of -999
cover_heslop %>% 
  filter(alder_bina == 0) %>% 
  distinct(cover_percent)

# Populate remaining fields ----
cover_heslop = as.data.frame(cover_heslop) %>% 
  rename(site_code = 'name') %>% 
  mutate(observe_date = substr(time, start = 1, stop = 10) %>% 
           str_replace_all(pattern = '/', '-')) %>% 
  mutate(date_string = str_replace_all(observe_date, pattern = '-', ''),
    site_visit_code = paste0(site_code, '_',date_string), # Create site visit codes
    name_original = 'Alnus viridis ssp. fruticosa', # Add species name
    name_adjudicated = 'Alnus alnobetula ssp. fruticosa',
    cover_type = 'absolute foliar cover',
    dead_status = "FALSE",
  ) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(cover_heslop, is.na)
    , sum))

# Are the values for percent cover either -999 or between 0% and 100%?
temp = cover_heslop %>% filter(cover_percent != -999)
summary(temp$cover_percent)

# Export as CSV ----
write.csv(cover_heslop, veg_cover_output,
          fileEncoding = "UTF-8",
          row.names = FALSE)

# Clear workspace ----
rm(list=ls())
