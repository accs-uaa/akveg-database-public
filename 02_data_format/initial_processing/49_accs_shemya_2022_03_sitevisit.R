# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for ACCS Shemya Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-03
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Visit Table for ACCS Shemya Data" formats site visit data collected and entered by ACCS for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, formats structural class data, populates fields with values that match constrained values in the AKVEG Database, and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/49_accs_shemya_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
site_visit_input = path(source_folder,'03_Site_Visit_Shemya_2022.xlsx')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Define output datasets
site_visit_output = path(plot_folder, '03_sitevisit_accsshemya2022.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing','connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define query
query_structural = 'SELECT * FROM structural_class'

# Read SQL table as dataframe
structural_data = as_tibble(dbGetQuery(akveg_connection, query_structural))

# Correct site code ----
# There are two sites listed with the same code (eas22r313), even though they relate to different sites (different coordinates, personnel, structural class, etc.)
# From Anjanette: the site with forb meadow as structural class is correct
site_visit = site_visit_original %>% 
  filter(!(site_visit_id == "eas22r313" & structural_class == "gm"))

# Format visit date ----
summary(site_visit$observe_date) # Ensure reasonable date range

site_visit = site_visit %>% 
  mutate(observe_date = as.character(as.Date(observe_date)))

# Create site visit code ----
# Combine visit date with site code to generate site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_visit_code = str_c(site_code,date_string,sep="_")) 

# Format structural class ----
# Use class names instead of code
site_visit = site_visit %>% 
  rename(structural_class_code = structural_class) %>% 
  left_join(structural_data,by=c("structural_class_code"))

# Address null value
site_visit = site_visit %>% 
  mutate(structural_class = case_when(structural_class_code == "es" ~ "sedge emergent",
                                      .default = structural_class))

# Match fields to constrained values ----
# Change env_observer to NULL since full environmental data were not collected
# Correct scope for bryophyte and lichen identification
site_visit = site_visit %>% 
  mutate(project_code = 'accs_shemya_2022',
         data_tier = 'map development & verification',
         env_observer = "NULL",
         soils_observer = "NULL",
         scope_bryophyte = "common species",
         scope_lichen = "common species",
         homogenous = TRUE) %>%
  select(all_of(template))

# QA/QC ----

# Ensure there aren't any null values that need to be addressed
cbind(
  lapply(
    lapply(site_visit, is.na)
    , sum)
)

# Verify values of categorical columns
unique(site_visit$veg_observer)
unique(site_visit$veg_recorder)
unique(site_visit$env_observer) # Should be NULL
unique(site_visit$soils_observer) # Should be NULL
unique(site_visit$structural_class)
unique(site_visit$scope_vascular)

# Export as CSV ----
write_csv(site_visit, site_visit_output)

# Clear workspace ----
rm(list=ls())