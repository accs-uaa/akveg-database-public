# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-15
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Visit Table for ACCS Chenega  Data" formats site visit data collected and entered by ACCS for ingestion into the AKVEG Database. The script creates site visit codes, formats dates and personnel names, populates fields with values that match constrained values in the AKVEG Database, and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
site_visit_input = path(source_folder,'03_Site_Visit_Chenega_2022.xlsx')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Define output datasets
site_visit_output = path(plot_folder, '03_sitevisit_accschenega2022.csv')

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

# Format site code ----
# Remove abbreviation for 'ground' (GRN)
site_visit = site_visit_original %>% 
  mutate(site_code = str_replace_all(site_code, "CHE_GRN_", "CHEN_"))

# Ensure all names are unique
length(unique(site_visit$site_code)) # Should be 18

# Format visit date ----
summary(site_visit$observe_date) # Ensure reasonable date range

site_visit = site_visit %>% 
  mutate(observe_date = as.character(as.Date(observe_date)))

# Create site visit code ----
# Combine visit date with site code to generate site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_visit_code = str_c(site_code,date_string,sep="_")) 

# Format staff names ----
# Type out Anjanette's full name
# Change soils_observer to NULL since soils data were not collected
site_visit = site_visit %>% 
  mutate(env_observer = case_when(env_observer == "A Steer" ~ "Anjanette Steer",
                                  .default = env_observer),
         soils_observer = "NULL")

# Match fields to constrained values ----
# Correct data tier
site_visit = site_visit %>% 
  mutate(project_code = 'accs_chenega_2022',
         data_tier = 'vegetation classification',
         soils_observer = "NULL",
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
unique(site_visit$env_observer)
unique(site_visit$soils_observer)

# Ensure structural class values match with values in AKVEG database
which(!site_visit$structural_class %in% structural_data$structural_class)

# Export as CSV ----
write_csv(site_visit, site_visit_output)

# Clear workspace ----
rm(list=ls())