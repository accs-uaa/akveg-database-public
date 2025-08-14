# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for NPS Arctic Network 2014 data"
# Author: Amanda Droghini
# Last Updated: 2025-04-09
# Usage: Must be executed in R 4.4.3+.
# Description: "Format Site Visit Table for NPS Arctic Network 2014 data" reads in CSV tables exported from the NPS ARCN SQL database and formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '35_nps_arcn_2014')
source_folder = path(plot_folder, 'source')
workspace_folder = path(plot_folder, 'working')
reference_folder = path(project_folder, 'References')
template_folder = path(project_folder, 'Data', "Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(drive,root_folder, 'Servers_Websites', 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define input datasets
site_visit_input = path(source_folder, "dbo_plot.csv")
metadata_input = path(source_folder,"dbo_metadatachoices.csv")
site_lookup_input = path(workspace_folder, "lookup_site_codes.csv")
structural_ref_input = path(reference_folder, 'structural_class_ref.xlsx')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Define output dataset
site_visit_output = path(plot_folder, "03_sitevisit_npsarcn2014.csv")

# Read in data ----
site_visit_original = read_csv(site_visit_input)
metadata_original = read_csv(metadata_input)
site_lookup_original = read_csv(site_lookup_input)
structural_ref = read_xlsx(structural_ref_input, sheet="nps_arcn_ecotype")
template = colnames(read_xlsx(template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define database dictionary query
query_dictionary = 'SELECT database_dictionary.dictionary_id as dictionary_id
, database_schema.field as field
, database_dictionary.data_attribute_id as data_attribute_id
, database_dictionary.data_attribute as data_attribute
, database_dictionary.definition as definition
FROM database_dictionary
LEFT JOIN database_schema ON database_dictionary.field_id = database_schema.field_id
ORDER BY dictionary_id;'

# Read SQL table as dataframe
dictionary_class = as_tibble(dbGetQuery(akveg_connection, query_dictionary)) %>% 
  filter(field=='structural_class')

# Format site code ----
site_visit = site_visit_original %>% 
  right_join(site_lookup_original,
                       by=c('Node', 'Plot')) # Drop 1 site with incomplete data

# Ensure all sites in Site table are in Site Visit table
which(!(site_lookup_original$site_code %in% site_visit$site_code))
which(!(site_visit$site_code %in% site_lookup_original$site_code))

# Format date & site visit code ----

# Format date
site_visit = site_visit %>% 
  mutate(observe_date = as.Date(DateTimeConverted))

# Ensure date range is reasonable
hist(as.Date(site_visit$observe_date), 
     breaks = "day", 
     xlab = "Survey Date") 
summary(as.Date(site_visit$observe_date))

# Create site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_remove_all(observe_date, "-"),
         site_visit_code = str_c(site_code, date_string, sep = "_"),
         observe_date = as.character.Date(observe_date))

# Verify results
head(site_visit$observe_date)
head(site_visit$site_visit_code)

# Format personnel names ----
# Use metadata names to obtain full names of personnel
metadata = metadata_original %>% 
  filter(LookupTable == "initials") %>% 
  select(Choice, ChoiceDescription)

site_visit = site_visit %>% 
  left_join(metadata, by=c("InitialsSoilSite"="Choice")) %>% 
  rename(soils_observer = ChoiceDescription) %>%
  left_join(metadata, by=c("InitialsPointsRead"="Choice")) %>% 
  rename(veg_observer = ChoiceDescription) %>% 
  left_join(metadata, by=c("InitialsPointsRecord"="Choice")) %>% 
  rename(veg_recorder = ChoiceDescription) %>% 
  mutate(veg_observer = if_else(is.na(veg_observer),"unknown",veg_observer), # Convert null values (and one missing vlaue, 'KD'), to 'unknown'
         veg_recorder = if_else(is.na(veg_recorder),"unknown",veg_recorder),
         env_observer = soils_observer) # Assume soils person also recorded environmental data

# Format structural class ----
# Use reference table to re-classify ecotype to appropriate structural class value
site_visit = site_visit %>% 
  left_join(structural_ref, by="Ecotype") %>% 
  mutate(structural_class = str_to_lower(structural_class))

site_visit %>% filter(is.na(structural_class)) # All sites have a structural class

# Populate remaining columns ----
site_visit_final = site_visit %>% 
  mutate(project_code = "nps_arcn_2014",
         data_tier = "ecological land classification",
         scope_vascular = "exhaustive",
         scope_bryophyte = "common species",
         scope_lichen = "common species",
         homogeneous = TRUE) %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Verify personnel names
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)
unique(site_visit_final$env_observer)
unique(site_visit_final$soils_observer)

# Verify that all structural class values match a constrained value
which(!(unique(site_visit_final$structural_class) %in% dictionary_class$data_attribute))

# Verify homogeneous values
table(site_visit_final$homogeneous)

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
