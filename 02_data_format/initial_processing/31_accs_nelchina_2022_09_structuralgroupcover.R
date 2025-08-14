# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Structural Group Cover for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-31
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Structural Group Cover for ACCS Nelchina data" appends unique site visit codes and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range and match values listed in the database dictionary. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(tidyr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(drive,root_folder, 'Servers_Websites', 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define input datasets
structural_cover_input = path(source_folder, "09_accs_nelchina.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "09_Structural_Group_Cover.xlsx")

# Define output datasets
structural_cover_output = path(plot_folder, "09_structuralgroupcover_accsnelchina2022.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
structural_cover_original = read_xlsx(path=structural_cover_input)
site_visit_original = read_csv(site_visit_input, col_select=c('site_visit_id', 'site_code'))
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define taxa query
query_structural = "SELECT database_dictionary.data_attribute as data_attribute
FROM database_dictionary
    LEFT JOIN database_schema on database_dictionary.field_id = database_schema.field_id
WHERE field='structural_group';"

# Read SQL table as dataframe
structural_groups = as_tibble(dbGetQuery(akveg_connection, query_structural))

# Format data ----
# Only one site is included for now. 
# Need to convert vegetation cover data to structural group for remaining sites.

# Append site visit id
# Keep only required columns
structural_cover = structural_cover_original %>%
  left_join(site_visit_original, by="site_code") %>%
  mutate(structural_cover_type = 'absolute foliar cover') %>% 
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(structural_cover, is.na)
    , sum)
)

# Ensure percent cover values are between 0 and 100
summary(structural_cover$structural_cover_percent)

# Ensure group names match constrained values in database
structural_cover %>% select(structural_group) %>% 
  arrange(structural_group) == structural_groups %>% 
  select(data_attribute) %>% 
  arrange(data_attribute) 

# Export data ----
write_csv(structural_cover, file = structural_cover_output)

# Clear workspace ----
rm(list=ls())
