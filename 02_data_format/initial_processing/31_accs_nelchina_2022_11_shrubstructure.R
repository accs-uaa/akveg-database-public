# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Shrub Structure for ACCS Nelchina 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-31
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Shrub Structure for ACCS Nelchina 2022 data" appends unique site visit codes and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range and match values listed in the database dictionary. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(RPostgres)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

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
shrub_structure_input = path(source_folder,"11_accs_nelchina.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "11_shrub_structure.xlsx")

# Define output dataset
shrub_structure_output = path(plot_folder, '11_shrubstructure_accsnelchina2022.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
shrub_structure_original = read_xlsx(path=shrub_structure_input)
site_visit_original = read_csv(site_visit_input, col_select=c("site_code", "site_visit_id"))
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
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Append site visit codes ----
shrub_structure = shrub_structure_original %>% 
  right_join(site_visit_original, by="site_code")

# Ensure all entries have a site visit code
shrub_structure %>% 
  filter(is.na(site_visit_id)) %>% 
  nrow()

# Ensure that all sites in Site Visit table have an entry
which(!site_visit_original$site_visit_id %in% unique(shrub_structure$site_visit_id))

# Obtain accepted taxonomic name ----
shrub_structure = shrub_structure %>% 
  mutate(name_original = str_to_lower(name_original)) %>% # Convert name original code to lowercase to match formatting in taxonomic table
  mutate(name_original = case_when(name_original == 'salarc' ~ 'salarct', # Correct typos in codes
                                   name_original == 'rotomsdec' ~ 'rhotomsdec',
                                   name_original == 'salbarr' ~ 'salbart',
                                   .default = name_original)) %>% 
  left_join(taxa_all, join_by(name_original == taxon_code))

# Ensure all codes matched with a name in the checklist
shrub_structure %>% 
  filter(is.na(taxon_name)) %>% 
  distinct(name_original)

# Rename & select columns ----
shrub_structure_final = shrub_structure %>% 
  select(-name_original) %>% 
  rename(site_visit_code = site_visit_id,
         name_original = taxon_name,
         name_adjudicated = taxon_name_accepted) %>% 
  mutate(shrub_subplot_area_m2 = round(shrub_subplot_area_m2, 3)) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(shrub_structure_final, is.na)
    , sum))

# Verify constrained values
unique(shrub_structure_final$shrub_class)
unique(shrub_structure_final$height_type)
unique(shrub_structure_final$cover_type)
unique(shrub_structure_final$shrub_subplot_area_m2)

# Verify that continuous values are within a reasonable range
summary(shrub_structure_final$cover_percent) # Between 0% and 100%
plot(shrub_structure_final$mean_diameter_cm ~ shrub_structure_final$height_cm)
hist(shrub_structure_final$number_stems) # Maximum value had 95% cover

# Export data ----
write_csv(shrub_structure_final,file=shrub_structure_output)

# Clear workspace ----
rm(list=ls())
