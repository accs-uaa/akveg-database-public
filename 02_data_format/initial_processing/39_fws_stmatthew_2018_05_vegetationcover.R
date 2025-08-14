# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for USFWS St Matthew 2018 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-24
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for USFWS St Matthew 2018 data" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/39_fws_stmatthew_2018')
source_folder = path(plot_folder, 'source', 'FieldData', 'Originals')
template_folder = path(project_folder, "Data/Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
veg_cover_input = path(source_folder, 'stmatthew_data_veg_cover.csv')
site_visit_input = path(plot_folder, "03_site_visit_fwsstmatthew2018.csv")
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_fwsstmatthew2018.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----
veg_cover_original = read_csv(veg_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define query
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

# Append site visit code ----
# Drops specimen collection plots
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

veg_cover_data = veg_cover_original %>% 
  mutate(site_code = str_remove(plot_id, "_2018")) %>% 
  right_join(site_visit_data, by ="site_code") # Use right join to drop specimen collection plots

# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover_data %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_data$site_visit_code %in% unique(veg_cover_data$site_visit_code))

# Remove abiotic elements from list ----
abiotic_elements = c("Bare Mineral Soil", "Water")

veg_cover_data = veg_cover_data %>% 
  filter(!(scientific_name %in% abiotic_elements))

# Obtain accepted taxonomic name ----

# Standardize spelling conventions to match AKVEG checklist
veg_cover_taxa = veg_cover_data %>%
  rename(name_original = scientific_name) %>% 
  mutate(name_original = case_when(name_original == "Unidentified lichen" ~ "lichen",
                                   name_original == "Unknown grass" ~ "grass (Poaceae)",
                                   name_original == "Unspecified moss, non-Sphagnum sp." ~ "moss",
                                   .default = str_remove(name_original, " sp\\."))) # Remove 'sp.' suffixed to the end of genera names

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Alopecurus magellanicus" ~ "Alopecurus",
                                      .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Does the dataset needs to be summarized i.e., are there any species originally identified as separate species that now need to be merged into a single one?
veg_cover_taxa %>% 
  group_by(site_visit_code, name_adjudicated) %>% 
  nrow == veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated) %>%  
  nrow() # If TRUE, no need to summarize

# Populate remaining columns ----
veg_cover_summary = veg_cover_taxa %>%
  mutate(cover_type = "absolute foliar cover",
         dead_status = "FALSE",
         cover_percent = signif(cover_percent, digits = 6)) %>% # Round to 3 decimal places
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_summary, is.na)
    , sum))

# Are the values for percent cover between 0.1% and 100%?
summary(veg_cover_summary$cover_percent)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_code)) == length(unique(veg_cover_summary$site_visit_code))

# Export data ----
write_csv(veg_cover_summary, veg_cover_output)

# Clean workspace ----
rm(list=ls())