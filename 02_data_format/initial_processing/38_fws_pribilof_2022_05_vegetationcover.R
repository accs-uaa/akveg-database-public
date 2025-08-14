# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for USFWS Pribilof Islands 2022 data" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species by converting Braun-Blanquet classes to a numerical estimate of cover. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, performs QA/QC checks, and enforces values to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input dataset
site_visit_input = path(plot_folder, '03_sitevisit_fwspribilof2022.csv')
veg_cover_input = path(source_folder, 'pribilofs2022.xlsx')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_fwspribilof2022.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----
veg_cover_original = read_xlsx(veg_cover_input)
template = colnames(read_xlsx(path=template_input))
site_visit_original = read_csv(site_visit_input)

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
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

veg_cover_data = veg_cover_original %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_")) %>% # Convert dashes to underscore
  right_join(site_visit_data, by = "site_code") # Use right join to drop 6 anthropogenic sites

# Explore cover data ----

# Ensure that all sites in Site Visit table have cover data
length(unique(site_visit_data$site_code)) == length(unique(veg_cover_data$site_code))

# Are there any plots that did not have plant species in them?
# 10 plots with 100% open water
veg_cover_data %>% 
  filter(`Number of species` == 0) %>% 
  select(site_code, `Cover open water (%)`)

# Select only relevant columns
veg_cover_data = veg_cover_data %>%
  select(site_visit_code, 
         `Aconitum delphiniifolium`:`Arctophila fulva`)

# Convert to long format ----
veg_cover_long = veg_cover_data %>%
  pivot_longer(`Aconitum delphiniifolium`:`Arctophila fulva`, 
               names_to="name_original", values_to="braun_blanquet_class") %>% 
  filter(!is.na(braun_blanquet_class)) # Null value means species was not found in the plot

# Confirm that number of sites w/ cover data is 10 fewer than number of sites in Site Visit table
length(unique(veg_cover_long$site_visit_code)) # Should be 85

# Obtain accepted taxonomic name ----

# Standardize spelling conventions to match AKVEG checklist
veg_cover_taxa = veg_cover_long %>% 
  mutate(name_original = str_remove(name_original, " species"), # Remove 'species' suffixed to the end of genera names
         name_original = str_replace(name_original, "s\\.", "ssp\\."),
         name_original = str_replace(name_original, "v\\.", "var\\."))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Minuartia" ~ "forb",
                                      name_original == "Potentilla uniflora" ~ "Potentilla vulcanicola",
                                   .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Convert Braun-Blanquet classes ----
# Use Table 2 on page 309  of Westhoff and van der Maarel 1978: Use the midpoint when a range of values is provided
veg_cover_taxa$braun_blanquet_class = as.factor(veg_cover_taxa$braun_blanquet_class)
unique(veg_cover_taxa$braun_blanquet_class)

veg_cover_percent = veg_cover_taxa %>% 
  mutate(cover_percent = case_when(braun_blanquet_class == 1 ~ 0,
                                   braun_blanquet_class == 2 ~ 1,
                                   braun_blanquet_class == 3 ~ 2.5,    
                                   braun_blanquet_class == 4 ~ 4,
                                   braun_blanquet_class == 5 ~ (5+10)/2,
                                   braun_blanquet_class == 6 ~ (11+25)/2,
                                   braun_blanquet_class == 7 ~ (26+50)/2,
                                   braun_blanquet_class == 8 ~ (51+75)/2,
                                   braun_blanquet_class == 9 ~ (76+100)/2))

# Ensure values were converted properly
table(veg_cover_percent$braun_blanquet_class, 
      veg_cover_percent$cover_percent)

# Check whether the dataset needs to be summarized i.e., are there any species originally identified as separate species that now need to be merged into a single one?
veg_cover_percent %>% 
  group_by(site_visit_code, name_adjudicated) %>% 
  nrow == veg_cover_percent %>% 
  group_by(site_visit_code, name_original, name_adjudicated) %>%  
  nrow()

# Populate remaining columns ----
veg_cover_summary = veg_cover_percent %>%
  mutate(cover_type = "absolute canopy cover",
         dead_status = "FALSE") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_summary, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_summary$cover_percent)

# Are the correct number of sites included? In this case, 10 sites should have no entry because they were 100% water
length(unique(site_visit_original$site_visit_code)) - 10 == length(unique(veg_cover_summary$site_visit_code))

# Export data ----
write_csv(veg_cover_summary, veg_cover_output)

# Clean workspace ----
rm(list=ls())