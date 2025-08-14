# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Abiotic Top Cover for select sites from NPS Arctic Network 2014"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-07-08
# Usage: Must be executed in R version 4.4.3+.
# Description: "Calculate Abiotic Top Cover for select sites from NPS Arctic Network 2014" uses processed ground cover and vegetation cover tables to calculate percent abiotic top cover for sites that do not have any vegetation cover data. The script must therefore be run after 05_vegetationcover and 08_vegetationcover. This step allows the dataset to meet the minimum requirements of each site having either vegetation cover or abiotic top cover. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '35_nps_arcn_2014')
template_folder = path(project_folder, "Data", "Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define inputs ----
veg_cover_input = path(plot_folder, '05_vegetationcover_npsarcn2014.csv')
ground_cover_input = path(plot_folder, '08_groundcover_npsarcn2014.csv')
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")

# Define output dataset
abiotic_cover_output = path(plot_folder, "06_abiotictopcover_npsarcn2014.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
ground_cover_original = read_csv(ground_cover_input)
veg_cover_original = read_csv(veg_cover_input)
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
query_ground = "SELECT * FROM ground_element;"

# Read SQL table as dataframe
ground_elements = as_tibble(dbGetQuery(akveg_connection, query_ground))

# Format abiotic elements list ----
# Remove entries that are only included in ground cover table only
abiotic_elements = ground_elements %>% 
  filter(!(ground_element %in% c("biotic", "boulder", "cobble", "gravel", "mineral soil", "organic soil", "stone"))) %>% 
  rename(abiotic_element = ground_element)

# Format sites with no vegetation cover ----
abiotic_cover = ground_cover_original %>% 
  filter(!(site_visit_code %in% veg_cover_original$site_visit_code)) %>% 
  mutate(abiotic_element = case_when(ground_element == 'gravel' ~ 'rock fragments',
                                     .default = ground_element)) %>% 
  group_by(site_visit_code, abiotic_element) %>% 
  summarise(abiotic_top_cover_percent = sum(ground_cover_percent)) %>% 
  select(all_of(template))

# Add abiotic elements with 0% cover ----
for (i in 1:length(unique(abiotic_cover$site_visit_code))) {
  site_code = unique(abiotic_cover$site_visit_code)[i]
  top_cover = abiotic_cover %>% filter(site_visit_code == site_code)
  
  # Determine which abiotic elements are not listed at that site
  missing_elements = abiotic_elements %>% 
    filter(!(abiotic_element %in% top_cover$abiotic_element)) %>% 
    select(abiotic_element) %>% 
    distinct()
  
  # Append missing elements to existing abiotic top cover data
  missing_elements = missing_elements %>%
    mutate(abiotic_top_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(all_of(template)) %>% 
    bind_rows(top_cover) %>% 
    arrange(abiotic_element)
  
  # Populate dataframe
  if (i==1) {
    abiotic_cover_final = missing_elements
  } else {
    abiotic_cover_final = bind_rows(abiotic_cover_final, 
                                    missing_elements)
  }
}

rm(i, top_cover, site_code, missing_elements)

# Verify results
table(abiotic_cover_final$site_visit_code) # 8 entries for each site (total number of abiotic elements)
table(abiotic_cover_final$abiotic_element) # 3 entries for each abiotic element (total number of sites)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_cover, is.na)
    , sum))

# Verify that total cover percent per site equals 100
abiotic_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(total_percent = sum(abiotic_top_cover_percent)) %>% 
  filter(total_percent != 100) %>% # All sites at 100%
  arrange(total_percent)

# Export data ----
write_csv(abiotic_cover_final, abiotic_cover_output)

# Clean workspace ----
rm(list=ls())
