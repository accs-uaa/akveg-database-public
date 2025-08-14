# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Abiotic Top Cover for ACCS Ribdon 2019 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-05
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Abiotic Top Cover for ACCS Ribdon 2019 data" uses data from ground plots to extract information on abiotic top cover. The script appends unique site visit codes, fills in missing values, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts=FALSE)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '18_accs_ribdon_2019')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define inputs
cover_input = path(source_folder, "2019_RibdonRiver_CoverLongForm.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsribdon2019.csv")
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")

# Define output
abiotic_cover_output = path(plot_folder, "06_abiotictopcover_accsribdon2019.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
cover_original = read_xlsx(cover_input)
site_visit_original = read_csv(site_visit_input, col_select = c('site_code', 'site_visit_id'))
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
query_abiotic = "SELECT database_dictionary.dictionary_id as dictionary_id
     , database_schema.field as field
     , database_dictionary.data_attribute_id as data_attribute_id
     , database_dictionary.data_attribute as data_attribute
     , database_dictionary.definition as definition
FROM database_dictionary
    LEFT JOIN database_schema ON database_dictionary.field_id = database_schema.field_id
WHERE field = 'ground_element'
ORDER BY dictionary_id;"

# Read SQL table as dataframe
abiotic_list = as_tibble(dbGetQuery(akveg_connection, query_abiotic))

# Restrict query to abiotic elements ----
abiotic_list = abiotic_list %>% 
  select(data_attribute) %>% 
  filter(!(data_attribute %in% c('biotic', 'boulder', 'cobble', 'gravel', 'mineral soil', 'organic soil', 'stone'))) %>% # Remove ground elements
  rename(abiotic_element = data_attribute)

# Append site visit code ----
cover_data = cover_original %>%
  mutate(site_code = str_remove(Site, '2019'),
          site_code = str_replace(site_code, "-", "_")) %>% 
  left_join(site_visit_original, by = 'site_code') %>% 
  rename(site_visit_code = site_visit_id)

## Ensure all entries have a site visit code
cover_data %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Calculate total number of points per line ----
totalPoints = cover_data %>% 
  group_by(site_visit_code, Line) %>% 
  summarize(maxPoint = max(Point)) %>% 
  ungroup() %>% 
  group_by(site_visit_code) %>% 
  summarize(totalPoint = sum(maxPoint)) # All sites have 150 points per line

# Restrict cover entries ----
## Restrict to top cover by only selecting the first layer
## Restrict to abiotic elements by selecting for codes shorter than 6 letters
abiotic_cover = cover_data %>% 
  filter(Layer == 1 & str_length(Abbreviation) < 6 
         & Abbreviation != "MOSS" 
         & Abbreviation != 'ALGAE')

unique(abiotic_cover$Observation)

# Calculate top cover percent ----
abiotic_cover = abiotic_cover %>%  
  mutate(Observation = str_to_lower(Observation),
         abiotic_element = case_when(Observation == 'litter' ~ 'litter (< 2 mm)',
                                     Observation == 'organic soil' ~ 'soil',
                                     Observation == 'woody litter' ~ 'dead down wood (≥ 2 mm)',
                                     .default = Observation)) %>% 
  group_by(site_visit_code, abiotic_element) %>% 
  summarize(numberOfHits = n()) %>% 
  left_join(totalPoints, by="site_visit_code") %>%
  mutate(abiotic_top_cover_percent = round((numberOfHits / totalPoint * 100), digits=3)) %>% 
  select(all_of(template))
  
# Add missing abiotic elements ----

for (i in 1:length(unique(site_visit_original$site_visit_id))) {
  site_code = unique(site_visit_original$site_visit_id)[i]
  top_cover = abiotic_cover %>% filter(site_visit_code == site_code)
  
  # Determine which abiotic elements are not listed at that site
  missing_elements = abiotic_list %>% 
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
abiotic_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(total_entries = n()) %>% 
  filter(total_entries != 8) # 8 entries for each site (total number of abiotic elements)

abiotic_cover_final %>% 
  group_by(abiotic_element) %>% 
  summarize(total_entries = n()) %>% 
  filter(total_entries != nrow(site_visit_original)) # Entries should equal total number of sites

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_cover_final, is.na)
    , sum)
)

# Are the correct number of sites included?
abiotic_cover_final %>% 
  distinct(site_visit_code) %>% 
  nrow() == nrow(site_visit_original)

# Are values for abiotic top cover reasonable?
summary(abiotic_cover_final$abiotic_top_cover_percent)

# Export data ----
write_csv(abiotic_cover_final, file=abiotic_cover_output)

# Clear workspace ----
rm(list=ls())
