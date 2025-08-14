# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Abiotic Top Cover for ABR 2019 Arctic Refuge data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-26
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Abiotic Top Cover for ABR 2019 Arctic Refuge data" uses data from aerial and ground plots to extract information on abiotic top cover. The script appends unique site visit codes, fills in missing values, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
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
plot_folder = path(project_folder, 'Data', 'Data_Plots', '29_abr_arcticrefuge_2019')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define inputs
abiotic_cover_input = path(source_folder, "abr_anwr_ns_lc_veg_aerial_deliverable_part1_longer.csv") # Includes top cover for ground plots
site_visit_input = path(plot_folder, "03_sitevisit_abrarcticrefuge2019.csv")
vegetation_cover_input = path(plot_folder, '05_vegetationcover_abrarcticrefuge2019.csv')
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")

# Define output
abiotic_cover_output = path(plot_folder, "06_abiotictopcover_abrarcticrefuge2019.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
abiotic_cover_original = read_csv(file=abiotic_cover_input)
site_visit_original = read_csv(site_visit_input, col_select = c('site_code', 'site_visit_code'))
vegetation_cover_original = read_csv(vegetation_cover_input)
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

# Append site visit code ----
abiotic_cover = site_visit_original %>% 
  mutate(original_code = str_c(site_code, "2019", sep="_")) %>% 
  left_join(abiotic_cover_original, join_by('original_code' == 'plot_id')) %>% 
  mutate(abiotic_element = str_remove(cover_type, '_top_cover'), # Clean up formatting
         abiotic_element = str_replace_all(abiotic_element, "_", " "))


## Ensure all entries have a site visit code
abiotic_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Restrict to abiotic elements ----
abiotic_list = abiotic_list %>% 
  select(data_attribute) %>% 
  filter(!(data_attribute %in% c('biotic', 'boulder', 'cobble', 'gravel', 'mineral soil', 'organic soil', 'stone'))) %>% # Remove ground elements
  rename(abiotic_element = data_attribute)
           
# Add snow and water to a single class (all values for snow except for 1 site are 0%)
total_water_cover = abiotic_cover %>% 
  filter(abiotic_element == 'snow' | abiotic_element == 'water') %>% 
  group_by(site_visit_code) %>% 
  summarise(abiotic_top_cover_percent = sum(percent_cover)) %>% 
  mutate(abiotic_element = 'water') %>% 
  select(all_of(template))

abiotic_cover = abiotic_cover %>%
  filter(abiotic_element != 'water' & abiotic_element != 'snow') %>%  # Added later
  mutate(abiotic_element = case_when(abiotic_element == 'bedrock' ~ 'bedrock (exposed)',
                                     abiotic_element == 'surface fragment' ~ 'rock fragments',
                                     abiotic_element == 'bare soil' ~ 'soil',
                                     abiotic_element == 'dead down wood' ~ 'dead down wood (≥ 2 mm)',
                                     abiotic_element == 'litter' ~ 'litter (< 2 mm)',
                                     .default = abiotic_element)) %>% 
  rename(abiotic_top_cover_percent = percent_cover) %>% 
  select(all_of(template)) %>% 
  bind_rows(total_water_cover) %>% 
  filter(abiotic_element %in% abiotic_list$abiotic_element)
  
# Add missing abiotic elements ----

for (i in 1:length(unique(site_visit_original$site_visit_code))) {
  site_code = unique(site_visit_original$site_visit_code)[i]
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

# Sum of vegetation cover + abiotic top cover for aerial plots will not equal 100% because vegetation cover data is not exhaustive e.g., does not include all bryophytes

# Export data ----
write_csv(abiotic_cover_final, file=abiotic_cover_output)

# Clear workspace ----
rm(list=ls())
