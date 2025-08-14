# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Ground Cover for ACCS Ribdon 2019 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-06
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Ground Cover for ACCS Ribdon 2019 data" calculates ground cover from line-point intercept data, appends unique site visit identifier, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
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
cover_input = path(source_folder, "2019_RibdonRiver_CoverLongForm.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsribdon2019.csv")
template_input = path(template_folder, "08_ground_cover.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Define output dataset
ground_cover_output = path(plot_folder, "08_groundcover_accsribdon2019.csv")

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
query_ground = "SELECT database_dictionary.dictionary_id as dictionary_id
     , database_schema.field as field
     , database_dictionary.data_attribute_id as data_attribute_id
     , database_dictionary.data_attribute as data_attribute
     , database_dictionary.definition as definition
FROM database_dictionary
    LEFT JOIN database_schema ON database_dictionary.field_id = database_schema.field_id
WHERE field = 'ground_element'
ORDER BY dictionary_id;"

# Define taxa query
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
, taxon_habit.taxon_habit as taxon_habit
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
  LEFT JOIN taxon_habit ON taxon_accepted.taxon_habit_id = taxon_habit.taxon_habit_id
WHERE taxon_habit.taxon_habit IN ('algae', 'crust', 'cyanobacteria', 'hornwort', 'lichen', 'liverwort', 'moss')
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

# Read SQL tables as dataframes
ground_list = as_tibble(dbGetQuery(akveg_connection, query_ground))
nonvascular_list = as_tibble(dbGetQuery(akveg_connection, query_taxa))

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
## Restrict to ground cover by selecting the last layer & the penultimate layer. Second-to-last layer takes precedence over abiotic elements if it is a non-vascular plant.
ground_cover = cover_data %>% 
  group_by(site_visit_code, Line, Point) %>% 
  mutate(maxLayer = max(Layer),
         taxon_code = str_to_lower(Abbreviation),
         taxon_code = case_when(taxon_code == 'algae' ~ 'ualgae',
                                taxon_code == 'brachythecium' ~ 'brachyt',
                                (taxon_code == 'brypse' | taxon_code == 'dicranella') ~ 'umoss',
                                taxon_code == 'clamit' ~ 'clamit1',
                                taxon_code == 'crustose' ~ 'ucrulic',
                                taxon_code == 'hypsub' ~ 'hypsubi',
                                taxon_code == 'biotic' ~ 'fgbiocru',
                                .default = taxon_code
                                )) %>% 
  ungroup() %>% 
  filter(Layer == maxLayer | (Layer == maxLayer - 1 & (taxon_code %in% nonvascular_list$taxon_code | Observation %in% c('Litter', 'Water', 'Animal Litter', 'Moss')))) %>%
  group_by(site_visit_code, Line, Point) %>%
  mutate(newLayer = 1:n(),
         minLayer = min(newLayer)) %>%  
  filter(newLayer == minLayer) %>% 
  ungroup()

# Correct ground element codes ----
## To match formatting used in data dictionary
ground_cover = ground_cover %>% 
  mutate(ground_element = str_to_lower(Observation),
         ground_element = case_when(ground_element %in% c('muck', 'duff', 'peat') ~ 'organic soil',
                                         ground_element == 'rock' ~ 'rock fragments',
                                         ground_element == 'litter' ~ 'litter (< 2 mm)',
                                    taxon_code %in% nonvascular_list$taxon_code ~ 'biotic',
                                    ground_element == 'moss' ~ 'biotic',
                                         grepl(pattern="dryas",x=ground_element) ~ "organic soil", # Basal hit not specified; assume forgot to enter soil 
                                         .default = ground_element))

## Ensure that all ground element codes match a code in the database dictionary
which(!(ground_cover$ground_element %in% ground_list$data_attribute))

# Calculate percent cover ----
ground_cover = ground_cover %>%
  group_by(site_visit_code,ground_element) %>% 
  summarize(numberOfHits = n()) %>% 
  left_join(totalPoints, by="site_visit_code") %>%
  mutate(ground_cover_percent = round((numberOfHits / totalPoint * 100), digits=3)) %>% 
  select(all_of(template))

# Add ground elements with 0% cover ----
for (i in 1:length(unique(site_visit_original$site_visit_id))) {
  site_code = unique(site_visit_original$site_visit_id)[i]
  ground_cover_subset = ground_cover %>% 
    filter(site_visit_code == site_code)
  
  # Determine which ground elements are not listed at that site
  missing_elements = ground_list %>% 
    rename(ground_element = data_attribute) %>% 
    filter(!(ground_element %in% c('soil', 'rock fragments'))) %>%  # Remove top cover elements; only included if they are present in the original dataset
    filter(!(ground_element %in% ground_cover_subset$ground_element)) %>% 
    select(ground_element) %>% 
    distinct()
  
  # Append missing elements to existing ground cover data
  missing_elements = missing_elements %>%
    mutate(ground_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(all_of(template)) %>% 
    bind_rows(ground_cover_subset) %>% 
    arrange(ground_element)
  
  # Populate dataframe
  if (i==1) {
    ground_cover_final = missing_elements
  } else {
    ground_cover_final = bind_rows(ground_cover_final, 
                                    missing_elements)
  }
}

rm(i, ground_cover_subset, site_code, missing_elements)

# Verify results
ground_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(count = n()) %>% 
  filter(count > 13) # 13 entries for each site (total number of ground elements); some sites have 14 or 15 in they included rock fragments or soil

ground_cover_final %>% 
  group_by(ground_element) %>% 
  summarize(count = n()) %>% 
  filter(count != nrow(site_visit_original)) # only 3-5 entries for rock fragments and soil 

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(ground_cover_final, is.na)
    , sum)
)

# Are the correct number of sites included?
ground_cover_final %>% 
  distinct(site_visit_code) %>% 
  nrow() == nrow(site_visit_original)

# Ensure ground cover values are between 0 and 100%
summary(ground_cover_final$ground_cover_percent)

# Ensure total % ground cover adds up to 100% for each site
ground_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(total_cover = round(sum(ground_cover_percent),digits=1)) %>% 
  filter(total_cover != 100)

# Export data ----
write_csv(ground_cover_final, file=ground_cover_output)

# Clear workspace ----
rm(list=ls())
