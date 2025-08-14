# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Ground cover for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-05
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Ground Cover for ACCS Nelchina data" calculates ground cover from line-point intercept data, appends unique site visit identifier, and renames columns to match the AKVEG template. The script also performs QA/QC checks to ensure that values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
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
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define input datasets
lpi_cover_input = path(plot_folder,"source","05_accs_nelchina_lpi_surveys.xlsx")
extra_site_input = file.path(plot_folder, "source","08_accs_nelchina.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "08_ground_cover.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Define output dataset
ground_cover_output = path(plot_folder, "08_groundcover_accsnelchina2022.csv")

# Read in data ----
lpi_cover_original = read_xlsx(path=lpi_cover_input, col_types=c("text","numeric","numeric",
                                                                 "text","text","text",
                                                                 "text","text","text",
                                                                 "text","text"))
site_visit_original = read_csv(site_visit_input, col_select=c('site_visit_id', 'site_code'))
extra_site_original = read_xlsx(extra_site_input)
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
abiotic_list_original = as_tibble(dbGetQuery(akveg_connection, query_ground))
nonvascular_list = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Format ground cover elements ----
## Remove elements that are only included in abiotic top cover
abiotic_list = abiotic_list_original %>% 
  rename(ground_element = data_attribute,
         ground_element_code = data_attribute_id) %>% 
  filter(!(ground_element %in% c('rock fragments', 'soil')))

# Append site visit code ----
cover_data_wide = left_join(lpi_cover_original, site_visit_original, by="site_code")
extra_site = extra_site_original %>% 
  left_join(site_visit_original,by="site_code") %>% 
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template))

# Ensure all sites have an associated site visit id
cover_data_wide %>% filter(is.na(site_visit_id))
extra_site %>% filter(is.na(site_visit_code))
cover_data_wide %>% distinct(site_visit_id) %>% nrow() + 1 == nrow(site_visit_original)

# Correct entry ----
## Nothing follows after SALPUL but basal hit is not designated. 
cover_data_wide = cover_data_wide %>% 
  mutate(layer_2 = case_when(site_visit_id == 'NLS3_319_20220707' & line == 1 & point == 10 ~ 'SALPUL-B',
                             .default = layer_2))

# Convert cover data to long format ----
cover_data_long = pivot_longer(cover_data_wide, 
                               cols = layer_1:layer_8,
                               names_to = "strata",
                               names_prefix = "layer_",
                               values_to = "code",
                               values_drop_na = TRUE)

# Correct ground element codes ----
# To match formatting used in data dictionary
cover_data_long = cover_data_long %>% 
  mutate(taxon_code = str_to_lower(code),
         taxon_code = case_when(taxon_code == 'dicran' ~ 'dicranel',
                                taxon_code == 'fgfeamos' ~ 'fgmosfea',
                                taxon_code == 'brachy' ~ 'brachyt',
                                taxon_code == 'polytr' ~ 'polytric',
                                taxon_code == 'fgturmos' ~ 'fgmostur',
                                taxon_code == 'fgcrulic' ~ 'ucrulic',
                                taxon_code == 'fgfollic' ~ 'ufollic',
                                taxon_code == 'callie' ~ 'calliergn',
                                taxon_code == 'sphagnum' ~ 'sphagn', 
                                .default = taxon_code),
         ground_element_code = case_when(code == 'O' ~ 'OS',
                                         code == 'HL' ~ 'L',
                                         code == 'S' ~ 'MS',
                                         code == 'W' ~ 'WA',
                                         grepl(pattern="-B",x=code) ~ "B", # basal hits become 'biotic' element
                                         taxon_code %in% nonvascular_list$taxon_code ~ 'B',
                                         .default = code))

# Restrict to ground-level stratum ----
## If biotic hit (e.g., non-vascular plant, algae, basal plant) precedes final hit, take biotic hit
ground_cover = cover_data_long %>% 
  group_by(site_visit_id, line, point) %>% 
  mutate(strata = as.numeric(strata),
         maxLayer = max(strata)) %>% 
  ungroup() %>% 
  filter(strata == maxLayer | (strata == maxLayer - 1 & ground_element_code %in% abiotic_list$ground_element_code)) %>% 
  group_by(site_visit_id, line, point) %>%
  mutate(newLayer = 1:n(),
         minLayer = min(newLayer)) %>%  
  filter(newLayer == minLayer) %>% 
  ungroup()

# Ensure that all ground element codes match a code in the database dictionary
which(!(ground_cover$ground_element_code %in% abiotic_list$ground_element_code))

# Calculate ground cover percent ----
# Each ground_element can appear a maximum of 120 times per plot
ground_cover_percent = ground_cover %>% 
  left_join(abiotic_list, by="ground_element_code") %>%
  group_by(site_visit_id, ground_element) %>% 
  mutate(hits = 1) %>% 
  summarize(total_hits = sum(hits)) %>% # Sum all hits per site
  mutate(ground_cover_percent = round(total_hits/120*100, digits = 3)) %>% # Convert to percent
  ungroup() %>%
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template)) %>% 
  bind_rows(extra_site) # Add site with no line-point intercept data

# Ensure that all sites are included in the ground cover data
ground_cover_percent %>% distinct(site_visit_code) %>% nrow() == nrow(site_visit_original)

# Add ground abiotic elements with 0% cover ----
for (i in 1:length(unique(ground_cover_percent$site_visit_code))) {
  site_code = unique(ground_cover_percent$site_visit_code)[i]
  ground_cover_subset = ground_cover_percent %>% filter(site_visit_code == site_code)
  
  # Determine which abiotic elements are not listed at that site
  missing_elements = abiotic_list %>% 
    filter(!(ground_element %in% ground_cover_subset$ground_element)) %>% 
    select(ground_element) %>% 
    distinct()
  
  # Append missing elements to existing abiotic top cover data
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
  filter(count != nrow(abiotic_list)) # 13 entries for each site (total number of ground elements)

ground_cover_final %>% 
  group_by(ground_element) %>% 
  summarize(count = n()) %>% 
  filter(count != nrow(site_visit_original)) # 22 entries for each ground elements (total number of sites)

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
