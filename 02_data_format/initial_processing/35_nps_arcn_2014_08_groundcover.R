# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Ground Cover for NPS Arctic Network 2014 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-07-08
# Usage: Must be executed in R version 4.4.3+.
# Description: "Calculate Ground Cover for NPS Arctic Network 2014 data" uses data from line-point intercept surveys to calculate site-level percent ground cover for each ground element. The script reads in CSV tables exported from the NPS ARCN SQL database, appends unique site visit identifiers, calculates percent cover, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
source_folder = path(plot_folder, 'source')
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data", "Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define inputs ----
ground_cover_input = path(source_folder, 'dbo_pointground.csv')
visit_input = path(plot_folder, "03_sitevisit_npsarcn2014.csv")
site_lookup_input = path(workspace_folder, "lookup_site_codes.csv")
template_input = path(template_folder, "08_ground_cover.xlsx")

# Define output dataset
ground_cover_output = path(plot_folder, "08_groundcover_npsarcn2014.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
ground_cover_original = read_csv(ground_cover_input)
visit_original = read_csv(visit_input)
site_lookup_original = read_csv(site_lookup_input)
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

# Append site visit code ----
site_lookup = visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(site_lookup_original, by="site_code") %>% 
  select(-site_code)

ground_cover = ground_cover_original %>% 
  right_join(site_lookup, by=c('Node', 'Plot')) %>% # Drop 1 site with incomplete cover data
  select(-SampleYear)

# Ensure all entries have a site visit code
ground_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!visit_original$site_visit_code %in% unique(ground_cover$site_visit_code))
which(!unique(ground_cover$site_visit_code) %in% visit_original$site_visit_code)

# Format line & point values -----
ground_cover = ground_cover %>% 
  mutate(line_letter = str_split_i(Point, pattern="\\d", i=1),
         point_number = as.numeric(str_remove(Point, pattern="\\w"))) %>%
  group_by(line_letter) %>%
  mutate(line_number = cur_group_id()) %>%
  ungroup()

# Convert decimals to whole numbers
unique_lines = ground_cover %>%
  group_by(site_visit_code, line_number) %>%
  select(site_visit_code, line_number, point_number) %>%
  mutate(point_whole = 1:n()) %>%
  ungroup()

unique_lines %>%
  group_by(line_number) %>%
  summarize(max_number = max(point_whole),
            count = n())

# Join whole number to ground cover dataframe
ground_cover = ground_cover %>% 
  left_join(unique_lines, by=c("site_visit_code", "line_number", "point_number")) %>% 
  select(site_visit_code, Point, line_number, point_whole, Ground) %>% 
  group_by(site_visit_code)

# Re-classify ground elements ----
unique(ground_cover$Ground)

ground_cover = ground_cover %>% 
  filter(Ground != 'NODATA') %>% 
  mutate(ground_element = str_to_lower(Ground),
         ground_element = case_when(ground_element == 'litter' ~ 'litter (< 2 mm)',
                                    grepl('^rock', ground_element) ~ 'rock fragments',
                                    grepl('gravel', ground_element) ~ 'gravel',
                                    grepl('^basal|^crypt', ground_element) ~ 'biotic',
                                    ground_element == 'wood' ~ 'dead down wood (≥ 2 mm)',
                                    ground_element == 'ash' ~ 'mineral soil',
                                    ground_element == 'soil' ~ 'soil',
                                    .default = ground_element))

# Ensure all values have been correctly reclassified
ground_cover %>% filter(is.na(ground_element))
which(!(unique(ground_cover$ground_element %in% ground_elements$ground_element)))

# Summarize percent cover ---

# Determine maximum number of hits per line
max_hits = unique_lines %>%
  group_by(site_visit_code) %>%
  summarize(count = n()) %>% 
  distinct(count) %>% as.numeric()

ground_cover_final = ground_cover %>%
  group_by(site_visit_code, ground_element) %>%
  mutate(hits = 1) %>% 
  summarize(ground_cover_percent = sum(hits) / max_hits * 100)

# Add a few decimal points to round sites with 99% cover up to 100%
sites_correct = ground_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(total_percent = sum(ground_cover_percent)) %>% 
  filter(round(total_percent) != 100) %>% # 5 sites with 99%
  arrange(total_percent)

ground_cover_final = ground_cover_final %>% 
  group_by(site_visit_code, ground_element) %>% 
  mutate(ground_cover_percent = case_when(site_visit_code %in% sites_correct$site_visit_code ~ ground_cover_percent * (100/99),
                                          .default = ground_cover_percent)) %>% 
  mutate(ground_cover_percent = round(ground_cover_percent, digits=0)) %>% 
  select(all_of(template)) %>% 
  arrange(site_visit_code)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(ground_cover_final, is.na)
    , sum))

# Verify that total cover percent per site equals 100
ground_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(total_percent = sum(ground_cover_percent)) %>% 
  filter(total_percent != 100) %>% # All sites at 100%
  arrange(total_percent)

# Are the correct number of sites included?
length(unique(visit_original$site_visit_code)) == length(unique(ground_cover_final$site_visit_code))

# Export data ----
write_csv(ground_cover_final, ground_cover_output)

# Clean workspace ----
rm(list=ls())
