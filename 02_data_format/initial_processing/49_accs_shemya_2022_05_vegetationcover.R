# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover Table for ACCS Shemya Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-11-21
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Vegetation Cover Table for ACCS Shemya Data" formats vegetation cover data collected and entered by ACCS for ingestion into the AKVEG Database. The script standardizes taxonomic names, ensures values are within reasonable ranges, and adds required metadata fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
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
plot_folder = path(project_folder, 'Data/Data_Plots/49_accs_shemya_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database-public')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define input datasets
veg_cover_input = path(source_folder,'05_Vegetation_Cover_Shemya_2022.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_accsshemya2022.csv')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Define output datasets
veg_cover_output = path(plot_folder, '05_vegetationcover_accsshemya2022.csv')

# Read in data ----
veg_cover_original = read_xlsx(veg_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'pull_functions','connect_database_postgresql.R')
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

# Obtain site visit code ----
veg_cover = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(veg_cover_original, by = c("site_code" = "site_visit_id"))

# Ensure correct number of sites (n=58)
length(unique(veg_cover$site_code))

# Ensure all entries have a site code
veg_cover %>% filter(is.na(veg_cover$site_visit_code)) %>%  nrow()

# Obtain accepted taxonomic name ----

# Drop 'sp.' from taxa that were only identified to the genus level to ensure match with AKVEG checklist
# Ensure all entries are written in sentence case (genus capitalized, species + everything else lower-case)
veg_cover = veg_cover %>% 
  mutate(name_original = case_when(grepl(pattern="(sp\\.)$", 
                                         x = name_original) ~ 
                                     str_remove(name_original, " sp."),
                                   .default = name_original)) %>% 
  mutate(name_original = str_to_sentence(name_original))

# Join with AKVEG comprehensive checklist
veg_cover = veg_cover %>% 
  select(-name_adjudicated) %>% 
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Address codes that did not match
# Drop 'na' entries for now
veg_cover = veg_cover %>% 
  filter(name_original != "Na") %>% 
  mutate(name_adjudicated = case_when(name_original == "Sanionia uncialis" ~ "Sanionia uncinata",
                                      name_original == "Sphagnum pailosum" ~ "Sphagnum papillosum",
                                      .default = name_adjudicated))

# Were there any other names that did not retrieve a match in AKVEG?
veg_cover %>% filter(is.na(name_adjudicated)) %>% distinct(name_original)

# Format dead status ----
table(veg_cover$dead_status)

# Based on distribution of values, I'm assuming that plants coded as '1' (n=659) were alive, while plants coded as '0' (n=5) were dead
veg_cover = veg_cover %>% 
  mutate(dead_status = case_when(dead_status == 1 ~ "FALSE",
                                 dead_status == 0 ~ "TRUE",
                                 .default = "ERROR")) %>% 
  mutate(dead_status = as.logical(dead_status))

# Ensure all values have been properly converted
table(veg_cover$dead_status)

# Format percent cover ----
summary(veg_cover$cover_percent)

# Drop two entries for which % cover is NA
veg_cover = veg_cover %>% 
  filter(!is.na(cover_percent))

# Summarize percent cover so that there is only one entry per site per species
# For each site, group entries with the same taxa/unknown code together and calculate total percent cover (does not change anything for this dataset)
summary_cover = veg_cover %>% 
  group_by(site_visit_code, name_original, name_adjudicated, dead_status)  %>% 
  summarise(cover_percent = sum(cover_percent)) %>% 
  ungroup()

# Populate remaining columns ----
# Restrict to only those columns required by AKVEG
summary_cover = summary_cover %>%
  mutate(cover_type = "absolute foliar cover") %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(summary_cover, is.na)
    , sum)
)

# Ensure cover_percent values are within a reasonable range
summary(summary_cover$cover_percent) # Minimum for this dataset should be 1%, since trace species were not included

# Calculate sum of top cover by site visit
# Abiotic top cover was not recorded, so cannot verify whether sum of biotic + abiotic = 100%
temp = summary_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(sum_cover = sum(cover_percent)) %>% 
  arrange(-sum_cover)

# Ensure that all taxa in name_adjudicated match with a taxon in the database
summary_cover %>% 
  filter(!(name_adjudicated %in% taxa_all$taxon_name_accepted)) %>% 
  distinct(name_original, name_adjudicated)

# Check values of categorical/Boolean variables
table(summary_cover$dead_status)
unique(summary_cover$cover_type) # Should be absolute foliar cover

# Export as CSV ----
write_csv(summary_cover, veg_cover_output)

# Clear workspace ----
rm(list=ls())
