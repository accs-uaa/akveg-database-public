# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for NPS Arctic Network 2014 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-09
# Usage: Must be executed in R version 4.4.3+.
# Description: "Calculate Vegetation Cover for NPS Arctic Network 2014 data" uses summarized data from line-point intercept surveys to calculate site-level percent foliar cover for each recorded species. The script reads in CSV tables exported from the NPS ARCN SQL database, appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
veg_cover_summary_input = path(source_folder, 'dbo_plantcoversummary.csv')
veg_cover_raw_input = path(source_folder, 'dbo_pointplant.csv')
metadata_input = path(source_folder,"dbo_metadatachoices.csv")
site_visit_input = path(plot_folder, "03_sitevisit_npsarcn2014.csv")
site_lookup_input = path(workspace_folder, "lookup_site_codes.csv")
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, "05_vegetationcover_npsarcn2014.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
veg_cover_original = read_csv(veg_cover_summary_input)
veg_points_original = read_csv(veg_cover_raw_input)
site_visit_original = read_csv(site_visit_input)
metadata_original = read_csv(metadata_input)
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
site_lookup = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(site_lookup_original, by="site_code") %>% 
  select(-site_code)

veg_cover = veg_cover_original %>% 
  left_join(site_lookup, by=c('Node', 'Plot')) %>% # Do not include 3 sites w/o veg cover
  select(site_visit_code, Plant, CoverPct)

# Ensure all entries have a site visit code
veg_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_original$site_visit_code %in% unique(veg_cover$site_visit_code)) # 3 sites only have ground cover data
which(!unique(veg_cover$site_visit_code) %in% site_visit_original$site_visit_code)

# Append unknown codes ----
# 'Plant' names that start with '2' refer to unknown codes or functional groups
# Ensure formatting and spelling matches with AKVEG database
metadata = metadata_original %>% 
  filter(grepl(pattern="2",x=Choice) & LookupTable=="Plants") %>% 
  mutate(name_original = str_to_lower(ChoiceDescription),
         name_original = str_remove_all(name_original, pattern = ","),
         name_original = case_when(name_original == "Alga" ~ "algae",
                                   name_original == "tree needleleaf (coniferous)" ~ "tree needleleaf",
                                   Choice == "2LC" ~ "lichen",
                                   Choice == "2LF" ~ "lichen",
                                   Choice == "2LU" ~ "lichen",
                                   Choice == "2FERN" ~ "spore-bearing",
                                   TRUE ~ name_original)) %>% 
  select(Choice, name_original)

# Change codes in cover_data
veg_cover = veg_cover %>% 
  left_join(metadata, by=c('Plant'='Choice')) %>% 
  mutate(name_original = if_else(is.na(name_original),
                                 Plant,name_original)) %>% 
  select(-Plant)

# Obtain accepted taxonomic name ----
# Correct obvious typos
veg_cover_taxa = veg_cover %>% 
  mutate(name_original = case_when(name_original == "Saxifraga hieracifolia" ~ "Saxifraga hieraciifolia",
                                   name_original == "Hierchloe alpina" ~ "Hierochloë alpina",
                                   name_original == "Hedysarum mackenzei" ~ "Hedysarum mackenziei",
                                   name_original == "Atriplex gmelini" ~ "Atriplex gmelinii",
                                   name_original == "Scorpidium scorpoides" ~ "Scorpidium scorpioides",
                                   name_original == "Bromus pumpelliana" ~ "Bromus pumpellianus",
                                   .default = name_original))

# Merge with AKVEG Comprehensive Checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>%
  mutate(name_adjudicated = case_when(name_original == "Cetraria (other dark)" ~ "Cetraria",
                                   name_original == "Cladina arbuscula/mitis" ~ "Cladonia arbuscula ssp. mitis",
                                   name_original == "Cladina rangiferina/stygia" ~ "Cladonia",
                                   name_original == "Cladonia amaurocraea/uncialis" ~ "Cladonia",
                                   name_original == "Cetraria ericetorum/islandica/laevigata" ~ "Cetraria",
                                   name_original == "Thamnolia subuliformis/vermicularis" ~ "Thamnolia",
                                   name_original == "Melanelia stygia/C. comixta/hepat./Allantoparmelia" ~ "crustose lichen (non-orange)",
                                   name_original == "Dryas octopetala" ~ "Dryas",
                                   name_original == "Saxifraga bronchialis" ~ "Saxifraga funstonii",
                                   name_original == "Aulacomnium palustre/acuminatum" ~ "Aulacomnium",
                                   name_original == "Poaceae" ~ "grass (Poaceae)",
                                   name_original == "Xanthoria" ~ "crustose lichen (orange)",
                                   name_original == "Asteraceae" ~ "forb",
                                   name_original == "Minuartia" ~ "forb",
                                   name_original == "Collema/Leptogium" ~ "crustose lichen (non-orange)",
                                   TRUE ~ name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Summarize percent cover ----

# Use raw data to determine total number of points per site
points_per_site = veg_points_original %>% 
  left_join(site_lookup, by=c('Node', 'Plot')) %>% 
  distinct(site_visit_code, Point) %>% 
  group_by (site_visit_code) %>% 
  summarise(total_points = n())

# Calculate percentage (sum of hits adjusted for total number of points)
veg_cover_summary = veg_cover_taxa %>%
  group_by(site_visit_code, name_original, name_adjudicated) %>%
  summarise(sum_hits = sum(CoverPct)) %>% 
  left_join(points_per_site, by="site_visit_code") %>% 
  mutate(cover_percent = sum_hits / total_points * 100)

# Populate remaining columns ----
veg_cover_final = veg_cover_summary %>%
  mutate(cover_type = "absolute foliar cover",
         dead_status = "FALSE", # Assume FALSE
         cover_percent = signif(cover_percent, digits = 3)) %>% # Round to 3 decimal places
  select(all_of(template)) %>% 
  arrange(site_visit_code)

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are values for dead status boolean?
table(veg_cover_final$dead_status)

# Are the correct number of sites included? (Account for 3 sites with only abiotic cover)
length(unique(site_visit_original$site_visit_code)) - 3 == length(unique(veg_cover_final$site_visit_code))

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_final, veg_cover_output)

# Clean workspace ----
rm(list=ls())
