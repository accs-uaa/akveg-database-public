# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Vegetation Cover table for USFWS Alaska Peninsula data.
# Author: Amanda Droghini
# Last Updated: 2024-11-10
# Usage: Code chunks must be executed sequentially in R 4.4.1+.
# Description: "Format Vegetation Cover table" reads in XLSX tables exported from the USFWS Access database, corrects taxonomic names according to the AKVEG comprehensive checklist, summarizes percent cover, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)

# Define directories ---- 

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 
                      'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '37_fws_alaskapeninsula_2006')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, 'Data', 'Data_Entry')

# Set repository directory
repository_folder = path(drive,root_folder, 'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_private_read')

# Define datasets ----

# Define input datasets
site_visit_input = path(plot_folder, '03_sitevisit_fwsakpen2006.csv')
veg_cover_input = path(source_folder, 'site_species.xlsx')
species_codes_input = path(source_folder, 'species.xlsx')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_fwsakpen2006.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_private.csv')

# Read in data ----
veg_cover_original = read_xlsx(path=veg_cover_input)
site_visit_original = read_csv(site_visit_input)
species_codes_original = read_xlsx(species_codes_input)
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
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
ORDER BY taxon_name_accepted, taxon_name;"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Append site visit code ----
veg_cover = veg_cover_original %>% 
  mutate(site_code = paste(area_name, year, site_number, sep="_"))

site_visit = site_visit_original %>% 
  select(site_code,site_visit_code)

veg_cover = veg_cover %>% 
  right_join(site_visit, by="site_code") # Drop all of the 2009 data for which we don't have coordinates

length(unique(veg_cover$site_code)) == nrow(site_visit)

# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit$site_visit_code %in% unique(veg_cover$site_visit_code))

# Ensure that percent cover is between 0 and 100
summary(veg_cover$percent_cover)

# Obtain accepted taxonomic name ----

# Join species symbol to species name
species_codes = veg_cover %>% 
  distinct(symbol) %>% 
  left_join(species_codes_original, by="symbol") %>% 
  select(symbol,species,common)

# Standardize spelling conventions to match AKVEG checklist: 1) Convert upper case to 'sentence case', 2) Drop 'spp' from genus-only names, 3) Remove extraneous periods e.g., from 'spp.' spelling, 4) Remove whitespaces
species_codes = species_codes %>% 
  mutate(name_original = str_to_sentence(species), 
         name_original = str_replace(name_original, 
                                     pattern = " spp", 
                                     replacement=""),
         name_original = str_remove_all(name_original, 
                                     pattern = "\\."),
         name_original = str_squish(name_original))

# Correct misspellings, unknown codes, and functional groups
# taxon_name column used as join column to retrieve matches in AKVEG
species_codes = species_codes %>%
  mutate(name_original = case_when(name_original == "Carex wet species" ~ "Carex",
                                   name_original == "Lichen cladina" ~ "Cladina",
                                   name_original == "Other shrub" ~ "shrub",
                                   name_original == "Salix dw" ~ "Salix",
                                   name_original == "Salix tree" ~ "Salix",
                                   name_original == "Shrub, unknown" ~ "shrub",
                                   .default = name_original)) %>% 
  mutate(taxon_name = case_when(name_original == "Aconitum delphinifolium" ~ "Aconitum delphiniifolium",
                                name_original == "Algae" ~ "algae",
                                name_original == "Alnus shrub" ~ "Alnus",
                                name_original == "Cicuta mackenziana" ~ "Cicuta mackenzieana",
                                name_original == "Elymus arenarius" ~ "Elymus arenarius ssp. mollis",
                                name_original == "Fern" ~ "fern",
                                name_original == "Forb" ~ "forb",
                                name_original == "Graminoid" ~ "graminoid",
                                name_original == "Grass" ~ "grass (Poaceae)",
                                name_original == "Harrimanella stellerana" ~ "Harrimanella stelleriana",
                                name_original == "Honkenya peploides" ~ "Honckenya peploides",
                                name_original == "Geranium pratense" ~ "Geranium pratense var. erianthum",
                                name_original == "Lichen" ~ "lichen",
                                name_original == "Loiseluria procumbens" ~ "Loiseleuria procumbens",
                                name_original == "Minuartia" ~ "forb",
                                name_original == "Moss" ~ "moss",
                                name_original == "Nuphar polysepalum" ~ "Nuphar polysepala",
                                name_original == "Saxifraga bronchialis" ~ "Saxifraga funstonii",
                                name_original == "Sedum rosea" ~ "Sedum rosea ssp. integrifolium",
                                name_original == "Soil, cryptobiotic" ~ "biotic soil crust",
                                name_original == "Vaccinium microcarpus" ~ "Vaccinium microcarpos",
                                TRUE ~ name_original))                               

# Join with AKVEG comprehensive checklist
species_codes = species_codes %>% 
  left_join(taxa_all, by='taxon_name')

# Ensure that unmatched entries are entries that aren't included in Vegetation Cover e.g., abiotic elements like litter
species_codes %>% 
  filter(is.na(taxon_code)) %>% 
  distinct(taxon_name)

# Add accepted name to veg cover dataframe and drop abiotic elements
veg_cover_taxa = veg_cover %>% 
  left_join(species_codes, by = "symbol") %>% 
  filter(!is.na(taxon_code)) %>%
  rename(name_adjudicated = taxon_name_accepted) %>% 
  select(site_visit_code, species, name_original, name_adjudicated, percent_cover)

# Summarize percent cover ----
# Add percent cover together e.g., in the case where percent cover for a species was separated by height category
veg_cover_summary = veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated) %>% 
  summarize(cover_percent = sum(percent_cover)) %>% 
  mutate(cover_percent = signif(cover_percent, digits = 3))

# Populate remaining columns ----
veg_cover_final = veg_cover_summary %>% 
  mutate(cover_type = "top foliar cover",
         dead_status = FALSE) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are the correct number of sites included?
nrow(site_visit_original) == length(unique(veg_cover_final$site_visit_code))
missing_sites = site_visit_original[which(!(site_visit_original$site_visit_code %in% unique(veg_cover_final$site_visit_code))), 3]

# Missing sites are sites with only abiotic cover - OK to proceed
veg_cover_original %>%
  mutate(site_code = paste(area_name, year, site_number, sep="_")) %>% 
  filter(site_code %in% missing_sites$site_code) %>% 
  distinct(symbol) %>% 
  left_join(species_codes, by ='symbol') %>% 
  select(taxon_name)

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export CSV ----
write_csv(veg_cover_final, veg_cover_output)

# Clean workspace ----
rm(list=ls())
