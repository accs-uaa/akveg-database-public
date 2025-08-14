# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover for ABR 2019 Arctic Refuge data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-25
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Vegetation Cover for ABR 2019 Arctic Refuge data" uses data from vegetation surveys to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data', 'Data_Plots', '29_abr_arcticrefuge_2019')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, 'Data', "Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define inputs
cover_aerial_input = path(source_folder, "abr_anwr_ns_lc_veg_aerial_cover_deliverable_view.csv")
cover_ground_input = path(source_folder, "abr_anwr_ns_lc_veg_cover_deliverable.csv")
site_input = path(plot_folder, '02_site_abrarcticrefuge2019.csv')
site_visit_input = path(plot_folder, "03_sitevisit_abrarcticrefuge2019.csv")
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output
veg_cover_output = path(plot_folder, "05_vegetationcover_abrarcticrefuge2019.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
aerial_original = read_csv(file=cover_aerial_input, col_select = c('plot_id', 'veg_taxonomy', 'cover_percent'))
ground_original = read_csv(file=cover_ground_input)
site_original = read_csv(site_input, col_select = c('site_code', 'perspective'))
site_visit_original = read_csv(site_visit_input, col_select = c('site_code', 'site_visit_code'))
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

# Format site data ----
# Add information on ground versus aerial plots
site_visit = site_visit_original %>% 
  mutate(original_code = str_c(site_code,"2019",sep="_")) %>% 
  left_join(site_original, by = 'site_code')

# Merge aerial & ground cover data ----
ground_cover = ground_original %>% 
  mutate(veg_taxonomy = if_else(is.na(taxonomy_note), veg_taxonomy, taxonomy_note)) %>% 
  select(plot_id,veg_taxonomy,cover_percent)

veg_cover_data = rbind(aerial_original, ground_cover)

# Obtain site visit code ----
# Use right join to drop excluded plots
veg_cover_data = right_join(veg_cover_data, 
                            site_visit, by=c("plot_id"="original_code"))

# Explore cover data ----
# Ensure all entries have a site visit code
veg_cover_data %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
veg_cover_data %>% filter(is.na(cover_percent))

# Ensure that % cover is between 0 and 100
summary(veg_cover_data$cover_percent)

# Obtain accepted taxonomic name ----

# Standardize spelling conventions to match AKVEG checklist
veg_cover_taxa = veg_cover_data %>%
  rename(name_original = veg_taxonomy) %>% 
  mutate(name_original = str_remove(name_original, "\\bsp.")) %>%  # Remove 'sp.'
  mutate(name_original = str_remove_all(name_original, pattern="\\d")) %>% 
  mutate(name_original = str_trim(name_original, side = "right")) %>% 
  mutate(name_original = case_when(name_original == 'Polemonium caeruleum ssp. Villosum' ~ 'Polemonium caeruleum ssp. villosum',
                                   name_original == 'Silene uralensis ssp. Uralensis' ~ 'Silene uralensis ssp. uralensis',
                                   .default = name_original))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all, by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Alopecurus magellanicus" ~ "Alopecurus",
                                      name_original == 'Cryptobiotic Crust' ~ 'biotic soil crust',
                                      name_original == "Unidentified algae" ~ "algae",
                                      name_original == "Unknown grass" ~ "grass (Poaceae)",
                                      grepl("Unspecified moss, non-Sphagnum|Unknown moss", name_original) ~ "moss",
                                      name_original == 'Dwarf Ericaceous Shrub' ~ 'shrub dwarf',
                                      grepl('Sphagnum \\(', name_original) ~ 'Sphagnum',
                                      grepl('crustose lichen', name_original) ~ 'crustose lichen',
                                      name_original == 'Unknown foliose lichen, usnic acid/light yellow' ~ 'foliose lichen',
                                      name_original == 'Unknown foliose/fruticose lichen' ~ 'lichen',
                                      name_original == 'Unknown fruticose lichen, usnic acid/light yellow' ~ 'fruticose lichen',
                                      name_original == 'Unknown orchid' ~ 'forb',
                                      name_original == 'Unknown sedge' ~ 'sedge (Cyperaceae)',
                                      name_original == 'Unknown shrub' ~ 'shrub',
                                      name_original == 'Hylocomium endens' ~ 'Hylocomium splendens',
                                      name_original == 'Luzula cata' ~ 'Luzula',
                                      name_original == 'Luzula wahlenbergii ssp. wahlenbergii' ~ 'Luzula wahlenbergii',
                                      name_original == 'Melandrium apetalum' ~ 'Silene uralensis',
                                      name_original == 'Minuartia' ~ 'forb',
                                      name_original == 'Pedicularis sudetica' ~ 'Pedicularis',
                                      name_original == 'Trisetum catum' ~ 'Trisetum spicatum',
                                      name_original == 'Dryas octopetala' ~ 'Dryas',
                                      .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Summarize percent cover ---
# In some cases, species were broken up by height class. Combine into a single cover value.
veg_cover_summary = veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated, perspective) %>%
  summarize(cover_percent = sum(cover_percent))

# Populate remaining columns ----
veg_cover_final = veg_cover_summary %>%
  mutate(cover_type = case_when(perspective == 'aerial' ~ "top foliar cover",
                                perspective == 'ground' ~ 'absolute foliar cover'),
         dead_status = "FALSE",
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
table(veg_cover_final$dead_status) # Should be all FALSE

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_code)) == length(unique(veg_cover_final$site_visit_code))

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Explore total cover percent by site
# Use veg_cover_summary to keep data on perspective
temp = veg_cover_summary %>% 
  group_by(site_visit_code, perspective) %>% 
  summarize(total_cover = sum(cover_percent)) %>% 
  arrange(-total_cover)

# Ensure that cover for aerial sites does not exceed 100%
temp %>% filter(perspective == 'aerial') %>% 
  ungroup() %>% 
  summarize(max_cover = max(total_cover))

# Export data ----
write_csv(veg_cover_final, veg_cover_output)
