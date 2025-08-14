# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover for ABR Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-11-10
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Vegetation Cover for ABR Various 2022 data" formats vegetation cover data for ingestion into the AKVEG Database. The script appends unique site visit identifiers, corrects taxonomic names using the AKVEG comprehensive checklist, performs QA/QC checks, and enforces formatting to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data', 'Data_Plots', '28_abr_various_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_private_read')

# Define datasets ----

# Define input datasets
veg_cover_input = path(source_folder, 'tnawrocki_deliverable_two_veg_cover.txt')
site_visit_input = path(plot_folder, '03_sitevisit_abrvarious2022.csv')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_abrvarious2022.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_private.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input, col_select=c('site_code',
                                                              'site_visit_id'))
veg_cover_original = read_delim(veg_cover_input, delim = '|', 
                                col_select=c('plot_id', 'veg_taxonomy',
                                             'veg_field_taxonomy_code', 'cover_percent', 'veg_cov_field_note', 'wetland_stratum'))
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing','connect_database_postgresql.R')
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
# Format plot id column to match site code
veg_cover = veg_cover_original %>%
  mutate(observe_year = case_when(str_starts(plot_id, 'SUWA_') ~ str_extract(plot_id, pattern='\\d{4}'),
                                  .default = NA),
         observe_year = case_when(!is.na(observe_year) ~ str_sub(observe_year, start=-2),
                                  .default = NA)) %>% 
  mutate(site_code = case_when(str_ends(plot_id, pattern="_\\d{4}") ~ str_remove(plot_id, pattern="_\\d{4}"),
                               str_ends(plot_id, pattern='-veg') ~ str_remove(plot_id, pattern='-veg'),
                               .default = plot_id)) %>% 
  mutate(site_code = case_when(str_starts(plot_id, 'SUWA_') ~ str_c(site_code,
                                                                    observe_year, 
                                                                    sep = ""),
                               .default = site_code)) %>% 
  right_join(site_visit_original, by='site_code') # Use right join to drop excluded sites


# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover %>% 
  filter(is.na(site_visit_id)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_original$site_visit_id %in% 
        unique(veg_cover$site_visit_id))

# Ensure that % cover is between 0 and 100
summary(veg_cover$cover_percent)

# Obtain accepted taxonomic name ----
# Standardize spelling conventions to match AKVEG checklist
veg_cover_taxa = veg_cover %>%
  mutate(name_original = case_when(grepl(pattern="algae",x=veg_taxonomy) ~ "algae",
                                   grepl(pattern="forb|Crucifer",x=veg_taxonomy) ~ "forb",
                                   grepl(pattern="lichen",x=veg_taxonomy) ~ "lichen",
                                   grepl(pattern="moss",x=veg_taxonomy) ~ "moss",
                                   grepl(pattern="grass",x=veg_taxonomy) ~ "grass (Poaceae)",
                                   grepl(pattern="graminoid",x=veg_taxonomy) ~ "graminoid",
                                   grepl(pattern="liverwort",x=veg_taxonomy) ~ "liverwort",
                                   grepl(pattern=' sp\\. \\d',x=veg_taxonomy) ~ str_remove(veg_taxonomy, ' sp\\. \\d'),
                                   grepl(pattern='Sphagnum \\(', veg_taxonomy) ~ str_split_i(veg_taxonomy, pattern = ' \\(', i=1),
                                   veg_taxonomy == 'Unknown fungus' ~ 'fungus',
                                   .default = str_remove(veg_taxonomy, " sp\\.")))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Alopecurus magellanicus" ~ "Alopecurus borealis",
                                      name_original == 'Androsace chamaejasme ssp. lehmannia' ~ 'Androsace chamaejasme ssp. andersonii',
                                      name_original == 'Arnica alpina' ~ 'Arnica',
                                      name_original == 'Astragalus eucosmus ssp. eucosmus' ~ 'Astragalus eucosmus',
                                      name_original == 'Brachythecium rutabulum' ~ 'Brachythecium',
                                      name_original == 'Brachythecium velutinum' ~ 'Brachythecium',
                                      name_original == 'Carex amblyorhyncha' ~ 'Carex marina ssp. marina',
                                      name_original == 'Cerastium jenisejense' ~ 'Cerastium regelii',
                                      name_original == 'Chrysanthemum' ~ 'Arctanthemum',
                                      name_original == 'Corallorrhiza trifida' ~ 'Corallorhiza trifida',
                                      name_original == 'Draba alpina' ~ 'Draba',
                                      name_original == 'Draba hirta' ~ 'Draba',
                                      name_original == 'Festuca vivipara' ~ 'Festuca viviparoidea',
                                      name_original == 'Gentiana propinqua ssp. propinqua' ~ 'Gentianella propinqua ssp. propinqua',
                                      name_original == 'Hedysarum mackenzii' ~ 'Hedysarum mackenziei',
                                      name_original == 'Lagotis glauca ssp. minor' ~ 'Lagotis glauca',
                                      name_original == 'Luzula wahlenbergii ssp. wahlenbergii' ~ 'Luzula wahlenbergii',
                                      name_original == 'Melandrium apetalum' ~ 'Silene uralensis ssp. arctica',
                                      name_original == 'Minuartia' ~ 'forb',
                                      name_original == 'Myriophyllum spicatum' ~ 'Myriophyllum sibiricum',
                                      name_original == 'Nostoc pruniforme' ~ 'algae',
                                      name_original == 'Pedicularis sudetica' ~ 'Pedicularis',
                                      name_original == 'Populus balsamifera x trichocarpa' ~ 'Populus balsamifera',
                                      name_original == 'Potentilla uniflora' ~ 'Potentilla vulcanicola',
                                      name_original == 'Pyrola secunda ssp. secunda' ~ 'Orthilia secunda',
                                      name_original == 'Ranunculus gmelinii ssp. gmelini' ~ 'Ranunculus gmelinii ssp. gmelinii',
                                      name_original == 'Saxifraga bronchialis' ~ 'Saxifraga funstonii',
                                      name_original == 'Solidago multiradiata var. multiradiata' ~ 'Solidago multiradiata',
                                      name_original == 'Trientalis europaea ssp. europaea' ~ 'Lysimachia europaea',
                                      .default = name_adjudicated))

# Ensure that there are no other entries w/o a name adjudicated (save for abiotic elements)
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Drop abiotic elements
veg_cover_taxa = veg_cover_taxa %>% 
  filter(!is.na(name_adjudicated))

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Summarize percent cover ----
veg_cover_summary = veg_cover_taxa %>%
  group_by(site_visit_id, name_original, name_adjudicated) %>% 
  summarize(cover_percent = sum(cover_percent))

# Populate remaining columns ----
veg_cover_final = veg_cover_summary %>% 
  mutate(cover_type = "absolute foliar cover",
       dead_status = "FALSE")

# Ensure that column names + order matches template, with the exception of site_visit_code (keeping it as site_visit_id to maintain backwards compatibility)
names(veg_cover_final)
template

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_id)) == length(unique(veg_cover_final$site_visit_id))

missing_sites = site_visit_original[which(!(site_visit_original$site_visit_id %in% unique(veg_cover_final$site_visit_id))), 2]

# Missing sites are sites with only abiotic cover - OK to proceed
veg_cover %>%
  filter(site_visit_id %in% missing_sites$site_visit_id) %>% 
  distinct(veg_taxonomy)

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_id, name_original, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

veg_cover_final %>% 
  distinct(site_visit_id, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_final, veg_cover_output)

# Clear workspace ----
rm(list=ls())
