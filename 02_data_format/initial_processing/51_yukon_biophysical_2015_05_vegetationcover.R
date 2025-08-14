# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for Yukon Biophysical Plots data"
# Author: Amanda Droghini, Matt Macander
# Last Updated: 2024-11-12
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for Yukon Biophysical Plots data" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species. The script appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, applies corrections to percent foliar cover values, performs QA/QC checks, and enforces formatting to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(janitor)
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
plot_folder = path(project_folder, 'Data/Data_Plots/51_yukon_biophysical_2015')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source/yukon_pft_2021')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input dataset
site_visit_input = path(plot_folder, "03_sitevisit_yukonbiophysical2015.csv")
veg_cover_input = path(source_folder, 'raw_data_yukon_biophysical', 'YukonPlotsSince2000.xlsx')
plant_codes_input = path(source_folder, 'ref','veg_ref_and_crosswalks_2020.xlsx')
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_yukonbiophysical2015.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----

# Specify column types to avoid ~funky~ results
veg_cover_original = read_xlsx(veg_cover_input,
                               sheet = 'Veg',
                               col_types = c("text","text","guess",
                                             rep("numeric",26),
                                             rep("guess",15)))

template = colnames(read_xlsx(path=template_input))
site_visit_original = read_csv(site_visit_input)
plant_codes_original = read_xlsx(plant_codes_input, sheet='ref_dataset_veg_synonymy')

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

# Format ref taxonomy data
yukon_plant_codes = plant_codes_original %>% 
  filter(dataset_name=="Yukon Biophysical Survey") %>%
  mutate(dataset_taxonomic_code = str_to_lower(dataset_taxonomic_code),
         nalc_code = str_to_lower(nalc_code),
         title = str_replace_all(title, " sp.", ""), # Remove 'sp.' suffix
         title = str_replace_all(title, " ssp\\.", " ssp"), # Coerce subspecies name to start with a minuscule letter; sentence case doesn't work when a period is present
         title = str_to_sentence(title),
         title = str_replace_all(title, " ssp", " ssp\\.")) %>%
  select(dataset_taxonomic_code, nalc_code, title)

# Explore cover data ----
# Replace NA values with 0
veg_cover = veg_cover_original %>%
  replace_na(list(Cover2 = 0,
                  Cover3 = 0,
                  Cover4 = 0,
                  Cover5 = 0,
                  Cover5a = 0,
                  Cover5b = 0,
                  Cover5c = 0,
                  CoverB = 0,
                  Cover6 = 0,
                  Cover7 = 0))

# Ensure 'totals' columns are exact summations before dropping them
# TotalA: sum of tree layers (cover 1-3)
veg_cover %>%
  filter(abs(TotalA - (Cover1 + Cover2 + Cover3)) > 0) %>% 
  nrow()

# TotalB: sum of shrub layers (cover 4-5)
temp = veg_cover %>%
  filter(abs(TotalB - (Cover4 + Cover5 + Cover5a + Cover5b + Cover5c)) > 0.1) %>% 
  remove_empty(which = c("rows", "cols")) # Some minute differences (<0.1), 7 rows w/ larger errors. Assume that the 'Cover' columns are more reliable than the 'Total' columns
rm(temp)

# Format cover data ----

# Remove empty/unnecessary rows and columns
veg_cover = veg_cover %>%
  remove_empty(which = c("rows", "cols")) %>% 
  select(-c(Flag, ID, TotalA, TotalB))

# Convert species code to lower case
veg_cover = veg_cover %>%
  mutate(Species = str_to_lower(Species)) 

# Convert to long format
veg_cover_long = veg_cover %>%
  pivot_longer(Cover2:Cover7, names_to="vertical_strata", values_to="cover")

# Drop entries with 0% cover
veg_cover_long = veg_cover_long %>% 
  filter(cover != 0)

# Obtain accepted taxonomic name ----

# Join with reference dataset to obtain 'original' taxonomic name
veg_cover_long = veg_cover_long %>%
  left_join(yukon_plant_codes,by=c("Species" = "dataset_taxonomic_code")) %>% 
  rename(name_original = title)

# Correct entries that did not match with an existing code (two unmatched entries are abiotic elements)
# I was unable to resolve these to species, so I only added an unknown code if all of the possible matches pointed to the same genus/functional group
# Remaining unknown codes are dropped
veg_cover_long = veg_cover_long %>%
  mutate(name_original = case_when(Species == "careatr" ~ "Carex",
                                   Species == "leciatr" ~ "lichen",
                                   Species == "caretro" ~ "Carex",
                                   name_original == "Arctous ruber" ~ "Arctous rubra", # Correct typo
                                   name_original == "Hylocomiumendens" ~ "Hylocomium splendens", # Correct typo
                                   name_original == "Luzulacata" ~ "Luzula",
                                   name_original == "Deschampsia caespitosa" ~ "Deschampsia cespitosa",
                                   name_original == "Poaceae" ~ "grass (Poaceae)",
                                   name_original == "Salix arctica (formerly s. tortulosa)" ~ "Salix arctica",
                                   name_original == "Trisetumcatum" ~ "Trisetum spicatum",
                                   grepl("Unknown",name_original) ~ str_replace_all(name_original,"Unknown ", ""),
                                   grepl("Uknown",name_original) ~ str_replace_all(name_original,"Uknown ", ""),
                                   .default = name_original)) %>% 
  filter(!is.na(name_original) & Species != "unknown") %>% 
  filter(name_original != "Russula fungi") # Drop fungus

# Are there other entries that did not return a match?
veg_cover_long %>% 
  filter(is.na(name_original)) %>% 
  distinct(Species)

# Join with AKVEG comprehensive checklist
veg_cover_long = veg_cover_long %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_long = veg_cover_long %>% 
  mutate(name_adjudicated = case_when(name_original == "Alopecurus magellanicus" ~ "Alopecurus",
                                      name_original == "Artemisia norvegica" ~ "Artemisia arctica",
                                      name_original == "Asteraceae" ~ "forb",
                                      name_original == "Bistorta bistortoides" ~ "Bistorta",
                                      name_original == "Boykinia occidentalis" ~ "Boykinia",
                                      name_original == "Carex ×flavicans" ~ "Carex",
                                      name_original == "Carex microptera" ~ "Carex",
                                      name_original == "Draba alpina" ~ "Draba",
                                      name_original == "Dryas octopetala" ~ "Dryas",
                                      name_original == "grass" ~ "grass (Poaceae)",
                                      name_original == "herb" ~ "forb",
                                      name_original == "Leymus angustus" ~ "grass (Poaceae)",
                                      name_original == "Minuartia" ~ "forb",
                                      name_original == "Parnassia glauca" ~ "Parnassia",
                                      name_original == "Parnassia parviflora" ~ "Parnassia",
                                      name_original == "Pedicularis sudetica" ~ "Pedicularis",
                                      name_original == "Phlox richardsonii var. alaskensis" ~ "Phlox alaskensis",
                                      name_original == "Poa cusickii" ~ "Poa",
                                      name_original == "Potentilla egedii" ~ "Potentilla",
                                      name_original == "Potentilla uniflora" ~ "Potentilla vulcanicola",
                                      
                                      name_original == "Primula stricta" ~ "Primula egaliksensis",
                                   name_original == "Salix brachycarpa" ~ "Salix niphoclada",
                                   name_original == "Salix farriae" ~ "Salix hastata",
                                   name_original == "Saxifraga bronchialis" ~ "Saxifraga funstonii",
                                   
                                   name_original == "Saxifraga flagellaris" ~ "Saxifraga setigera",  
                                   name_original == "Saxifraga paniculata" ~ "forb",
                                   name_original == "Vaccinium microcarpum" ~ "Oxycoccus microcarpus",
                                   name_original == "Valeriana dioica" ~ "Valeriana",
                                   name_original == "Xanthoria" ~ "lichen",
                                   .default = name_adjudicated))

# Are there any other entries that did not return a match?
veg_cover_long %>% 
       filter(is.na(name_adjudicated)) %>% 
       distinct(name_original)

# Summarize percent foliar cover ----
# Sum percent foliar cover across all strata for each species and plot

# There are a few entries that have different name original, but the same name adjudicated
# Change the name original so that the summary step can retain name original
veg_cover_summary = veg_cover_long %>% 
  mutate(name_original = case_when(PlotNumber == 'GSW2203' & name_original == 'Carex lacustris' ~ 'Kobresia simpliciuscula',
                                   PlotNumber == 'YK02126' & name_original == 'Cladina' ~ 'Cladonia',
                                   PlotNumber == 'YK03391' & name_original == 'Kobresia' ~ 'Carex',
                                   PlotNumber == 'YK11136' & name_original == 'Xanthoria' ~ 'lichen',
                                   .default = name_original
                                   )) %>% 
  group_by(PlotNumber, name_original, name_adjudicated) %>% 
  summarize(cover_percent = sum(cover)) %>% 
  ungroup()

# Round entries with cover <0.1% to 0% (minimum value in AKVEG database)
veg_cover_summary = veg_cover_summary %>% 
  mutate(cover_percent = case_when(cover_percent <0.1 ~ 0,
                                   .default = cover_percent))

veg_cover_summary %>% filter(cover_percent <0.1) %>% distinct(cover_percent)

# Drop plots with unknown cover or incorrect coordinates ----
# Use site visit table to also obtain site visit codes
site_visit = site_visit_original %>% 
  select(site_code, site_visit_code)

veg_cover_summary = veg_cover_summary %>% 
  right_join(site_visit, join_by("PlotNumber" == "site_code")) %>% 
  arrange(site_visit_code, name_adjudicated)

# Populate remaining columns ----
veg_cover_summary = veg_cover_summary %>%
  mutate(cover_type = "absolute foliar cover",
         dead_status = "FALSE") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_summary, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_summary$cover_percent)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_code)) == length(unique(veg_cover_summary$site_visit_code))

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_summary %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_summary) # If TRUE, no duplicates to address

veg_cover_summary %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_summary) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_summary, veg_cover_output)

# Clear workspace ----
rm(list=ls())
