# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-03
# Usage: Must be executed in R version 4.4.0+.
# Description: "Calculate Vegetation Cover for ACCS Nelchina 2023 data" uses data from line-point intercept surveys to calculate site-level percent foliar cover for each recorded species. The script also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, applies corrections to percent foliar cover values based on field notes, performs QA/QC checks to ensure values match constrained fields and are within a reasonable range, and enforces formatting to match the AKVEG template.
# ---------------------------------------------------------------------------

rm(list=ls())

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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/36_accs_nelchina_2023')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define inputs
input_trace = path(source_folder, "05_accs_nelchina_diversity_2023.xlsx")
input_lpi = path(source_folder, "05_accs_nelchina_lpi_2023.xlsx")
input_elements = path(source_folder,"abiotic_ground_elements.xlsx")
input_template = path(template_folder, "05_Vegetation_Cover.xlsx")
input_site_visit = path(plot_folder, "03_sitevisit_accsnelchina2023.csv")


# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Define outputs
output_veg_cover = path(plot_folder, "05_vegetationcover_accsnelchina2023.csv")

# Read in data ----
veg_trace_original = read_xlsx(path=input_trace)
veg_lpi_original = read_xlsx(path=input_lpi, col_types=c("text","numeric","numeric",
                                                 "text","text","text",
                                                 "text","text","text",
                                                 "text","text"))
element_codes = read_xlsx (path=input_elements)
template = colnames(read_xlsx(path=input_template))
site_visit_original = read_csv(input_site_visit, 
                      col_select=c(site_code, site_visit_code))

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

# Format elements code list ----
element_codes = element_codes %>% 
  mutate(code = str_to_lower(code)) %>% 
  filter(vegetation_cover == FALSE) # Restrict to codes that are excluded from vegetation cover

# Format LPI cover data ----
veg_lpi = veg_lpi_original %>%
  left_join(site_visit_original,by="site_code") %>% # Append site visit code
  pivot_longer(cols = layer_1:layer_8, # Convert to long format
             names_to = "strata",
             names_prefix = "layer_",
             values_to = "code",
             values_drop_na = TRUE) %>% 
  mutate(dead_status = if_else(grepl(pattern="-D",x=code), # Create dead status
                               "TRUE",
                               "FALSE"),
         code = str_replace_all(code, pattern=c("-D"="", "-B"="")), # Remove dead status + basal hits designation
         code = str_to_lower(code)) %>% # Convert to lowercase
  rename(name_original = code) %>% 
  filter(!(name_original %in% element_codes$code)) # Exclude non-veg codes

veg_lpi %>% filter(is.na(site_visit_code))

# Format trace cover data ----
veg_trace = veg_trace_original %>% 
  mutate(name_original = str_to_lower(name_original)) %>% 
  left_join(site_visit_original,by="site_code") %>% # Append site visit code
  select(all_of(template)) # Keep only required columns

veg_trace %>% filter(is.na(site_visit_code))

# Correct species code ----
# NLC_415: CARMIN should be CARMICC
veg_lpi = veg_lpi %>% 
  mutate(name_original = case_when(name_original == "carmin" ~ "carmicc",
                                   .default = name_original))

# NLC_358: porflaa code changed to porfla3
veg_lpi = veg_lpi %>% 
  mutate(name_original = case_when(name_original == "porflaa" ~ "porfla3",
                                   .default = name_original))

# Calculate cover percent ----
# Group by line & point to make sure species don't get counted twice per point
# Then group by plot
# Each species can appear a maximum of 120 times per plot
veg_cover = veg_lpi %>% 
  group_by(site_visit_code, line, point, name_original, dead_status) %>% 
  summarize(hits = 1) %>% 
  group_by(site_visit_code, name_original, dead_status) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(cover_percent = round(total_hits/120*100, digits = 3),
         cover_type = "absolute foliar cover") %>% 
  select(-total_hits) %>% 
  ungroup()

veg_cover$dead_status = as.logical(veg_cover$dead_status)

# Append trace species data ----

# QA/QC - Look for duplicates
# Create unique site-taxon-dead status combination on the LPI and the trace datasets
veg_cover = veg_cover %>% 
  mutate(site_taxon_id = paste(site_visit_code,name_original,dead_status,sep="_"))

veg_trace = veg_trace %>% 
  mutate(site_taxon_id = paste(site_visit_code,name_original,dead_status,sep="_"))

# Are there any duplicated entries within the trace data itself?
# These are worth checking manually to see if that is a field error or a data entry error, and if the cover percentages are the same
which(duplicated(veg_trace$site_taxon_id))

# Are there any duplicated entries between the LPI and the trace data?
# These can usually be removed from the trace data since the observer likely forgot they had already hit that species
veg_trace = veg_trace %>% mutate(duplicated = site_taxon_id %in% veg_cover$site_taxon_id) %>% 
  filter(duplicated==FALSE) %>% 
  select(-c(duplicated))

# Append trace data to LPI data
veg_cover = bind_rows(veg_cover, veg_trace)

# Perform final check for duplicated taxa
veg_cover[which(duplicated(veg_cover$site_taxon_id)),]

# Drop unneeded columns
veg_cover = veg_cover %>% 
  arrange(site_visit_code) %>% 
  select(-c(site_taxon_id,name_adjudicated))

# Make cover corrections ----
veg_cover = veg_cover %>%
  mutate(
    cover_percent =
      case_when(
        grepl(pattern = "_396_", x = site_visit_code) &
          name_original == "castet" &
          dead_status == FALSE ~ cover_percent + 5,
        grepl(pattern = "_202_", x = site_visit_code) &
          name_original == "eriang" &
          dead_status == FALSE ~ cover_percent - 7,
        grepl(pattern = "_311_", x = site_visit_code) &
          name_original == "carmem" &
          dead_status == FALSE ~ cover_percent - 2,
        .default = cover_percent
      )) %>%
  add_row(
    site_visit_code = "NLC_202_20230627",
    name_original = "carutr",
    cover_type = "absolute foliar cover",
    dead_status = FALSE,
    cover_percent = 5
  ) %>%
  add_row(
    site_visit_code = "NLC_202_20230627",
    name_original = "carmem",
    cover_type = "absolute foliar cover",
    dead_status = FALSE,
    cover_percent = 2
  ) %>%   
  add_row(
    site_visit_code = "NLC_311_20230714",
    name_original = "carsax",
    cover_type = "absolute foliar cover",
    dead_status = FALSE,
    cover_percent = 2
  )
  
# Add correct taxonomic name ----
veg_cover = veg_cover %>% 
  left_join(taxa_all, by = c("name_original" = "taxon_code"))

# Address unknown species codes, if any
veg_cover %>% filter(is.na(taxon_name_accepted))

# Select final columns ----
veg_cover = veg_cover %>%
  select(-name_original) %>% 
  rename(name_original = taxon_name,
         name_adjudicated = taxon_name_accepted) %>%
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover, is.na)
    , sum))

# Are the list of sites the same?
veg_cover$site_visit_code[!(veg_cover$site_visit_code %in% site_visit_original$site_visit_code)]
site_visit_original$site_visit_code[!(site_visit_original$site_visit_code %in% veg_cover$site_visit_code)] 

# Are the values for percent cover between 0 and 100?
summary(veg_cover$cover_percent)

# Export data ----
write_csv(veg_cover,output_veg_cover)

# Clean workspace ----
rm(list=ls())