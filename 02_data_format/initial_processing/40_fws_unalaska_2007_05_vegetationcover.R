# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for USFWS Unalaska 2007 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for USFWS Unalaska 2007 data" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species by converting Braun-Blanquet classes to a numerical estimate of cover. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/40_fws_unalaska_2007'
source_folder = path(plot_folder, 'source', 'modified_source_data')
template_folder = path(project_folder, "Data/Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
veg_cover_input = path(source_folder, 'aava_unalaska_stalbot_2010_spp_modsrc.xlsx')
site_input = path(source_folder, 'aava_unalaska_stalbot_2010_allenv_modrc.xlsx')
site_visit_input = path(plot_folder, paste0("03_site_visit_", 'fwsunalaska2007', ".csv"))
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, paste0("05_vegetation_cover_", 'fwsunalaska2007', ".csv"))

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----
veg_cover_original = read_xlsx(veg_cover_input, skip = 7)
site_original = read_xlsx(site_input, skip = 7)
site_visit_original = read_csv(site_visit_input)
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

# Specify column names ----
veg_cover_data = veg_cover_original

# Column names of first and second columns are listed in first row of dataset
colnames(veg_cover_data) = c(as.character(veg_cover_data[1,1]), as.character(veg_cover_data[1,2]),"scientific_name",colnames(veg_cover_data[4:73]))
veg_cover_data = veg_cover_data[-1,-c(1:2)] # Drop first row, which is all NA except for the now-transferred column names. Drop first two columns (sci name cross-referenced to PASL)

# Convert to long format ----
veg_cover_long = veg_cover_data %>%
  pivot_longer(`1`:`70`, 
               names_to="plot_number", values_to="braun_blanquet_class") %>% 
  filter(!is.na(braun_blanquet_class) & braun_blanquet_class != 0) %>% # Null or zero values means species was not found in the plot  
  mutate(plot_number = as.integer(plot_number)) %>% 
  arrange(plot_number)

# Format site code ----

# Ensure plot numbers range from 1 to 70
hist(veg_cover_long$plot_number)
summary(veg_cover_long$plot_number)

# Join with original site data to obtain other set of site codes used in these scripts
veg_cover_long = site_original %>% 
  select(`FIELD RELEVE NUMBER (PROVIDED BY AUTHOR)`, `PUBLISHED RELEVE NUMBER`) %>% 
  left_join(veg_cover_long, join_by(`PUBLISHED RELEVE NUMBER` == plot_number)) %>% 
  mutate(site_code = str_c("FWS", `FIELD RELEVE NUMBER (PROVIDED BY AUTHOR)`, 
                           sep = "_"), .keep="unused") %>% 
  select(-`PUBLISHED RELEVE NUMBER`)

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

veg_cover_long = veg_cover_long %>%
  right_join(site_visit_data, by ="site_code") # Use right join to drop any excluded plots

# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover_long %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_data$site_visit_code %in% unique(veg_cover_long$site_visit_code))

# Obtain accepted taxonomic name ----

# Standardize spelling conventions to match AKVEG checklist
veg_cover_taxa = veg_cover_long %>%
  rename(name_original = scientific_name) %>% 
  mutate(name_original = str_remove(name_original, " species"), # Remove 'species' suffixed to the end of genera names
         name_original = str_replace(name_original, " v\\. ", " var\\. "),
         name_original = str_replace(name_original, " s\\. ", " ssp\\. "))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Aconitum delphinifolium" ~ "Aconitum delphiniifolium",
                                      name_original == "Hierochloe hierta" ~ "Hierochloë odorata",
                                      name_original == "Luzula multiflora ssp. multiflora var. kobayasii" ~ "Luzula multiflora ssp. kobayasii",
                                      name_original == "Moerkia blytii" ~ "Moerckia blyttii",
                                      name_original == "Pertusaria papillata" ~ "Pertusaria", # Not found in AKVEG Checklist
                                      name_original == "Petasitus frigidus var. nivalis" ~ "Petasites frigidus var. frigidus",
                                      name_original == "Peltigera concinna" ~ "Peltigera leucophlebia", # Per PASL cross-reference
                                      name_original == "Peltigera britanica" ~ "Peltigera britannica",
                                      .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Does the dataset needs to be summarized i.e., are there any species originally identified as separate species that now need to be merged into a single one?
veg_cover_taxa %>% 
  group_by(site_visit_code, name_adjudicated) %>% 
  nrow == veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated) %>%  
  nrow() # If TRUE, no need to summarize

# Convert Braun-Blanquet classes ----
# Use Table 2 on page 309  of Westhoff and van der Maarel 1978: Use the midpoint when a range of values is provided

veg_cover_taxa$braun_blanquet_class = as.factor(veg_cover_taxa$braun_blanquet_class)
unique(veg_cover_taxa$braun_blanquet_class)

veg_cover_percent = veg_cover_taxa %>% 
  mutate(cover_percent = case_when(braun_blanquet_class == 1 ~ 0,
                                   braun_blanquet_class == 2 ~ 1,
                                   braun_blanquet_class == 3 ~ 2.5,    
                                   braun_blanquet_class == 4 ~ 4,
                                   braun_blanquet_class == 5 ~ (5+10)/2,
                                   braun_blanquet_class == 6 ~ (11+25)/2,
                                   braun_blanquet_class == 7 ~ (26+50)/2,
                                   braun_blanquet_class == 8 ~ (51+75)/2,
                                   braun_blanquet_class == 9 ~ (76+100)/2))

# Ensure values were converted properly
table(veg_cover_percent$braun_blanquet_class, 
      veg_cover_percent$cover_percent)

# Populate remaining columns ----
veg_cover_summary = veg_cover_percent %>%
  mutate(cover_type = "absolute canopy cover",
         dead_status = "FALSE",
         cover_percent = signif(cover_percent, digits = 6)) %>% # Round to 3 decimal places
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

# Export data ----
write_csv(veg_cover_summary, veg_cover_output)

# Clean workspace ----
rm(list=ls())
