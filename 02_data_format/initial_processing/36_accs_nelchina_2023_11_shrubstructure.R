# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Shrub Structure for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2023-10-27
# Usage: Must be executed in R version 4.3.1+.
# Description: "Format Shrub Structure for ACCS Nelchina 2023 data" appends unique site visit identifier, matches species code to accepted scientific name, and keeps relevant columns to match the formatting of the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(dplyr)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----
drive <- "D:"
project_folder <- file.path(drive,"ACCS_Work/Projects")
db_folder <- file.path(project_folder,"AKVEG_Database", "Data")
template_folder <- file.path(db_folder, "Data_Entry")
data_folder <- file.path(project_folder, "Caribou_Nelchina","Data", "Summer 2023")
output_folder <- file.path(db_folder,"Data_Plots","36_accs_nelchina_2023")

# Define Git repository directory
repository <- file.path(drive,"ACCS_Work/GitHub/akveg-database-public")

# Define inputs ----
input_shrub <- file.path(data_folder, "11_accs_nelchina_shrub_2023.xlsx")
input_template <- file.path(template_folder, "11_Shrub_Structure.xlsx")
input_site_visit <- file.path(output_folder, "03_accs_nelchina_2023.csv")

# Define outputs ----
output_shrub <- file.path(output_folder,"11_accs_nelchina_2023.csv")

# Connect to AKVEG PostgreSQL database ----

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connectDatabasePostGreSQL.R',
                          sep = '/')
source(connection_script)

authentication = file.path(drive,
                           'ACCS_Work/Servers_Websites/Credentials/accs-postgresql/authentication_akveg.csv')
akveg_connection = connect_database_postgresql(authentication)

# Read in data ----
shrub_data <- read_xlsx(path=input_shrub)
template <- colnames(read_xlsx(path=input_template))
site_visit <- read_csv(input_site_visit)

# Read PostgreSQL taxonomy tables
query_all = 'SELECT * FROM taxon_all' # Define query for AKVEG
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_all))

# Format site visit data ----
site_visit <- site_visit %>% 
  select(site_code, site_visit_code)

# Format taxonomy tables ----
taxa_all <- taxa_all %>% 
  select(taxon_code, taxon_name, taxon_accepted_code)

# Format shrub structure data ----
shrub_data <- shrub_data %>% 
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023"),
         taxon_code = str_to_lower(name_original)) %>% 
  select(-c(site_visit_code,name_original)) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  left_join(taxa_all, by="taxon_code") %>% 
  rename(name_original = taxon_name)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(shrub_data, is.na)
    , sum)
)
# Do all species codes match an accepted code?
shrub_data %>% filter(taxon_code != taxon_accepted_code)

# Verify values of categorical entries
shrub_data %>% distinct(shrub_class)
shrub_data %>% distinct(height_type)
shrub_data %>% distinct(cover_type)
shrub_data %>% distinct(shrub_subplot_area_m2)

# Verify range of numeric columns
summary(shrub_data$cover_percent) # One 'no data' value (-999)
summary(shrub_data$mean_diameter_cm) # One 'no data' value (-999)
summary(shrub_data$height_cm)
summary(shrub_data$number_stems) # One 'no data' value (-999)

# Verify total number of sites (n=80)
shrub_data %>% distinct(site_visit_code) %>% nrow()

# Keep only required columns
shrub_data <- shrub_data %>% 
  select(all_of(template))

# Export data ----
write_csv(shrub_data,file=output_shrub)

# Clear workspace
rm(list=ls())