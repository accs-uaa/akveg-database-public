# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Vegetation Cover Data
# Author: Timm Nawrocki and Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-11-01
# Usage: Script must be executed in R 4.1.2+.
# Description: "Format Vegetation Cover Data" calculates percent vegetation cover at subplots, summarizes to the plot level, corrects taxonomic names, and creates additional fields match minimum standards required by the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(tidyverse)
library(readxl)
library(RSQLite)
library(RPostgres)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "35_fia_various_2021")

# Set repository directory
repository <- 'D:/ACCS_Work/GitHub/akveg-database-public'

# Define inputs ----
FIA_database = file.path(project_folder,
                     'source',
                     'Public_AK_SQlite.db')

# Define template for AKVEG table
input_template <- file.path(root_folder, "Data/Data_Entry","05_Vegetation_Cover.xlsx")
input_plants <- file.path(project_folder, "reference", "speciesPlants_Combined_20221021.csv")

# Define outputs ----
output_cover = file.path(project_folder,
                    'temp_files',
                    '05_usfs_fia.csv')

# Read in tabular data ----
template <- colnames(read_xlsx(path=input_template))
plants_codes = read_csv(input_plants)

# Connect to FIA SQLite Database ----
fia_connection = dbConnect(drv = SQLite(), dbname = FIA_database)

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

# Obtain accepted taxonomy ----

# Define queries for AKVEG
query_all = 'SELECT * FROM taxon_all'
query_accepted = 'SELECT * FROM taxon_accepted'

# Read PostgreSQL taxonomy tables into dataframes
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_all))
taxa_accepted = as_tibble(dbGetQuery(akveg_connection, query_accepted))

# Simplify taxonomy tables
taxa_all <- taxa_all %>% 
  select(taxon_code, taxon_name, taxon_accepted_code)

taxa_accepted <- taxa_all %>% 
  filter(taxon_code == taxon_accepted_code) %>% 
  select(-taxon_code) %>% 
  rename(name_adjudicated = taxon_name)

plants_codes <- plants_codes %>% 
  select(codePLANTS,namePLANTS)

# Calculate percent cover ----

# Define query for FIA subplots to obtain species hits
query_vascular = 'SELECT P2VEG_SUBPLOT_SPP.PLOT as plot
    , P2VEG_SUBPLOT_SPP.SUBP as subplot
    , P2VEG_SUBPLOT_SPP.GROWTH_HABIT_CD as strata
    , P2VEG_SUBPLOT_SPP.LAYER as layer
    , P2VEG_SUBPLOT_SPP.COVER_PCT as cover
    , P2VEG_SUBPLOT_SPP.VEG_FLDSPCD as name_original_code
    , PLOT.MEASDAY as day
    , PLOT.MEASMON as month
    , PLOT.MEASYEAR as year
FROM P2VEG_SUBPLOT_SPP
    LEFT JOIN PLOT ON P2VEG_SUBPLOT_SPP.PLOT = PLOT.PLOT
ORDER BY plot;'

# Summarize FIA vascular cover by subplot
vascular_subplot_fia = as_tibble(dbGetQuery(fia_connection, query_vascular)) %>%
  mutate(observe_date = if_else(day < 10,paste(year, '-0', month, '-0', day, sep =''),
                                         paste(year, '-0', month, '-', day, sep = '')),
         date_string = if_else(day < 10,paste0(year, '0', month, '0', day),
                                        paste0(year, '0', month, '', day)),
                  site_code = case_when(nchar(as.integer(plot)) == 1 ~ paste('FIAINT_', '0000', plot, sep = ''),
                                        nchar(as.integer(plot)) == 2 ~ paste('FIAINT_', '000', plot, sep = ''),
                                        nchar(as.integer(plot)) == 3 ~ paste('FIAINT_', '00', plot, sep = ''),
                                        nchar(as.integer(plot)) == 4 ~ paste('FIAINT_', '0', plot, sep = ''),
                                        nchar(as.integer(plot)) == 5 ~ paste('FIAINT_', plot, sep = ''),
                                        TRUE ~ 'none'),
                  site_visit_id = paste(site_code,date_string, sep = "_")) %>%
  group_by(site_code,site_visit_id,subplot, name_original_code) %>%
  summarize(max_cover = sum(cover), min_cover = max(cover)) %>%
  mutate(cover = (max_cover + min_cover)/2)

# Ensure that no site codes are labeled as 'none'
vascular_subplot_fia %>% filter(site_code == "none")

# Summarize FIA cover by plot and add metadata
vascular_plot_fia = vascular_subplot_fia %>%
  group_by(site_visit_id, name_original_code) %>%
  summarize(cover_percent = mean(cover)) %>%
  left_join(plants_codes, by = c('name_original_code' = 'codePLANTS')) %>%
  rename(name_original = namePLANTS) %>%
  left_join(taxa_all, by=c('name_original'='taxon_name')) %>%
  left_join(taxa_accepted, by='taxon_accepted_code') %>% 
  mutate(cover_type = 'total cover',
         dead_status = FALSE)

# Address unknown taxonomy codes ----
#### ##### #### NEED TO ADDRESS UNKNOWN NAMES CSV BEFORE CONTINUING.
vascular_plot_fia = vascular_plot_fia %>%
  filter(name_original_code != '2GP' &
           name_original_code != '2GL') %>%
  mutate(name_adjudicated = case_when(name_original_code == '2FORB' ~ 'forb',
                                   name_original_code == '2GRAM' ~ 'graminoid',
                                   name_original_code == '2SHRUB' ~ 'shrub',
                                   name_original_code == 'DROC' ~ 'Dryas',
                                   name_original_code == 'LYOB' ~ 'spore-bearing',
                                   name_original_code == 'VAOX' ~ 'Oxycoccus microcarpus',
                                   TRUE ~ name_adjudicated))  %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(vascular_plot_fia, is.na)
    , sum)
)

# Export data ----
write_csv(vascular_plot_fia, file = output_cover)