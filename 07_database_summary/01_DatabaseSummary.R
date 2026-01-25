# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Summarize AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-07
# Usage: Script should be executed in R 4.3.2+.
# Description: "Summarize AKVEG Database" summarizes the current status of the AKVEG Database.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(fs)
library(openxlsx)
library(readr)
library(readxl)
library(RPostgres)
library(rvest)
library(sf)
library(stringr)
library(tibble)
library(tidyr)

# Set root directory
drive = 'D:'
root_folder = 'ACCS_Work'

# Define input folders
akveg_repository = path(drive, root_folder, 'Repositories/akveg-database')
credentials_folder = path(drive, root_folder, 'Administrative/Credentials/akveg_private_read')
queries_folder = path(drive, root_folder, 'Repositories/akveg-database/05_queries')

# Define queries
project_file = path(queries_folder, 'standard/Query_01_Project.sql')
site_visit_file = path(queries_folder, 'non_standard/03_SiteVisit_Spatial_All.sql')
vegetation_file = path(queries_folder, 'standard/Query_05_VegetationCover.sql')
environment_file = path(queries_folder, 'standard/Query_12_Environment.sql')

#### QUERY AKVEG DATABASE
####------------------------------

# Import database connection function
connection_script = path(akveg_repository, 'package_DataProcessing', 'connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = path(credentials_folder, 'authentication_akveg_private.csv')
database_connection = connect_database_postgresql(authentication)

# Read project data from AKVEG Database
project_query = read_file(project_file)
project_data = as_tibble(dbGetQuery(database_connection, project_query))

# Read site visit data from AKVEG Database
site_visit_query = read_file(site_visit_file)
site_visit_data = as_tibble(dbGetQuery(database_connection, site_visit_query))

# Read vegetation cover data from AKVEG Database
vegetation_query = read_file(vegetation_file)
vegetation_data = as_tibble(dbGetQuery(database_connection, vegetation_query))

# Read environment data from AKVEG Database
environment_query = read_file(environment_file)
environment_data = as_tibble(dbGetQuery(database_connection, environment_query))

#### Summarize AKVEG Database
####------------------------------

aerial_visits = site_visit_data %>%
  filter(perspective == 'aerial')

ground_visits = site_visit_data %>%
  filter(perspective == 'ground')

endsa_visits = site_visit_data %>%
  filter(observe_date > '2000-05-01')

originator = project_data %>%
  select(originator) %>%
  rename(contributor = originator)
funder = project_data %>%
  select(funder) %>%
  rename(contributor = funder)

contributor = rbind(originator, funder) %>%
  distinct(contributor) %>%
  arrange(contributor)
  

