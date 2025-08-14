# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Compile data views (example)
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-02-24
# Usage: Script should be executed in R 4.3.2+.
# Description: "Compile data views" provides an example of compiling a set of data views for a user-specified region from the AKVEG Database.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(fs)
library(janitor)
library(lubridate)
library(readr)
library(readxl)
library(RPostgres)
library(sf)
library(stringr)
library(terra)
library(tibble)
library(tidyr)

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Set root directory (modify to your folder structure)
root_folder = 'ACCS_Work'

# Define input folders (modify to your folder structure)
database_repository = path('C:', root_folder, 'Repositories/akveg-database')
credentials_folder = path('C:', root_folder, 'Credentials/akveg_private_read')
project_folder = path('C:', root_folder, 'Projects/VegetationEcology/AKVEG_Map/Data/Data_Input')
input_folder = path(project_folder, 'region_data')
output_folder = path(project_folder, 'example_data')

# Define input files
zone_input = path(input_folder, 'AlaskaYukon_VegetationZones_v1.1_3338.shp') # Use EPSG:3338 for Alaska

# Define output files
site_visit_output = path(output_folder, '03_site_visit.csv')
site_points_output = path(output_folder, '03_site_visit_3338.shp') # Spatial output in EPSG:3338 for Alaska
vegetation_output = path(output_folder, '05_vegetation.csv')

# Define queries
taxa_file = path(database_repository, '05_queries/analysis/00_taxonomy.sql')
site_visit_file = path(database_repository, '05_queries/analysis/03_site_visit.sql')
vegetation_file = path(database_repository, '05_queries/analysis/05_vegetation.sql')

# Read local data
zone_shape = st_read(zone_input)

#### QUERY AKVEG DATABASE
####------------------------------

# Import database connection function
connection_script = path(database_repository, 'package_DataProcessing', 'connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = path(credentials_folder, 'authentication_akveg_private.csv')
database_connection = connect_database_postgresql(authentication)

# Read taxonomy standard from AKVEG Database
taxa_query = read_file(taxa_file)
taxa_data = as_tibble(dbGetQuery(database_connection, taxa_query))

# Get geometry for intersection
intersect_geometry = st_geometry(zone_shape[zone_shape$zone == 'Arctic Northern',])

# Read site visit data from AKVEG Database
site_visit_query = read_file(site_visit_file)
site_visit_data = as_tibble(dbGetQuery(database_connection, site_visit_query)) %>%
  # Convert geometries to points with EPSG:4269
  st_as_sf(x = ., coords = c('long_dd', 'lat_dd'), crs = 4269, remove = FALSE) %>%
  # Reproject coordinates to EPSG 3338
  st_transform(crs = st_crs(3338)) %>%
  # Add EPSG:3338 centroid coordinates
  mutate(cent_x = st_coordinates(.$geometry)[,1],
         cent_y = st_coordinates(.$geometry)[,2]) %>%
  # Subset points to those within the target zone
  st_intersection(intersect_geometry) %>% # Intersect with named feature
  st_zm(drop = TRUE, what = "ZM") %>%
  # Filter out aerial observations
  filter(perspect == 'ground') %>%
  # Filter out data from before 2010
  filter(year(obs_date) >= 2010) %>%
  # Select columns
  dplyr::select(st_vst, prjct_cd, obs_date, scp_vasc, scp_bryo, scp_lich, perspect,
                cvr_mthd, plt_dim_m, lat_dd, long_dd, cent_x, cent_y, geometry)

# Export site visit data to shapefile
st_write(site_visit_data, site_points_output, append = FALSE) # Optional to check point selection in a GIS

# Write where statement for site visits
input_sql = site_visit_data %>%
  # Drop geometry
  st_drop_geometry() %>%
  select(st_vst) %>%
  # Format site visit codes
  mutate(st_vst = paste('\'', st_vst, '\'', sep = '')) %>%
  # Collapse rows
  summarize(st_vst=paste(st_vst,collapse=", ")) %>%
  # Pull result out of dataframe
  pull(st_vst)
where_statement = paste('\r\nWHERE site_visit.site_visit_code IN (',
                        input_sql,
                        ');',
                        sep = '')

# Read vegetation cover data from AKVEG Database for selected site visits
vegetation_query = read_file(vegetation_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
vegetation_data = as_tibble(dbGetQuery(database_connection, vegetation_query))

# Check number of cover observations per project
project_data = vegetation_data %>%
  left_join(site_visit_data, join_by('st_vst')) %>%
  group_by(prjct_cd) %>%
  summarize(obs_n = n())

# Export data to csv files
site_visit_data %>%
  st_drop_geometry() %>%
  write.csv(., file = site_visit_output, fileEncoding = 'UTF-8', row.names = FALSE)
vegetation_data %>%
  write.csv(., file = vegetation_output, fileEncoding = 'UTF-8', row.names = FALSE)
