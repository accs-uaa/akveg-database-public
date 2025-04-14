# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Query data from AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-13
# Usage: Script should be executed in R 4.4.3+.
# Description: "Query data from AKVEG Database" is an example script to pull data for the ACCS Nelchina project from the AKVEG Database for all available tables.
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
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders (modify to your folder structure)
database_repository = path(drive, root_folder, 'Repositories/akveg-database-public')
credentials_folder = path(drive, root_folder, 'Example/Credentials/akveg_public_read')
input_folder = path(drive, root_folder, 'Example/Data_Input')
output_folder = path(input_folder, 'plot_data')

# Define input files
domain_input = path(input_folder, 'region_data/AlaskaYukon_MapDomain_3338.shp')
zone_input = path(input_folder, 'region_data/AlaskaYukon_VegetationZones_v1.1_3338.shp')
fireyear_input = path(input_folder, 'ancillary_data/AlaskaYukon_FireYear_10m_3338.tif')

# Define output files
taxa_output = path(output_folder, '00_taxonomy.csv')
project_output = path(output_folder, '01_project.csv')
site_visit_output = path(output_folder, '03_site_visit.csv')
site_point_output = path(output_folder, '03_site_point_3338.shp')
site_buffer_output = path(output_folder, '03_site_buffer_3338.shp')
vegetation_output = path(output_folder, '05_vegetation.csv')
abiotic_output = path(output_folder, '06_abiotic_top_cover.csv')
tussock_output = path(output_folder, '07_whole_tussock_cover.csv')
ground_output = path(output_folder, '08_ground_cover.csv')
structural_output = path(output_folder, '09_structural_group_cover.csv')
shrub_output = path(output_folder, '11_shrub_structure.csv')
environment_output = path(output_folder, '12_environment.csv')
soilmetrics_output = path(output_folder, '13_soil_metrics.csv')
soilhorizons_output = path(output_folder, '14_soil_horizons.csv')

# Define queries
taxa_file = path(database_repository, 'queries/00_taxonomy.sql')
project_file = path(database_repository, 'queries/01_project.sql')
site_visit_file = path(database_repository, 'queries/03_site_visit.sql')
vegetation_file = path(database_repository, 'queries/05_vegetation.sql')
abiotic_file = path(database_repository, 'queries/06_abiotic_top_cover.sql')
tussock_file = path(database_repository, 'queries/07_whole_tussock_cover.sql')
ground_file = path(database_repository, 'queries/08_ground_cover.sql')
structural_file = path(database_repository, 'queries/09_structural_group_cover.sql')
shrub_file = path(database_repository, 'queries/11_shrub_structure.sql')
environment_file = path(database_repository, 'queries/12_environment.sql')
soilmetrics_file = path(database_repository, 'queries/13_soil_metrics.sql')
soilhorizons_file = path(database_repository, 'queries/14_soil_horizons.sql')

# Read local data
domain_shape = st_read(domain_input)
zone_shape = st_read(zone_input)
fireyear_raster = rast(fireyear_input)

# Get geometry for intersection (example to subset data by Boreal)
#intersect_geometry = st_geometry(zone_shape[zone_shape$zone == 'Boreal Southern'
#                                            | zone_shape$zone == 'Boreal Central'
#                                            | zone_shape$zone == 'Boreal Northern'
#                                            | zone_shape$zone == 'Boreal Western'
#                                            | zone_shape$zone == 'Boreal Southwest',])

# Get geometry for intersection (example to subset data by Arctic)
intersect_geometry = st_geometry(zone_shape[zone_shape$zone == 'Arctic Northern'
                                            | zone_shape$zone == 'Arctic Western',])

#### QUERY AKVEG DATABASE
####------------------------------

# Import database connection function
connection_script = path(database_repository, 'pull_functions', 'connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = path(credentials_folder, 'authentication_akveg_public_read.csv')
database_connection = connect_database_postgresql(authentication)

# Read taxonomy standard from AKVEG Database
taxa_query = read_file(taxa_file)
taxa_data = as_tibble(dbGetQuery(database_connection, taxa_query))

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
  # Subset points to map domain (example to subset using a feature class)
  st_intersection(st_geometry(domain_shape)) %>%
  # Subset points to those within the target zone (example to subset using a feature class selection)
  st_intersection(intersect_geometry) %>%
  # Extract raster data to points
  mutate(fire_yr = terra::extract(fireyear_raster, ., raw=TRUE)[,2]) %>%
  # Drop geometry
  st_zm(drop = TRUE, what = "ZM") %>%
  # Example filter by project code (uncomment line below)
  #filter(prjct_cd == 'accs_nelchina_2023') %>%
  # Example filter by observation year (uncomment line below)
  #filter(year(obs_date) >= 2000) %>%
  # Example filter by perspective (uncomment line below)
  #filter(perspect == 'ground') %>%
  # Select columns
  dplyr::select(st_vst, prjct_cd, st_code, data_tier, obs_date, scp_vasc, scp_bryo, scp_lich,
                perspect, cvr_mthd, strc_class, fire_yr, hmgneous, plt_dim_m,
                lat_dd, long_dd, cent_x, cent_y, geometry)

# Export site visit data to shapefile
st_write(site_visit_data, site_point_output, append = FALSE) # Optional to check point selection in a GIS

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

# Read project data from AKVEG Database for selected site visits
project_query = read_file(project_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
project_data = as_tibble(dbGetQuery(database_connection, project_query)) %>%
  arrange(prjct_cd)

# Read vegetation cover data from AKVEG Database for selected site visits
vegetation_query = read_file(vegetation_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
vegetation_data = as_tibble(dbGetQuery(database_connection, vegetation_query))

# Read abiotic top cover data from AKVEG Database for selected site visits
abiotic_query = read_file(abiotic_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
abiotic_data = as_tibble(dbGetQuery(database_connection, abiotic_query))

# Read whole tussock cover data from AKVEG Database for selected site visits
tussock_query = read_file(tussock_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
tussock_data = as_tibble(dbGetQuery(database_connection, tussock_query))

# Read ground cover data from AKVEG Database for selected site visits
ground_query = read_file(ground_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
ground_data = as_tibble(dbGetQuery(database_connection, ground_query))

# Read structural group cover data from AKVEG Database for selected site visits
structural_query = read_file(structural_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
structural_data = as_tibble(dbGetQuery(database_connection, structural_query))

# Read shrub structure data from AKVEG Database for selected site visits
shrub_query = read_file(shrub_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
shrub_data = as_tibble(dbGetQuery(database_connection, shrub_query))

# Read environment data from AKVEG Database for selected site visits
environment_query = read_file(environment_file) %>%
  # Modify query with where statement
  str_replace(., ';', where_statement)
environment_data = as_tibble(dbGetQuery(database_connection, environment_query))

# Read soil metrics data from AKVEG Database for selected site visits
soilmetrics_query = read_file(soilmetrics_file) %>%
  str_replace(., ';', where_statement)
soilmetrics_data = as_tibble(dbGetQuery(database_connection, soilmetrics_query))

# Read soil horizons data from AKVEG Database for selected site visits
soilhorizons_query = read_file(soilhorizons_file) %>%
  str_replace(., ';', where_statement)
soilhorizons_data = as_tibble(dbGetQuery(database_connection, soilhorizons_query))

# Check number of cover observations per project
project_check = vegetation_data %>%
  left_join(site_visit_data, join_by('st_vst')) %>%
  group_by(prjct_cd) %>%
  summarize(obs_n = n())

# Export data to csv files
taxa_data %>%
  write.csv(., file = taxa_output, fileEncoding = 'UTF-8', row.names = FALSE)
project_data %>%
  write.csv(., file = project_output, fileEncoding = 'UTF-8', row.names = FALSE)
site_visit_data %>%
  st_drop_geometry() %>%
  write.csv(., file = site_visit_output, fileEncoding = 'UTF-8', row.names = FALSE)
vegetation_data %>%
  write.csv(., file = vegetation_output, fileEncoding = 'UTF-8', row.names = FALSE)
abiotic_data %>%
  write.csv(., file = abiotic_output, fileEncoding = 'UTF-8', row.names = FALSE)
tussock_data %>%
  write.csv(., file = tussock_output, fileEncoding = 'UTF-8', row.names = FALSE)
ground_data %>%
  write.csv(., file = ground_output, fileEncoding = 'UTF-8', row.names = FALSE)
structural_data %>%
  write.csv(., file = structural_output, fileEncoding = 'UTF-8', row.names = FALSE)
shrub_data %>%
  write.csv(., file = shrub_output, fileEncoding = 'UTF-8', row.names = FALSE)
environment_data %>%
  write.csv(., file = environment_output, fileEncoding = 'UTF-8', row.names = FALSE)
soilmetrics_data %>%
  write.csv(., file = soilmetrics_output, fileEncoding = 'UTF-8', row.names = FALSE)
soilhorizons_data %>%
  write.csv(., file = soilhorizons_output, fileEncoding = 'UTF-8', row.names = FALSE)
