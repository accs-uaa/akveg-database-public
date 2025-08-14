# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Tetlin 2022-2024 transect data for AKVEG Database
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2024-11-10
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Tetlin 2022-2024 transect data for AKVEG Database" formats project, site, site visit, vegetation cover, and abiotic top cover data for entry into AKVEG Database.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(sf)
library(stringr)
library(tibble)
library(tidyr)

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/53_fws_tetlin_2024')
template_folder = path(project_folder, 'Data/Data_Entry')
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive, root_folder, 'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_private_read')

# Define input datasets
site_2022_input = path(source_folder, 'TetlinNWR_2022_wood_bison_habitat_assessment', 'TNWR_2022_points_sampled_coordinates.csv')
sample_2022_input = path(source_folder, 'TetlinNWR_2022_wood_bison_habitat_assessment', 'TNWR_2022_points_sampled_field_data.csv')
species_2022_input = path(source_folder, 'TetlinNWR_2022_wood_bison_habitat_assessment', 'TNWR_2022_species_dictionary.csv')
site_2024_input = path(source_folder, 'TetlinNWR_2024_wood_bison_habitat_assessment', 'TetlinNWR_2024_sites_compiled.csv')
sample_2024_input = path(source_folder, 'TetlinNWR_2024_wood_bison_habitat_assessment', 'TetlinNWR_2024_species_compiled.csv')

# Define input templates
project_template = path(template_folder, '01_project.xlsx')
site_template = path(template_folder, '02_site.xlsx')
visit_template = path(template_folder, '03_site_visit.xlsx')
vegetation_template = path(template_folder, '05_vegetation_cover.xlsx')

# Define output dataset
project_output = path(plot_folder, '01_project_fwstetlin2024.csv')
site_output = path(plot_folder, '02_site_fwstetlin2024.csv')
site_visit_output = path(plot_folder, '03_sitevisit_fwstetlin2024.csv')
vegetation_output = path(plot_folder, '05_vegetationcover_fwstetlin2024.csv')
abiotic_output = path(plot_folder, '06_abiotictopcover_fwstetlin2024.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_private.csv')

#### QUERY AKVEG DATABASE
####------------------------------

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

#### PARSE 2022 DATA
####------------------------------

# Create data templates
project_columns = colnames(read_xlsx(path=project_template))

# Parse project data
project_data = setNames(data.frame(matrix(ncol = length(project_columns), nrow = 1)), project_columns) %>%
  mutate(project_code = 'fws_tetlin_2024',
         project_name = 'Tetlin National Wildlife Refuge Bison Habitat Linear Transects',
         originator = 'USFWS',
         funder = 'USFWS',
         manager = 'Hunter Gravley',
         completion = 'ongoing',
         year_start = 2022,
         year_end = 2024,
         project_description = 'Linear transect vegetation plots assessed to quantify and describe potential habitat for wood bison on the Tetlin National Wildlife Refuge.',
         private = 'TRUE')

# Parse 2022 site data
site_2022 = read_csv(site_2022_input) %>%
  # Convert geometries to points with EPSG:4326
  st_as_sf(x = ., coords = c('POINT_X', 'POINT_Y'), crs = 4326, remove = FALSE) %>%
  # Reproject coordinates to EPSG 4269
  st_transform(crs = st_crs(4269)) %>%
  # Add EPSG:4269 centroid coordinates
  mutate(longitude_dd = st_coordinates(.$geometry)[,1],
         latitude_dd = st_coordinates(.$geometry)[,2]) %>%
  st_zm(drop = TRUE, what = "ZM") %>%
  st_drop_geometry() %>%
  # Create site code
  mutate(site_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, sep = ''),
                               TransectID < 100 ~ paste('TET22_0', TransectID, sep = ''),
                               TRUE ~ paste('TET22_', TransectID, sep = ''))) %>%
  # Add site metadata
  mutate(establishing_project_code = 'fws_tetlin_2024',
         perspective = 'ground',
         cover_method = 'subplot transect visual estimate',
         h_datum = 'NAD83',
         h_error_m = 3,
         positional_accuracy = 'consumer grade GPS',
         plot_dimensions_m = '1×10',
         location_type = 'targeted') %>%
  # Select final columns
  select(site_code, establishing_project_code, perspective, cover_method, h_datum, latitude_dd, longitude_dd,
         h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse 2022 site visit data
site_visit_2022 = read_csv(site_2022_input) %>%
  # Create site code
  mutate(site_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, sep = ''),
                               TransectID < 100 ~ paste('TET22_0', TransectID, sep = ''),
                               TRUE ~ paste('TET22_', TransectID, sep = ''))) %>%
  # Create site visit code
  mutate(site_visit_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, '_20220715', sep = ''),
                               TransectID < 100 ~ paste('TET22_0', TransectID, '_20220715', sep = ''),
                               TRUE ~ paste('TET22_', TransectID, '_20220715', sep = ''))) %>%
  # Add site visit metadata
  mutate(project_code = 'fws_tetlin_2024',
         data_tier = 'map development & verification',
         observe_date = '2022-07-15',
         veg_observer = 'Brent Jamison',
         veg_recorder = 'Trent Gilmore',
         env_observer = 'none',
         soils_observer = 'none',
         scope_vascular = 'top canopy',
         scope_bryophyte = 'none',
         scope_lichen = 'none',
         structural_class = 'not assessed',
         homogeneous = 'TRUE') %>%
  # Select final columns
  select(site_visit_code, project_code, site_code, data_tier, observe_date, veg_observer, veg_recorder,
         env_observer, soils_observer, scope_vascular, scope_bryophyte, scope_lichen, structural_class, homogeneous)

# Read species codes and names
species_2022_data = read_csv(species_2022_input) %>%
  select(abbreviation, name_original) %>%
  mutate(abbreviation = str_to_upper(abbreviation))

# Prepare 2022 vegetation cover and abiotic top cover data
vegetation_2022 = read_csv(sample_2022_input) %>%
  # Create site visit code
  mutate(site_visit_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, '_20220715', sep = ''),
                                     TransectID < 100 ~ paste('TET22_0', TransectID, '_20220715', sep = ''),
                                     TRUE ~ paste('TET22_', TransectID, '_20220715', sep = ''))) %>%
  # Add vegetation cover metadata
  mutate(dead_status = 'FALSE',
         cover_type = 'top canopy cover') %>%
  rename(cover_percent = CoverTx) %>%
  # Join original species names
  mutate(SpeciesCode = str_to_upper(SpeciesCode)) %>%
  left_join(species_2022_data, by = join_by(SpeciesCode == abbreviation)) %>%
  # Join adjudicated and accepted names
  left_join(taxa_all, by = join_by(name_original == taxon_name), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Adjudicate names
  mutate(name_adjudicated = case_when(name_original == 'Arctostaphlyos rubra' ~ 'Arctostaphylos rubra',
                                      name_original == 'Arctostaphlyos uva-ursi' ~ 'Arctostaphylos uva-ursi',
                                      name_original == 'Astragalus species' ~ 'Astragalus',
                                      name_original == 'Calamagrostis purperea' ~ 'Calamagrostis purpurascens ssp. purpurascens',
                                      name_original == 'Calamagrostis species' ~ 'Calamagrostis',
                                      name_original == 'Carex capita' ~ 'Carex capitata',
                                      name_original == 'Epibolium angustifolium' ~ 'Epilobium angustifolium',
                                      name_original == 'Equisetum species' ~ 'Equisetum',
                                      name_original == 'Juncus species' ~ 'Juncus',
                                      name_original == 'Picea species' ~ 'Picea',
                                      name_original == 'Poa species' ~ 'Poa',
                                      name_original == 'Rubus articus' ~ 'Rubus arcticus',
                                      name_original == 'Salix species' ~ 'Salix',
                                      name_original == 'Sagittatus species' ~ 'Sagittaria cuneata',
                                      name_original == 'Vaccinium vitis-idea' ~ 'Vaccinium vitis-idaea',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final columns
  select(site_visit_code, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  # Join adjudicated and accepted names
  left_join(taxa_all, by = join_by(name_adjudicated == taxon_name), keep = TRUE)

# Parse 2022 abiotic top cover
abiotic_2022 = vegetation_2022 %>%
  # Remove vegetation observations
  filter(is.na(name_adjudicated)) %>%
  # Assign abiotic element
  mutate(abiotic_element = case_when(name_original == 'Bare ground' ~ 'soil',
                                     name_original == 'Burned or fallen wood' ~ 'dead down wood (≥ 2 mm)',
                                     name_original == 'Lichen and mosses grouped' ~ 'biotic',
                                     name_original == 'Dead graminoids and forbes' ~ 'litter (< 2 mm)',
                                     name_original == 'Submerged in water' ~ 'water',
                                     TRUE ~ 'error')) %>%
  # Change cover name
  rename(abiotic_top_cover_percent = cover_percent) %>%
  # Select final columns
  select(site_visit_code, abiotic_element, abiotic_top_cover_percent)
  
# Parse 2022 vegetation cover
vegetation_2022 = vegetation_2022 %>%
  # Remove abiotic classes
  filter(!is.na(name_adjudicated)) %>%
  # Remove absences
  filter(cover_percent != 0) %>%
  # Select final columns
  select(site_visit_code, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

#### PARSE 2024 DATA
####------------------------------

# Parse 2024 site data
site_2024 = read_csv(site_2024_input) %>%
  # Convert geometries to points with EPSG:4326
  st_as_sf(x = ., coords = c('Longitude', 'Latitude'), crs = 4326, remove = FALSE) %>%
  # Reproject coordinates to EPSG 4269
  st_transform(crs = st_crs(4269)) %>%
  # Add EPSG:4269 centroid coordinates
  mutate(longitude_dd = st_coordinates(.$geometry)[,1],
         latitude_dd = st_coordinates(.$geometry)[,2]) %>%
  st_zm(drop = TRUE, what = "ZM") %>%
  st_drop_geometry() %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Add site metadata
  mutate(establishing_project_code = 'fws_tetlin_2024',
         perspective = 'ground',
         cover_method = 'subplot transect visual estimate',
         h_datum = 'NAD83',
         positional_accuracy = 'consumer grade GPS',
         plot_dimensions_m = '1×10',
         location_type = 'targeted') %>%
  # Correct horizontal error
  rename(h_error_m = Cord_error) %>%
  mutate(h_error_m = case_when(h_error_m == 0 ~ 3,
                               TRUE ~ h_error_m)) %>%
  # Select final columns
  select(site_code, establishing_project_code, perspective, cover_method, h_datum, latitude_dd, longitude_dd,
         h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse 2024 site visit data
site_visit_2024 = read_csv(site_2024_input) %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Create site visit code
  mutate(site_visit_code = paste(site_code, '_', Date, sep='')) %>%
  # Reformat date
  mutate(observe_date = paste(str_sub(Date, 1, 4), '-', str_sub(Date, 5, 6), '-', str_sub(Date, 7, 8), sep='')) %>%
  # Rename columns
  rename(veg_observer = Veg_observer,
         veg_recorder = Recorder,
         structural_class = Structural_class) %>%
  # Correct recorder
  mutate(veg_recorder = case_when(veg_recorder == 'Carrol Mahara' ~ 'Carol Mahara',
                                  TRUE ~ veg_recorder)) %>%
  # Correct structural class
  mutate(structural_class = str_to_lower(structural_class)) %>%
  mutate(structural_class = case_when(structural_class == 'dwarf needle forest' ~ 'dwarf needleleaf forest',
                                      structural_class == 'n/a' ~ 'low shrub',
                                      structural_class == 'unknown' ~ 'dwarf broadleaf forest',
                                      TRUE ~ structural_class)) %>%
  # Add site visit metadata
  mutate(project_code = 'fws_tetlin_2024',
         data_tier = 'map development & verification',
         env_observer = 'none',
         soils_observer = 'none',
         scope_vascular = 'top canopy',
         scope_bryophyte = 'none',
         scope_lichen = 'none',
         homogeneous = 'TRUE') %>%
  # Select final columns
  select(site_visit_code, project_code, site_code, data_tier, observe_date, veg_observer, veg_recorder,
         env_observer, soils_observer, scope_vascular, scope_bryophyte, scope_lichen, structural_class, homogeneous)

# Parse 2024 abiotic top cover data
abiotic_2024 = read_csv(site_2024_input) %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Create site visit code
  mutate(site_visit_code = paste(site_code, '_', Date, sep='')) %>%
  # Create a biotic sum
  mutate(Cover_biotic = Cover_lichens + Cover_sphagnum + Cover_feather_moss) %>%
  # Select columns for pivot
  select(site_visit_code, Cover_litter, Cover_water, Cover_bare_rock, Cover_soil, Cover_biotic) %>%
  # Pivot data to long form
  pivot_longer(!site_visit_code, names_to = "abiotic_element", values_to = "abiotic_top_cover_percent") %>%
  # Format abiotic element names
  mutate(abiotic_element = case_when(abiotic_element == 'Cover_bare_rock' ~ 'rock fragments',
                                     abiotic_element == 'Cover_biotic' ~ 'biotic',
                                     abiotic_element == 'Cover_litter' ~ 'litter (< 2 mm)',
                                     abiotic_element == 'Cover_soil' ~ 'soil',
                                     abiotic_element == 'Cover_water' ~ 'water',
                                     TRUE ~ 'error')) %>%
  # Correct na to zero
  mutate(abiotic_top_cover_percent = case_when(is.na(abiotic_top_cover_percent) ~ 0,
                                               TRUE ~ abiotic_top_cover_percent))

# Parse 2024 vegetation cover data
vegetation_2024 = read_csv(sample_2024_input) %>%
  # Format site_code
  rename(site_code = 1) %>%
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Pivot data to long form
  pivot_longer(!site_code, names_to = 'name_original', values_to = 'cover_percent') %>%
  # Join site visit code
  left_join(site_visit_2024, by = join_by('site_code' == 'site_code')) %>%
  # Select columns
  select(site_visit_code, name_original, cover_percent) %>%
  # Join taxon names
  mutate(name_adjudicated = name_original) %>%
  mutate(name_adjudicated = str_replace(name_adjudicated, ' species', '')) %>%
  mutate(name_adjudicated = str_replace(name_adjudicated, ' s. ', ' ssp. ')) %>%
  mutate(name_adjudicated = case_when(name_adjudicated == 'Carex_t1' ~ 'Carex',
                                      name_adjudicated == 'Carex_t2' ~ 'Carex',
                                      name_adjudicated == 'Salix_t1' ~ 'Salix',
                                      name_adjudicated == 'Arctostaphylos rubra' ~ 'Arctous rubra',
                                      TRUE ~ name_adjudicated)) %>%
  # Filter out zero and n/a values
  filter(!is.na(cover_percent)) %>%
  filter(cover_percent != 0) %>%
  # Join taxon data
  left_join(taxa_all, by = join_by('name_adjudicated' == 'taxon_name')) %>%
  # Add metadata
  mutate(dead_status = 'FALSE',
         cover_type = 'top canopy cover') %>%
  # Select final columns
  select(site_visit_code, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  # Correct duplicates
  filter(site_visit_code != 'TET24_008_20240725' | name_original != 'Arctous rubra') %>%
  mutate(cover_percent = case_when(site_visit_code == 'TET24_008_20240725' & name_adjudicated == 'Arctous rubra' ~ 3.2,
                                   TRUE ~ cover_percent))

#### MERGE AND EXPORT 2022 AND 2024 DATA
####------------------------------

# Merge data
site_data = rbind(site_2022, site_2024) %>%
  filter(site_code != 'TET24_036' & site_code != 'TET22_540')
site_visit_data = rbind(site_visit_2022, site_visit_2024) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')
vegetation_data = rbind(vegetation_2022, vegetation_2024) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')
abiotic_data = rbind(abiotic_2022, abiotic_2024) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')

# Export data
write.csv(project_data, project_output, row.names = FALSE, fileEncoding = "UTF-8")
write.csv(site_data, site_output, row.names = FALSE, fileEncoding = "UTF-8")
write.csv(site_visit_data, site_visit_output, row.names = FALSE, fileEncoding = "UTF-8")
write.csv(vegetation_data, vegetation_output, row.names = FALSE, fileEncoding = "UTF-8")
write.csv(abiotic_data, abiotic_output, row.names = FALSE, fileEncoding = "UTF-8")