# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Compile data views (example)
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-02-24
# Usage: Script should be executed in Python 3.12+.
# Description: "Compile data views" provides an example of compiling a set of data views for a user-specified region from the AKVEG Database.
# ---------------------------------------------------------------------------

# Import packages
import os
import pandas as pd
import geopandas as gpd
from datetime import datetime
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Set root directory (modify to your folder structure)
root_folder = 'ACCS_Work'

# Define input folders (modify to your folder structure)
database_repository = os.path.join('C:/', root_folder, 'Repositories/akveg-database')
credentials_folder = os.path.join('C:/', root_folder, 'Credentials/akveg_private_read')
project_folder = os.path.join('C:/', root_folder, 'Projects/VegetationEcology/AKVEG_Map/Data/Data_Input')
input_folder = os.path.join(project_folder, 'region_data')
output_folder = os.path.join(project_folder, 'example_data')

# Define input files
zone_input = os.path.join(input_folder, 'AlaskaYukon_VegetationZones_v1.1_3338.shp') # Use EPSG:3338 for Alaska

# Define output files
site_visit_output = os.path.join(output_folder, '03_site_visit.csv')
site_points_output = os.path.join(output_folder, '03_site_visit_3338.shp') # Spatial output in EPSG:3338 for Alaska
vegetation_output = os.path.join(output_folder, '05_vegetation.csv')

# Define queries
taxa_file = os.path.join(database_repository, '05_queries/analysis/00_taxon_query.sql')
site_visit_file = os.path.join(database_repository, '05_queries/analysis/03_site_visit_query.sql')
vegetation_file = os.path.join(database_repository, '05_queries/analysis/05_vegetation_query.sql')

# Read local data
zone_shape = gpd.read_file(zone_input)

#### QUERY AKVEG DATABASE
####------------------------------

# Create a connection to the AKVEG PostgreSQL database
authentication_file = os.path.join(credentials_folder, 'authentication_akveg_private.csv')
database_connection = connect_database_postgresql(authentication_file)

# Read taxonomy standard from AKVEG Database
taxa_read = open(taxa_file, 'r')
taxa_query = taxa_read.read()
taxa_read.close()
taxa_data = query_to_dataframe(database_connection, taxa_query)

# Read site visit data from AKVEG Database
site_visit_read = open(site_visit_file, 'r')
site_visit_query = site_visit_read.read()
site_visit_read.close()
site_visit_data = query_to_dataframe(database_connection, site_visit_query)
site_visit_data['obs_datetime'] = pd.to_datetime(site_visit_data['obs_date'])
site_visit_data['obs_year'] = site_visit_data['obs_datetime'].dt.year

# Create geodataframe
site_visit_data = gpd.GeoDataFrame(
    site_visit_data,
    geometry=gpd.points_from_xy(site_visit_data.long_dd,
                                site_visit_data.lat_dd),
    crs='EPSG:4269')

# Convert geodataframe to EPSG:3338
site_visit_data = site_visit_data.to_crs(crs='EPSG:3338')

# Extract coordinates in EPSG:3338
site_visit_data['cent_x'] = site_visit_data.geometry.x
site_visit_data['cent_y'] = site_visit_data.geometry.y

# Get geometry for intersection
intersect_geometry = zone_shape[zone_shape['zone_short'].isin(['Arctic Northern', 'Arctic Western'])]

# Intersect points with intersect geometry
site_visit_data = gpd.clip(site_visit_data, intersect_geometry)

# Filter out aerial observations
site_visit_data = site_visit_data[site_visit_data['perspect'] == 'ground']

# Filter out data from before 2010
site_visit_data = site_visit_data[site_visit_data['obs_year'] >= 2010]

# Select columns
site_visit_data = site_visit_data['st_vst', 'prjct_cd', 'obs_date', 'scp_vasc', 'scp_bryo', 'scp_lich', 'perspect']
