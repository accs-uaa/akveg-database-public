# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment Data for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini
# Last Updated: 2024-09-05
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Environment Data for USFWS Pribilof Islands 2022 data" uses environment data recorded during vegetation surveys to populate fields in the AKVEG Environment table. The script appends unique site visit identifiers, re-classifies categorical values to match constraints in the AKVEG database, replaces empty observations with appropriate null values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input dataset
site_visit_input = path(plot_folder, '03_sitevisit_fwspribilof2022.csv')
elevation_input = path(workspace_folder, 'pribilof_centroid_coordinates.csv')
environment_input = path(source_folder, 'pribilofs2022.xlsx')
template_input = path(template_folder, "12_environment.xlsx")

# Define output dataset
environment_output = path(plot_folder, '12_environment_fwspribilof2022.csv')

# Read in data ----
environment_original = read_xlsx(environment_input)
elevation_original = read_csv(elevation_input)
template = colnames(read_xlsx(path=template_input))
site_visit_original = read_csv(site_visit_input)

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

# Append elevation data ----
elevation_data = elevation_original %>% 
  rename(elevation_m = RASTERVALU) %>% 
  mutate(site_code = str_replace(Relevé, "-", "_")) %>% 
  select(site_code, elevation_m)

environment_data = environment_original %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_")) %>% # Convert dashes to underscore
  right_join(site_visit_data, by = "site_code") %>% # Use right join to drop 6 anthropogenic sites
  left_join(elevation_data, by = 'site_code')

summary(environment_data$elevation_m) # No null values

# Select only relevant columns
environment_data = environment_data %>%
  select(site_visit_code, Topo_pos, Micr_topo, Moist_regi,
         Mean_relie, Remarks, elevation_m)

# Re-classify landscape variables ----
# Classify as hill or mountain based on elevation data and thresholds listed in the AKVEG database dictionary
# For physiography: Can we come up with elevation-based thresholds to categorize higher-elevation areas as one of upland, subalpine, alpine? Not sure what makes sense for the Pribilofs.
unique(environment_data$Topo_pos)

environment_reclass = environment_data %>% 
  mutate(Topo_pos = str_to_lower(Topo_pos)) %>% 
  mutate(macrotopography = case_when(grepl('ridge', Topo_pos) ~ 'ridge',
                                     grepl('slop', Topo_pos) ~ 'slope',
                                     Remarks == 'Ephemeral Lake Bed' ~ 'lake bed',
                                     grepl('Lake', Remarks) ~ 'lake shore',
                                     grepl('depression', Topo_pos) ~ 'depression',
                                     grepl('plateau', Topo_pos) ~ 'summit',
                                     Remarks == 'intertidal lagoon mud flat' ~ 'tidal flat',
                                     grepl('lagoon', Remarks) ~ 'tidal',
                                     grepl('plain', Topo_pos) ~ 'plane',
                                     Topo_pos == 'dunes' ~ 'dunes',
                                     site_visit_code == 'STP22_006_20220813' ~ 'dunes',
                                     .default = 'NULL',
                                     )) %>% 
  mutate(geomorphology = case_when(grepl('valley', Topo_pos) ~ 'valley',
                                   grepl('plateau', Topo_pos) ~ 'plateau',
                                   grepl('lagoon', Remarks) ~ 'lagoon',
                                   elevation_m > 30 & elevation_m <= 300 & (macrotopography == 'slope' | macrotopography == 'ridge') ~ 'hill',
                                   elevation_m > 300 ~ 'mountain',
                                   .default = 'NULL')) %>% 
  mutate(physiography = case_when(geomorphology == 'lagoon' ~ 'coastal',
                                  grepl('Sand Beach', Remarks) ~ 'coastal',
                                  grepl('subalpine', Remarks) ~ 'subalpine',
                                  grepl('lake', macrotopography) ~ 'lacustrine', # needs review
                                  .default = "NULL"))

table(environment_reclass$Topo_pos, environment_reclass$geomorphology)
table(environment_reclass$Topo_pos, environment_reclass$macrotopography)

# Reclassify microtopography ----
unique(environment_reclass$Micr_topo)

environment_reclass = environment_reclass %>% 
  mutate(Micr_topo = str_to_lower(Micr_topo),
         Micr_topo = str_replace(Micr_topo, 'convaced', 'concaved')) %>% 
  mutate(microtopography = case_when(site_visit_code == 'STG22_042_20220820' ~ 'boulder field',
                                     site_visit_code == 'STP22_050_20220811' ~ 'scree',
                                     Micr_topo == 'linear drainages; boulder mounds' ~ 'water tracks',
    grepl('vegetated talus', ignore.case = TRUE, Remarks) ~ 'soil-covered rocks',
                                     grepl('talus field', ignore.case = TRUE, Remarks) ~ 'talus',
    grepl('boulder field', ignore.case = TRUE, Remarks) ~ 'boulder field',
                                    grepl('talus mounds', Micr_topo) ~ 'soil-covered rocks',
                                    grepl('^talus', Micr_topo) ~ 'talus',
                                     grepl('^boulders', Micr_topo) ~ 'boulder field',
    grepl('rock mounds', Micr_topo) ~ 'soil-covered rocks',
    grepl('^concav', Micr_topo) ~ 'concave',
                                     grepl('^hummock', Micr_topo) ~ 'hummocks',
                                     Remarks == 'Talus/Scree Field' ~ 'talus',
                                     Micr_topo == 'undulations' ~ 'undulating',
                                     Micr_topo == 'ridges and trough' ~ 'ridges and swales',
                                    grepl('boulder mounds', Micr_topo) ~ 'soil-covered rocks',
                                   grepl('boulder hummocks', Micr_topo) ~ 'soil-covered rocks',
    grepl("^m[a-z]+ hummocks", Micr_topo) ~ 'hummocks',
                                     .default = 'NULL'))
                                     
# Format moisture regime
unique(environment_reclass$Moist_regi)

environment_reclass = environment_reclass %>% 
  mutate(moisture_regime = str_remove(Moist_regi, 'sub')) # Or do we want to convert 'sub' to encompass two moisture regimes e.g., mesic-xeric heterogeneous?

# Add remaining columns ----
# With appropriate null values
# Unclear what column 'Mean_relie' in original dataset is supposed to represent. All values are zero except for two. Seems like that shouldn't be the case if it were microrelief since many of the sites are said to have hummocks.
environment_final = environment_reclass %>% 
  mutate(drainage = "NULL",
         disturbance = case_when(grepl('game trails', Remarks) ~ 'wildlife trails',
                                 .default = 'NULL'),
         disturbance_severity = 'NULL',
         disturbance_time_y = -999,
         depth_water_cm = -999,
         depth_moss_duff_cm = -999,
         depth_restrictive_layer_cm = -999,
         restrictive_type = "NULL",
         microrelief_cm = -999,
         surface_water = "NULL",
         soil_class = "NULL",
         cryoturbation = "NULL",
         depth_15_percent_coarse_fragments_cm = -999,
         dominant_texture_40_cm = "NULL") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(environment_final, is.na)
    , sum))

# Export as CSV ----
write_csv(x=environment_final,
          file=environment_output)

# Clean workspace ----
rm(list=ls())
