# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Soil Horizons for AIM 2021 Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-07
# Usage: Must be executed in R version 4.4.3+.
# Description: "Calculate Soil Horizons for AIM 2021 Data" uses data from soil surveys to extract measurements related to soil composition, color, and structure. The script appends unique site visit identifiers, re-classifies values to match constraints in the AKVEG database, replaces empty observations with appropriate null values, corrects errors in the data, performs QA/QC checks, and renames columns to match the AKVEG template. Finally, the script combines the cleaned dataset with a previously formatted BLM AIM GMT-2 dataset. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(janitor)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)
library(tibble)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, 
                      root_folder,
                      'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/32_aim_various_2021')
source_folder = path(plot_folder, 'source')
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")

# Set repository directory
repository = path(drive,root_folder,'Repositories/akveg-database')

# Define input datasets
soil_original_input = path(workspace_folder, 
                           'AIM_Terrestrial_Alaska_Soils.csv')
soil_gmt2_input = path(workspace_folder,
                       "14_soilhorizons_aimvarious2021.csv")
soil_kobuk_input = path(source_folder, 
                        'blm_aim_seward_pen_2021_soils_data', 
                        'soil_horizon_texture_struct_deliverable.csv')
template_input = path(template_folder, 
                      "14_soil_horizons.xlsx")

# Define output dataset
soil_horizons_output = path(plot_folder, '14_soilhorizons_aimvarious2021.csv')

# Connect to AKVEG Database ----

# Import database connection function
connection_script = path(repository,
                         'package_DataProcessing', 
                         'connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = path(project_folder,
                      'Credentials',
                      'akveg_private_read',
                      'authentication_akveg_private_read.csv')
akveg_connection = connect_database_postgresql(authentication)

# Read in data ----
soil_horizons_original = read_csv(soil_original_input)
soil_gmt2_original = read_csv(soil_gmt2_input)
soil_kobuk_original = read_csv(soil_kobuk_input)
template = colnames(read_xlsx(path=template_input))

# Read PostgreSQL site visit table
query_site = 'SELECT site_visit.site_visit_code,
  site_visit.site_code,
  site.establishing_project_code
FROM site_visit
INNER JOIN site ON site_visit.site_code=site.site_code;'

site_data = as_tibble(dbGetQuery(akveg_connection, query_site))

# Subset data ----
# Drop sites that have already been processed or that were excluded from the AKVEG database
projects_to_process = c('aim_campbell_2018', 
                        'aim_kobuknortheast_2021', 
                        'aim_kobukwest_2021')

partial_horizons = soil_horizons_original %>%
  rename(site_code = PlotID) %>%
  left_join(site_data, by = 'site_code') %>% # Obtain project code & site visit code
  filter(establishing_project_code %in% projects_to_process) %>%
  select(site_visit_code, everything(), 
         -c(PrimaryKey, ProjectName,Latitude_NAD83,Longitude_NAD83,
            DateEstablished,DateVisited,ESD_EC,ESD_pH)) %>% 
  remove_empty(which = "cols")

# Calculate horizon order ----
# Convert horizon depth measurements to cm
# Generate sequential numbers starting from smallest depth_upper value
partial_horizons = partial_horizons %>% 
  group_by(site_visit_code) %>% 
  mutate(depth_upper = case_when(DepthMeasure=="in" ~ HorizonDepthUpper * 2.54,
                                       DepthMeasure=="cm" ~ HorizonDepthUpper),
         depth_lower = case_when(DepthMeasure=="in" ~ HorizonDepthLower * 2.54,
                                       DepthMeasure=="cm" ~ HorizonDepthLower)) %>% 
  arrange(depth_upper) %>% 
  mutate(horizon_order = row_number())
  
# Format primary and secondary horizons ----
# If there is more than one letter in ESD_Horizon, the second letter is the secondary horizon
# Site SMR-011, horizon order #3: ESD_Horizon is NULL, but horizon_suffix is 'a', which can only be associated with a horizon of 'O'
# Site KSNE-005, horizon order #1: ESD_Horizon is A, but horizon_suffix is 'i', which can only be associated with a horizon of 'O'. All lower horizons are 'O'
partial_horizons = partial_horizons %>%
  mutate(
    horizon_primary = case_when(
      site_visit_code == "SMR-011_20210704" & horizon_order == 3 ~ "O",
      site_visit_code == "KSNE-005_20210719" & horizon_order == 1 ~ "O",
      grepl(pattern = "^[A-Z]{1}$", x = ESD_Horizon) ~ ESD_Horizon,
      grepl(pattern = "^[A-Z]{2}$", x = ESD_Horizon) ~ str_split_i(ESD_Horizon,
                                                                   pattern = "", i = 1),
      grepl(pattern = "\\W", x = ESD_Horizon) ~ str_split_i(ESD_Horizon,
                                                            pattern = "\\W", i = 1),
      is.na(ESD_Horizon) ~ "NULL"
    ),
    horizon_secondary = case_when(
      grepl(pattern = "^[A-Z]{1}$", x = ESD_Horizon) ~ "NULL",
      grepl(pattern = "^[A-Z]{2}$", x = ESD_Horizon) ~ str_split_i(ESD_Horizon,
                                         pattern = "", i = 2),
      grepl(pattern = "\\W", x = ESD_Horizon) ~ str_split_i(ESD_Horizon,
                                                            pattern = "\\W", i = 2),
      is.na(ESD_Horizon) ~ "NULL"
    )
  )

# Format horizon suffixes ----
# If the observation contains a single digit, transform to NULL (does not correspond to any soil horizon that we know of)
# If the observation contains more than >1 letter, split into suffix_1 and suffix_2, unless it is one of a set of standard codes that is 2 letters long. Not all possible 2-letter codes are coded, but only 'jj' is present in our dataset.
# If the observation contains a letter and a digit e.g., g1, g2, keep only the letter.
# Add missing 'jj' horizon_suffix_2 for sites KSNE-11, KSNE-14, and WNTR-197 (based on original data from ABR)
partial_horizons = partial_horizons %>% 
  mutate(ESD_HorizonModifier = str_to_lower(ESD_HorizonModifier),
         horizon_suffix_1 = case_when(is.na(ESD_HorizonModifier) ~ "NULL",
                                      grepl(pattern="^(?!jj|ff)[a-z]{2}$",
                                            x=ESD_HorizonModifier,
                                            perl=T) ~ 
                                        str_split_i(ESD_HorizonModifier,pattern = "",i=1),
                                      grepl(pattern="^\\d{1}$", x=ESD_HorizonModifier) ~ "NULL",
                                      grepl(pattern="^[a-z]\\d", x=ESD_HorizonModifier) ~ str_split_i(ESD_HorizonModifier,pattern = "",i=1),
                                      .default = ESD_HorizonModifier),
         horizon_suffix_2 = case_when(grepl(pattern="^(?!jj|ff)[a-z]{2}$",
                                            x=ESD_HorizonModifier,
                                            perl=T) ~ 
                                        str_split_i(ESD_HorizonModifier,pattern = "",i=2),
                                      site_code %in% c('KSNE-011','WNTR-197') & horizon_order == 3 ~ 'jj',
                                      site_code == 'KSNE-014' & horizon_order == 4 ~ 'jj',
                                      .default = "NULL"),
         horizon_suffix_3 = "NULL",
         horizon_suffix_4 = "NULL")

# Generate depth_extend value (Boolean) ----
# From AKVEG Database Schema: The value can be TRUE only for the lowest measured horizon.
partial_horizons = partial_horizons %>%
  group_by(site_visit_code) %>%
  mutate(
    depth_extend = case_when(
      horizon_order != max(horizon_order) ~ "FALSE",
      depth_lower >= 100 ~ "FALSE",
      depth_lower < 100 &
        (horizon_suffix_1 == "f" | horizon_suffix_2 == "f") ~ "TRUE",
      depth_lower < 100 &
        horizon_primary %in% c("B", "C", "O") & horizon_suffix_1 != "f" & horizon_suffix_2 != "f" ~ "FALSE",
      .default = "NULL" # Any NULL values require a manual check
    )
  ) 

# Convert to logical class type
partial_horizons$depth_extend = as.logical(partial_horizons$depth_extend)

# Calculate horizon thickness ----
# Thickness is -999 if depth_extend is TRUE (measurement hindered by presence of restrictive layer)
partial_horizons = partial_horizons %>% 
  mutate(thickness_cm = case_when(depth_extend == "TRUE" ~ -999,
                                  depth_extend == "FALSE" ~ depth_lower - depth_upper,
                                  .default = -999))

# Format soil texture ----
# Replace codes FO, HO, SO, NT with NULL. FO, HO, and SO refer to organic soils; texture only applies to mineral soils.
partial_horizons = partial_horizons %>% 
  mutate(texture = case_when(is.na(Texture) ~ "NULL",
                             grepl(pattern="O", x=Texture) ~ "NULL",
                             Texture == "NT" ~ "NULL",
                             Texture == 'SIL' ~ 'silt loam',
                             Texture == 'SCL' ~ 'sandy clay loam',
                             Texture == 'S' ~ 'sand',
                             Texture == 'SI' ~ 'silt',
                             Texture == 'SL' ~ 'sandy loam',
                             Texture == 'L' ~ 'loam',
                             Texture == 'LS' ~ 'loamy sand',
                             Texture == 'CL' ~ 'clay loam',
                             Texture == 'SICL' ~ 'silty clay loam'))
  
# Format soil structure ----
partial_horizons = partial_horizons %>% 
  mutate(structure_uncorrected = case_when(is.na(ESD_Structure) ~ "NULL",
                               ESD_Structure == "other" ~ "NULL",
                               ESD_Structure == "MA" ~ "m", # 'Massive'
                               .default = str_to_lower(ESD_Structure)))

# Use original data from ABR to fill in some of the missing soil structure values
kobuk_structure = soil_kobuk_original %>% 
  mutate(site_code = str_to_upper(plot_id)) %>% 
  distinct(site_code, horizon_number, soil_structure) %>% 
  arrange(site_code, horizon_number) %>% 
  rename(horizon_order = horizon_number, structure_abr = soil_structure) %>% 
  select(site_code, horizon_order, structure_abr) %>% 
  right_join(partial_horizons, by = c("site_code", "horizon_order")) %>% 
  filter(structure_uncorrected == 'NULL' & !is.na(structure_abr)) %>% 
  mutate(structure_corrected = case_when(structure_abr == 'Massive' ~ 'm',
                                   structure_abr == 'Subangular blocky' ~ 'sbk',
                                   .default = 'NULL')) %>% 
  select(site_code, horizon_order, structure_corrected) %>% 
  filter(!(site_code == 'WNTR-203' & horizon_order == 3 & structure_corrected == 'm') &
           !(site_code == 'WNTR-197' & horizon_order == 3 & structure_corrected == 'sbk')) # Keep only one entry per site horizon

partial_horizons_corrected = partial_horizons %>% 
  left_join(kobuk_structure, by = c("site_code", "horizon_order")) %>% 
  mutate(structure = case_when(structure_uncorrected == 'NULL' 
                               & !is.na(structure_corrected) ~ structure_corrected,
                               .default = structure_uncorrected))

# Format remaining columns ----
# Note that the AIM database lists 'Stone' as the ESD_FragmentType2 for AIM Kobuk sites, but original data obtained from ABR indicates that values in ESD_FragVolPct2 refer to 'cobble' and values in ESD_FragVolPct3 refer to 'stone'. Assumed this was the case for AIM Campbell Tract sites as well. It seems likely that those sites would be primarily underlain by gravel.
partial_horizons_final = partial_horizons_corrected %>% 
  mutate(clay_percent = ESD_PctClay,
         total_coarse_fragment_percent = RockFragments,
         gravel_percent = ESD_FragVolPct,
         cobble_percent = ESD_FragVolPct2,
         stone_percent = ESD_FragVolPct3,
         matrix_value = ESD_Value,
         matrix_chroma = ESD_Chroma) %>% 
  mutate(across(clay_percent:matrix_chroma, ~replace_na(.x, -999))) %>% 
  mutate(matrix_hue = case_when(is.na(ESD_Hue) ~ "NULL",
                                .default = ESD_Hue)) %>%
  mutate(boulder_percent = -999,
         nonmatrix_feature = "NULL",
         nonmatrix_hue = "NULL",
         nonmatrix_value = -999,
         nonmatrix_chroma = -999) %>% 
    arrange(site_visit_code, horizon_order) %>% 
  select(all_of(template))

# QA/QC ----

# Numeric data: Ensure all values are positive and within a reasonable range
table(partial_horizons_final$horizon_order) # Whole numbers only

summary(partial_horizons_final$thickness_cm) # Should be some -999 values

# Ensure that thickness_cm = -999 is always paired with depth_extend = TRUE (and vice-versa)
partial_horizons_final %>% filter(thickness_cm==-999) %>% 
  ungroup(site_visit_code) %>%
  distinct(depth_extend)

partial_horizons_final %>% filter(depth_extend == TRUE) %>% 
  ungroup(site_visit_code) %>%
  distinct(thickness_cm)

boxplot(depth_upper ~ as.factor(horizon_order), 
        data = partial_horizons_final) # Should increase with increasing Horizon Order
boxplot(depth_lower ~ as.factor(horizon_order), 
        data = partial_horizons_final) # Should increase with increasing Horizon Order

# Create temp object without null values (coded as -999) to examine distribution of fragment types
# Range should be between 0-100
temp = partial_horizons_final %>% 
  filter(!(gravel_percent == -999 | stone_percent == -999 | cobble_percent == -999 | clay_percent == -999 | total_coarse_fragment_percent == -999))

summary(temp$gravel_percent)
summary(temp$cobble_percent)
summary(temp$stone_percent)
summary(temp$clay_percent)
summary(temp$total_coarse_fragment_percent) 

# Explore sites for which percent of total coarse fragments is 100%
# 2 sites with 100%. 1 site also has 100% gravel_percent, the other site has 75% gravel and 25% stone
temp %>% filter(total_coarse_fragment_percent==100) %>% 
  select(site_visit_code, horizon_order,
         gravel_percent, cobble_percent,stone_percent)

# All values for boulder_percent should be -999
unique(partial_horizons_final$boulder_percent)

rm(temp)

summary(partial_horizons_final$matrix_value) # Matrix value should be between 0 and 10 (some -999)
summary(partial_horizons_final$nonmatrix_value) # Should be all null (-999)

summary(partial_horizons_final$matrix_chroma) # Chroma should be between 0 and 8
summary(partial_horizons_final$nonmatrix_chroma) # Should be all null (-999)

# Categorical data: Ensure all values are part of the set of constrained values in the AKVEG data dictionary

# Depth extend should be either TRUE or FALSE
table(partial_horizons_final$depth_extend)

# Check values for horizon_primary and horizon_secondary
table(partial_horizons_final$horizon_primary) # 3 entries are NULL (Horizons 2-4 for site WNTR-198)

# Check that all horizon modifier values are valid
# horizon suffix 3 and 4 should all be NULL
unique(partial_horizons_final$horizon_suffix_3)
unique(partial_horizons_final$horizon_suffix_4)

partial_horizons_final %>%
  ungroup(site_visit_code) %>% 
  filter(horizon_suffix_1!="NULL") %>% 
  distinct(horizon_suffix_1, horizon_suffix_2) %>% 
  arrange(horizon_suffix_1)

# Check that horizon_suffix values are appropriately paired with horizons 
# a, e, i can only be paired with O
# w can only be paired with B
# f and jj typically paired with C
table(partial_horizons_final$horizon_suffix_1,
      partial_horizons_final$horizon_primary)

# Check values for texture
partial_horizons_final %>% 
  ungroup(site_visit_code) %>% 
  filter(texture!="NULL") %>% 
  distinct(texture)

# Check values for structure
partial_horizons_final %>% 
  ungroup(site_visit_code) %>% 
  filter(structure!="NULL") %>% 
  distinct(structure)

# Check matrix hue
partial_horizons_final %>% 
  ungroup(site_visit_code) %>% 
  filter(matrix_hue!="NULL") %>% 
  distinct(matrix_hue)

# Confirm that all nonmatrix_feature and nonmatrix_hue values are NULL
unique(partial_horizons_final$nonmatrix_feature)
unique(partial_horizons_final$nonmatrix_hue)

# Append GMT-2 data ----
soil_horizons_final = soil_gmt2_original %>% 
  select(all_of(template)) %>% 
  bind_rows(partial_horizons_final)

# Export as CSV ----
write_csv(x=soil_horizons_final,
          file=soil_horizons_output)

# Clear workspace ----
rm(list=ls())
