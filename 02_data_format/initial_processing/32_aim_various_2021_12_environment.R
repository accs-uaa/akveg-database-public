# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment for AIM 2021 Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-06
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Environment for AIM 2021 Data" processes environmental data and supplemental indicators from the BLM AIM database. The script re-classifies categorical values to match constraints in the AKVEG database, appends data from supplemental indicators, replaces empty observations with appropriate null values, and performs QA/QC checks. It requires the output from the 32_aim_various_2021_14_soilhorizons.R script. The final output is combined with BLM AIM GMT-2 data that was previously formatted.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(janitor, warn.conflicts = FALSE)
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
                      'OneDrive - University of Alaska', 
                      'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '32_aim_various_2021')
template_folder = path(project_folder, 'Data', 'Data_Entry')
workspace_folder = path(plot_folder, 'working')

# Define credentials folder
credentials_folder = path(project_folder, 
                  'Credentials', 
                  'akveg_public_read')

# Set repository directory
repository = path(drive,
                  root_folder,'Repositories', 'akveg-database')

# Define files ----
# Define input datasets
envr_original_input = path(workspace_folder, 'AIM_Terrestrial_Alaska_Environment.csv')
envr_gmt2_input = path(workspace_folder,"12_environment_aimvarious2021.csv")
suppl_input = path(workspace_folder, 'AIM_Supplemental_Moss.csv')
horizons_input = path(plot_folder, '14_soilhorizons_aimvarious2021.csv')

# Define template file
template_input = path(template_folder, "12_environment.xlsx")

# Define database authentication file
authentication = path(credentials_folder, "authentication_akveg_public_read.csv")

# Define output dataset
envr_output = path(plot_folder, '12_environment_aimvarious2021.csv')

# Connect to AKVEG Database ----

# Import database connection function
connection_script = path(repository,
                         'package_DataProcessing', 
                         'connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Read in data ----
envr_original = read_csv(envr_original_input, show_col_types = FALSE)
envr_gmt2 = read_csv(envr_gmt2_input, show_col_types = FALSE)
suppl_original = read_csv(suppl_input)
horizons_original = read_csv(horizons_input, show_col_types = FALSE)

# Read template file
template = colnames(read_xlsx(path=template_input))

# Read SQL tables
query_site = 'SELECT site_visit.site_visit_code, site_visit.site_code, site.establishing_project_code
FROM site_visit
INNER JOIN site ON site_visit.site_code=site.site_code;'
site_data = as_tibble(dbGetQuery(akveg_connection, query_site))

# Subset data ----
# Drop sites that have already been processed or that were excluded from the AKVEG database

projects_to_process = c('aim_campbell_2018', 
                        'aim_kobuknortheast_2021', 
                        'aim_kobukwest_2021')

envr_subset = envr_original %>%
  rename(site_code = PlotID) %>%
  left_join(site_data, by = 'site_code') %>% # Obtain project code & site visit code
  filter(establishing_project_code %in% projects_to_process) %>%
  select(site_visit_code, everything(), -c(PrimaryKey, ProjectName)) %>% 
  remove_empty(which = "cols")

# Generate values for landscape features ----
# Physiography, geomorphology, macrotopography, microtopography
# Use elevation data and thresholds in data dictionary to determine whether something is a 'hill' or a 'mountain'
# Need guidance on how to convert fields to microtopography. NRCS Manual: https://www.nrcs.usda.gov/sites/default/files/2022-09/SSM-ch2.pdf
envr_subset = envr_subset %>% 
  mutate(geomorphology = case_when(site_code == 'KSNE-002' ~ 'coastal plain', # Review
                                   site_code == 'SMR-001' ~ 'coastal plain',
                                   LandscapeType == "Terrace" & Elevation >= 30 ~ 'hill',
                                   LandscapeType == "Floodplain_Basin" ~ 'floodplain',

                                   LandscapeType == 'AlluvialFan' ~ 'valley, alpine', # Review
                                   LandscapeType == 'Flat_Plain' ~ 'plain',
                                   grepl('Hill', LandscapeType) & Elevation <= 300 ~ 'hill', # Manually checked one entry with elevation value < 30 m
                                   grepl('Hill', LandscapeType) & Elevation > 300 ~ 'mountain',
                                   establishing_project_code == 'aim_campbell_2018' ~ 'intermontane basin',
                                   .default = "NULL")) %>% 
  mutate(macrotopography = case_when(site_code == 'SMR-007' ~ 'polygons low-center',
                                     site_code == 'KSNE-011' ~ 'slope planar',
                                     LandscapeType =="Terrace" ~ "floodplain terrace",
                                     LandscapeType =="AlluvialFan" ~ "alluvial fan",
                                     LandscapeTypeSecondary=="Summit" ~ "summit",
                                     LandscapeTypeSecondary=="Shoulder" ~ "ridge",
                                     Slope > 0 & grepl("^convex", ESD_SlopeShape) ~ "slope convex",
                                     Slope > 0 & grepl("^concave",
                                                                                  ESD_SlopeShape) ~ "slope concave",
                                     Slope > 0 & grepl("^linear",
                                                                                  ESD_SlopeShape) ~ 'slope planar',
                                     establishing_project_code == 'aim_campbell_2018' ~ 'plane', # Review. All slopes zero
                                     Slope == 0 ~ 'plane',
                                     .default = "NULL")) %>% 
  mutate(microtopography = case_when(site_code == 'SMR-007' ~ 'polygonal',
                                     site_code == 'SMR-011' ~ 'tussocks',
                                     site_code == 'SMR-014' ~ 'convex', # Review
                                     .default = "NULL"))  %>% 
  mutate(physiography = case_when(establishing_project_code == "aim_campbell_2018" ~ "lowland",
                                  site_code == 'SMR-014' ~ 'upland',
                                  site_code == 'SMR-007' ~ 'lacustrine', # Review. Comments mention old lake basin
                                  site_code == 'KSNE-017' ~ 'upland',
                                  geomorphology == 'coastal plain' ~ 'coastal', # These sites are <= 3km from the coast
                                  LandscapeType == 'Terrace' & Elevation >= 30 ~ 'upland',
                                  geomorphology == 'mountain' ~ 'alpine',
                                  geomorphology == 'hill' ~ 'upland',
                                  geomorphology == 'plain' & Elevation > 120 ~ 'upland',
                                  .default = "NULL"))

# Generate values for disturbance ----
# Different type of disturbance may be more appropriate for WNTR-197_20210703: inactive gelifluction lobe
# How should we treat the Campbell Tract sites?
envr_subset = envr_subset %>%
  mutate(across(DisturbWildfire:DisturbOther, ~replace_na(., 0))) %>% 
  mutate(sum_disturbances = rowSums(across(DisturbWildfire:DisturbOther))) %>%
  mutate(
    disturbance = case_when(
      site_visit_code == 'KSNE-010_20210715' ~ 'permafrost dynamics',
      site_visit_code == 'UNST-168_20210711' ~ 'permafrost dynamics',
      site_visit_code == 'UNST-166_20210712' ~ 'aeolian process',
      site_visit_code == "MOOSETRACK_DISTURBED_20180717" ~ "trail",
      grepl(pattern = "exclosure",
        x = site_visit_code, ignore.case = TRUE) ~ "structure",
      grepl(pattern = "permafrost",
            x = Comments,
            ignore.case = TRUE) ~ "permafrost dynamics",
      grepl(pattern = "thermokarst",
            x = Comments,
            ignore.case = TRUE) ~ "permafrost dynamics",
      grepl(pattern = "thermokarst",
            x = DisturbOther,
            ignore.case = TRUE) ~ "permafrost dynamics",
      grepl(pattern = "4 wheeler",
            x = ManagementHistory, ignore.case = TRUE) ~ "ATV use",
      grepl(pattern = "fire", 
            x = Comments, ignore.case = TRUE) ~ "fire",
      grepl(pattern = "burrows",
        x = WildlifeUse, ignore.case = TRUE) ~ "wildlife digging",
      grepl(pattern = "graz",
        x = WildlifeUse, ignore.case = TRUE) ~ "wildlife foraging",
      grepl(pattern = "game trails",
        x = WildlifeUse, ignore.case = TRUE) ~ "wildlife trails",
      DisturbMammals == 1 ~ "wildlife foraging",
      grepl('non', ManagementHistory) & 
        OffsiteInfluences == 'none' &
        is.na(DisturbOtherDesc) &
        sum_disturbances == 0 ~ 'none',
      .default = "NULL")
  ) %>%
  mutate(
    disturbance_severity = case_when(
      site_visit_code == "KSNE-007_20210715" ~ "low",
      site_visit_code == 'SMR-012_20210709' ~ 'moderate',
      site_visit_code == 'SMR-009_20210708' ~ 'low',
      site_visit_code == 'KSNE-009_20210716' ~ 'low',
      site_visit_code == 'KSNE-010_20210715' ~ 'low',
      grepl(pattern = "thermokarst",
            x = DisturbOther) ~ 'moderate',
      grepl(pattern = "heavy",
        x = WildlifeUse,
        ignore.case = TRUE) & grepl('wildlife', disturbance) ~ "high",
      grepl(pattern = "burrows",
        x = WildlifeUse,
        ignore.case = TRUE) & grepl('wildlife', disturbance) ~ "low",
      .default = "NULL"
    )
  ) %>%
  mutate(disturbance_time_y = case_when(
    site_visit_code == 'UNST-168_20210711' ~ 0,
    grepl(pattern = "recent",
      x = WildlifeUse,
      ignore.case = TRUE
    ) & grepl(pattern = "wildlife", x = disturbance) ~ 0,
    grepl(pattern = "recent",
          x = DisturbOtherDesc,
          ignore.case = TRUE
    ) & !is.na(disturbance) ~ 0,
    disturbance == "structure" ~ 0,
    disturbance == "ATV use" ~ 0,
    disturbance == 'aeolian process' ~ 0,
    .default = -999
  ))

# Format dominant texture at 40 cm ----
texture_40cm = horizons_original %>% 
  filter(depth_lower >= 40 & texture != "NULL") %>% 
  select(site_visit_code, horizon_order, depth_upper, depth_lower, texture) %>%
  group_by(site_visit_code) %>% 
  arrange(site_visit_code, horizon_order) %>% 
  mutate(min_horizon = min(horizon_order),
         max_horizon = max(horizon_order),
         keep_row = case_when(min_horizon == horizon_order & depth_lower != 40 ~ TRUE,
                              min_horizon == max_horizon & depth_lower == 40 ~ TRUE,
                              max_horizon == horizon_order & depth_upper == 40 ~ TRUE,
                              .default = FALSE)) %>% 
  filter(keep_row == TRUE) %>% 
  select(site_visit_code, texture) %>% 
  rename(dominant_texture_40_cm = texture)

# Combine with environment table
# Ignore GMT-2 sites since those sites have already been formatted, and I don't want to overwrite those values
envr_subset = envr_subset %>%
  left_join(texture_40cm, by='site_visit_code') %>% 
  mutate(dominant_texture_40_cm = case_when(is.na(dominant_texture_40_cm) ~ "NULL",
                                                          .default = dominant_texture_40_cm))

# Format moisture regime ----
unique(envr_subset$Moisture_Regime)

envr_subset = envr_subset %>% 
  mutate(moisture_regime = case_when(Moisture_Regime == 'mesic-hygric' ~ 'hygric-mesic heterogenous',
                                     Moisture_Regime == 'hygric-hydric' ~ 'hydric-hygric heterogenous',
                                     is.na(Moisture_Regime) ~ 'NULL',
                                     .default = Moisture_Regime))

# Format other supplemental indicators ----
# Moss/duff depth, depth to water, and depth to restrictive layer have multipled entries per site
# Take the average of these values
suppl_data = suppl_original %>% 
  filter(!(grepl('GMT', MossActiveLayerPlotKey))) %>% # GMT-2 sites entered separately
  group_by(MossActiveLayerPlotKey) %>% 
  summarize(depth_moss_duff_cm = mean(MossDuff_Thickness_cm, na.rm=TRUE),
            depth_water_cm = mean(Depth_to_Water_cm, na.rm=TRUE),
            depth_restrictive_layer_cm = mean(Depth_Soil_to_Permafrost_cm, na.rm=TRUE)) %>% 
  remove_empty(which="rows", cutoff=0.6) %>%  # Drop rows where all suppl. indicators are empty. Cut-off is strict so using 0.75 would get rid of all sites that have 1 or more missing values
  mutate(restrictive_type = case_when(!is.na(depth_restrictive_layer_cm) ~ "permafrost",
                                      .default = "NULL"),
         across(depth_moss_duff_cm:depth_restrictive_layer_cm, ~ signif(.x, digits = 3)))

envr_subset = envr_subset %>% 
  left_join(suppl_data, join_by("PlotKey" == "MossActiveLayerPlotKey"))

## Ensure all sites were added
which(!(suppl_data$MossActiveLayerPlotKey %in% envr_subset$PlotKey))
envr_subset %>% filter(!is.na(depth_moss_duff_cm)) %>% nrow() == nrow(suppl_data) 

# Generate appropriate null values ----
# restrictive_type, cryoturbation = Is there a way to convert from soil horizons?
envr_subset = envr_subset %>% 
  mutate(microrelief_cm = -999,
         drainage = "NULL",
         depth_moss_duff_cm = case_when(is.na(depth_moss_duff_cm) ~ -999,
                                        .default = depth_moss_duff_cm),
         soil_class = "NULL",
         depth_restrictive_layer_cm = case_when(is.na(depth_restrictive_layer_cm) ~ -999,
                                                .default = depth_restrictive_layer_cm),
         restrictive_type = case_when(is.na(restrictive_type) ~ "NULL",
                                      .default = restrictive_type),
         surface_water = "NULL",
         cryoturbation = "NULL",
         depth_15_percent_coarse_fragments_cm = -999,
         depth_water_cm = case_when(is.na(depth_water_cm) ~ -999,
                                    .default = depth_water_cm))

# Select only columns present in AKVEG template
envr_final = envr_subset %>% 
  select(all_of(template))

# Append existing data ----
# Add GMT-2 data to final environment table
envr_final = bind_rows(envr_final,
                       envr_gmt2)

# Format depth at 15% coarse fragments ----
# In this case, all values for this column were -999 (incl. for GMT-2 sites)
coarse_fragments = horizons_original %>% 
  filter(total_coarse_fragment_percent >= 15) %>% 
  select(site_visit_code, horizon_order, depth_upper, total_coarse_fragment_percent) %>% 
  group_by(site_visit_code) %>% 
  arrange(site_visit_code, horizon_order) %>% 
  mutate(min_horizon = min(horizon_order),
         keep_row = case_when(horizon_order == min_horizon ~ TRUE,
                              .default = FALSE)) %>% 
  filter(keep_row == TRUE) %>% 
  select(site_visit_code, depth_upper) %>% 
  rename(depth_15_percent_coarse_fragments_cm = depth_upper)

# Combine with environment table
# Replace null with -999
envr_final = envr_final %>% 
  select(-depth_15_percent_coarse_fragments_cm) %>% 
  left_join(coarse_fragments, by='site_visit_code') %>% 
  mutate(depth_15_percent_coarse_fragments_cm = case_when(is.na(depth_15_percent_coarse_fragments_cm) ~ -999,
                                                          .default = depth_15_percent_coarse_fragments_cm))

rm(coarse_fragments)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(envr_final, is.na)
    , sum)
)

# Are all values where restrictive depth = -999 associated with a restrictive type of NULL?
envr_final %>% 
  filter(depth_restrictive_layer_cm == -999) %>%
  distinct(restrictive_type)

# Export as CSV ----
write_csv(x=envr_final,
          file=envr_output)

# Clear workspace ----
rm(list=ls())
