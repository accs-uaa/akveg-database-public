# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for USFS Glacier Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-11
# Usage: Must be executed in R version 4.4.0+.
# Description: "Calculate Vegetation Cover for USFS Glacier Data" uses data from surveys conducted by the U.S. Forest Service to calculate vegetation cover. The script standardizes scientific names and adds required metadata fields for inclusion in the AKVEG database.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
template_folder = path(project_folder, 'Data','Data_Entry')
taxonomy_folder = path(project_folder, 'Data','Tables_Taxonomy','USDA_Plants')
plot_folder = path(project_folder, 'Data/Data_Plots/46_usfs_glacier_2023')
workspace_folder = path(plot_folder, 'working')
source_folder = path(plot_folder, 'source')

# Set repository directory
repository = path(drive,root_folder,'Repositories/akveg-database')

# Define input datasets
site_visit_input = path(plot_folder,"03_sitevisit_usfsglacier2023.csv")
vegetation_cover_input = path(source_folder,'GRD_AccessDB_08182023.xlsx')
site_codes_input = path(workspace_folder, 'site_codes_glacier.csv')
template_input = path(template_folder, "05_vegetation_cover.xlsx")
usda_codes_input = path(taxonomy_folder,'USDA_Plants_20240301.csv')

# Define output dataset
vegetation_cover_output = path(plot_folder, '05_vegetationcover_usfsglacier2023.csv')
analysis_output = path(workspace_folder, '05_vegetationcover_analysis_usfsglacier2023.csv')

# Connect to AKVEG Database ----

# Import database connection function
connection_script = path(repository,
                         'package_DataProcessing/connect_database_postgresql.R')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = path("C:/ACCS_Work/Servers_Websites/Credentials/akveg_private_read/authentication_akveg_private.csv")
akveg_connection = connect_database_postgresql(authentication)

# Read in data ----
site_visit_original = read_csv(site_visit_input)
cover_original = read_xlsx(vegetation_cover_input,sheet="11_NRT_DX_OC_PLANT_COVER")
site_codes = read_csv(site_codes_input)
template = colnames(read_xlsx(path=template_input))
usda_codes = read_csv(usda_codes_input)

# Read PostgreSQL tables ----

# Define query
query_all = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
, taxon_level.taxon_level as taxon_level
, taxon_category.taxon_category as taxon_category
, taxon_habit.taxon_habit as taxon_habit
, taxon_accepted.taxon_native as taxon_native
, taxon_accepted.taxon_non_native as taxon_non_native
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
  LEFT JOIN taxon_level ON taxon_accepted.taxon_level_id = taxon_level.taxon_level_id
  LEFT JOIN taxon_hierarchy ON taxon_accepted.taxon_genus_code = taxon_hierarchy.taxon_genus_code
  LEFT JOIN taxon_category ON taxon_hierarchy.taxon_category_id = taxon_category.taxon_category_id
  LEFT JOIN taxon_habit ON taxon_accepted.taxon_habit_id = taxon_habit.taxon_habit_id
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

# Read taxonomy table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_all))

# Format taxonomy tables ----
usda_codes = usda_codes %>%
  mutate(usda_code = case_when(is.na(`Synonym Symbol`) ~ Symbol,
                               !is.na(`Synonym Symbol`) ~ `Synonym Symbol`)) %>% 
  select('usda_code','Scientific Name with Author')

# Obtain site visit codes ----

# Link site visit codes original site codes using site code table
site_visit = site_visit_original %>% 
  left_join(site_codes,by=c("site_code"="new_site_code")) %>% 
  select(original_code,
         site_code,site_visit_code)

# Join site visit code to cover data using "original code" link
cover_data = cover_original %>%
  left_join(site_visit, by=c("SiteID#"="original_code"))

# Drop entries that do not have a Site ID associated with them. As far as I can tell, there is some weird formatting issue in the Excel spreadsheet where someone used a VLOOKUP formula to refer to a Site ID, but we do not have the workbook that the formula points to.
cover_data = cover_data %>% 
  filter(!is.na(site_visit_code))

# Select only relevant columns
# Exclude any column for which all values are empty (NA)
cover_data = cover_data %>%
  select(site_visit_code, PLANT_CODE, LIFE_FORMS, LIVE_DEAD,
         COVER_PERCENT, REMARKS) %>% 
  select_if(function(x){!all(is.na(x))})

# Format dead status ----
unique(cover_data$LIVE_DEAD)

# Convert dead status to Boolean
cover_data = cover_data %>% 
  mutate(dead_status = case_when(LIVE_DEAD == "L" ~ "FALSE",
                                 .default = "ERROR"))

unique(cover_data$dead_status)

# Format percent cover ----

# Change entries listed as 0% to 0.1% (minimum in AKVEG)
cover_data = cover_data %>% 
  mutate(cover_percent = case_when(COVER_PERCENT == 0 ~ 0.1,
                                   .default = COVER_PERCENT))

summary(cover_data$cover_percent)

# Correct species codes ----

# Exclude observations for which life form is "unknown" and plant code is listed as "other"
# Unfortunately, there is no way for us to use these observations in the AKVEG mapping process
cover_data = cover_data %>% 
  mutate(exclude_unknowns = case_when(LIFE_FORMS == "other" & PLANT_CODE == "UNKN" ~ "TRUE",
                                      LIFE_FORMS == "other" & is.na(PLANT_CODE) ~ "TRUE",
                                      is.na(LIFE_FORMS) & PLANT_CODE == "UNKN" ~ "TRUE",
                                      .default = "FALSE")) %>% 
  filter(exclude_unknowns == "FALSE") %>% 
  select(-exclude_unknowns)

# Obtain scientific names by joining cover data with table of USDA plant codes
cover_data = cover_data %>% 
  left_join(usda_codes, by = c("PLANT_CODE" = "usda_code"))

# Convert codes for unknown plants to match names in AKVEG database
# USDA Plant Code CASPP is used for several entries (n = 90); all of them are listed as life form 'graminoid', even though the scientific name for CASPP matches with a non-native forb. I believe this code was used as shorthand for "Carex spp." (unknown Carex).
cover_data = cover_data %>% 
  mutate(`Scientific Name with Author` = case_when(PLANT_CODE == "UNKN" & LIFE_FORMS == "shrubs" ~ "shrub",
                                 grepl(pattern="moss",x=PLANT_CODE,ignore.case=TRUE) ~ "moss",
                                 PLANT_CODE == "other_lichen" ~ "lichen",
                                 PLANT_CODE == "other_gram" ~ "graminoid",
                                 PLANT_CODE == "other_forb" ~ "forb",
                                 LIFE_FORMS == "graminoids" & PLANT_CODE == "UNKN" ~ "graminoid",
                                 LIFE_FORMS == "forbs" & PLANT_CODE == "UNKN" ~ "forb",
                                 LIFE_FORMS == "mosses" & PLANT_CODE == "UNKN" ~ "moss",
                                 LIFE_FORMS == "dwarf_shrub" & PLANT_CODE == "UNKN" ~ 
                                   "shrub dwarf",
                                 LIFE_FORMS == "graminoids" & PLANT_CODE == "CASPP" ~ "Carex",
                                 .default = `Scientific Name with Author`
                                 ))

# Drop observations that are too ambiguous to be attributed to an existing unknown code
cover_data = cover_data %>% 
  filter(!(LIFE_FORMS =="trees" & PLANT_CODE == "UNKN")) %>% 
  filter(!(LIFE_FORMS == "fern_or_ally" & PLANT_CODE == "UNKN")) %>% 
  filter(!(LIFE_FORMS == "fern_or_ally" & PLANT_CODE == "other_fern_or_ally"))

# Ensure that there are no remaining observations that do not match to a USDA code
cover_data %>% 
  filter(is.na(`Scientific Name with Author`)) %>% 
  distinct(PLANT_CODE)

# Obtain accepted taxonomic name ----

# Remove author information from scientific name
cover_data = cover_data %>% 
  mutate(name_original = case_when(grepl(pattern="\\(", 
                                    x = `Scientific Name with Author`) ~ 
                                str_split_i(string=`Scientific Name with Author`,
                                            pattern="\\(", 
                                            i=1),
                              .default = `Scientific Name with Author`)) %>% 
  mutate(name_original = case_when(grepl(pattern="(\\s[A-Z]\\W)", 
                                    x = name_original,
                                    perl=TRUE) ~ 
                                str_split_i(string=name_original,
                                            pattern=regex("(\\s[A-Z]\\W)"), 
                                            i=1),
                              grepl(pattern="(\\s[A-Z])", 
                                    x = name_original,
                                    perl=TRUE) ~ 
                                str_split_i(string=name_original,
                                            pattern=regex("(\\s[A-Z])"), 
                                            i=1),
                              .default = name_original)) %>% 
  mutate(name_original = str_trim(name_original)) %>% 
  mutate(name_original = case_when(grepl(pattern="(\\s[A-Z])", 
                                    x = name_original,
                                    perl=TRUE) ~ 
                                str_split_i(string=name_original,
                                            pattern=regex("(\\s[A-Z])"), 
                                            i=1),
                              .default = name_original))
  

# Obtain accepted taxonomic name by joining with AKVEG comprehensive checklist
cover_data = cover_data %>% 
  left_join(taxa_all,by=c("name_original" = "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# What scientific names did not match with a code in the AKVEG database?
cover_data %>% 
  filter(is.na(taxon_code)) %>% 
  distinct(name_original)

# Drop 1 questionable observation: Tragia glanduligera
cover_data = cover_data %>%
  filter(name_original != "Tragia glanduligera")

# Correct taxon_code and taxon_accepted_code for remaining taxa that didn't return any matches but that can reasonably be attributed to a correct taxon
cover_data = cover_data %>% 
  mutate(name_adjudicated = case_when(name_original == "Vaccinium oxycoccos" ~ 
                                        "Oxycoccus microcarpus",
                                      name_original == "Nuphar lutea" ~ 
                                        "Nuphar polysepala",
                                      .default = name_adjudicated),
         taxon_category = case_when(is.na(taxon_category) ~ "eudicot",
                                    .default = taxon_category),
         taxon_habit = case_when(name_original == "Vaccinium oxycoccos" ~ 
                                   "dwarf shrub",
                                 name_original == "Nuphar lutea" ~ "forb",
                                 .default = taxon_habit),
         taxon_non_native = case_when(is.na(taxon_non_native) ~ FALSE,
                                      .default = taxon_non_native))

# Check that all entries have an adjudicated name
cover_data %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original)

# Summarize cover percent ----

# For each site, group entries with the same taxa/unknown code together and calculate total percent cover
# Thankfully there isn't a case whereby 2 entries have the same name adjudicated but different original names, so you can throw all the columns you want to keep in the group_by function
# Include additional columns (taxon_category, taxon_habit, taxon_non_native) to support USFS CNF Revegetation analysis
summary_cover = cover_data %>% 
  group_by(site_visit_code, name_original, name_adjudicated, 
           taxon_category, taxon_habit, taxon_non_native, dead_status) %>% 
  summarise(cover_percent = sum(cover_percent)) %>% 
  ungroup()

# Correct cover percent ----
# Apply small correction to site_visit_code GRD20210096_20210729 to lower total cover percent by a few percentage points (currently at 109.5%)
summary_cover = summary_cover %>% 
  mutate(cover_percent = case_when(site_visit_code == "GRD20210096_20210729" & name_adjudicated == "Carex pluriflora" ~ 48,
                                   site_visit_code == "GRD20210096_20210729" & cover_percent == 10 ~ 9,
                                   .default = cover_percent))

# Populate remaining columns ----
summary_cover = summary_cover %>%
  mutate(cover_type = "top canopy cover")

# Create dataset for AKVEG export
summary_cover_akveg = summary_cover %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(summary_cover_akveg, is.na)
    , sum)
)

# Ensure cover_percent values are within a reasonable range
summary(summary_cover_akveg$cover_percent) # Minimum should be 0.1

# Calculate sum of top cover by site visit
# Should not be much more than 100%
temp = summary_cover_akveg %>% 
  group_by(site_visit_code) %>% 
  summarize(sum_cover = sum(cover_percent)) %>% 
  arrange(-sum_cover)

# Check values of categorical/Boolean variables
unique(summary_cover_akveg$dead_status) # Should only be FALSE for this dataset
unique(summary_cover_akveg$cover_type) # Should only be top canopy cover

# Check number of unique site visit codes
summary_cover_akveg %>% 
  distinct(site_visit_code) %>% 
  nrow()

# Export as CSV ----
write_csv(summary_cover_akveg, vegetation_cover_output)
write_csv(summary_cover, analysis_output)

# Clear workspace ----
rm(list=ls())