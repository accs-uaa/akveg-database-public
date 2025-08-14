# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Extract Shrub Height Data from AIM 2021 Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-02-05
# Usage: Must be executed in R version 4.3.2+.
# Description: "Extract Shrub Height Data from AIM 2021 Data" summarizes data from line-point intercepts to obtain species-specific heights (98th percentile and mean) for each site of interest. The script also formats site codes, merges USDA plant codes with accepted taxonomic names, and corrects errors in the original data. Initial data tables were extracted from BLM's 2022 TerrADat geodatabase in ArcGIS Pro.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----
drive = "D:"
project_folder = path(drive, "ACCS_Work/Projects/VegetationEcology/AKVEG_Database/Data")
plot_folder = path(project_folder,"Data_Plots/32_aim_various_2021")
data_folder = path(plot_folder, "source/extracted_tables")
template_folder = path(project_folder, "Data_Entry")

# Set repository directory
repository = 'C:/ACCS_Work/GitHub/akveg-database-public'

# Define inputs ----
input_cover = path(data_folder, "tbl_lpidetail_2022.csv")
input_projects = path(data_folder, "terradat_2022.csv")
input_template = path(template_folder, "11_shrub_structure.xlsx")
input_sites = path(drive, "ACCS_Work/Projects/VegetationEcology/BLM_NPRA_Benchmarks/temp_files/sites_by_strata.csv") # File from colleague with a list of sites to subset
input_taxonomy = path(project_folder,"Tables_Taxonomy/USDA_Plants/plants_20210611.csv")

# Define outputs ----
output_heights = path(plot_folder, "11_shrubstructure_aimvarious2021.csv")

# Connect to AKVEG PostgreSQL database ----

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connectDatabasePostGreSQL.R',
                          sep = '/')
source(connection_script)

authentication = path(
  "C:/ACCS_Work/Servers_Websites/Credentials/accs-postgresql/authentication_akveg.csv"
)

akveg_connection = connect_database_postgresql(authentication)


# Read in data ----
plot_cover = read_csv(file=input_cover)
projects = read_csv(file=input_projects, col_select=c("PrimaryKey",
                                                      "ProjectName",
                                                      "PlotID",
                                                      "DateVisited"))
sites = read_csv(input_sites)
template = colnames(read_xlsx(path=input_template))
plant_codes = read_csv(input_taxonomy, 
                       col_select=c("code_plants","name_plants"))

# Read PostgreSQL taxonomy tables
query_all = 'SELECT * FROM taxon_all'
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_all))

# Read PostgreSQL vegetation cover table
query_cover = 'SELECT * FROM vegetation_cover'
cover_all = as_tibble(dbGetQuery(akveg_connection, query_cover))

# Read PostgreSQL site visit table
query_visit = 'SELECT site_visit_code, site_code FROM site_visit'
visit_all = as_tibble(dbGetQuery(akveg_connection, query_visit))

# Format taxonomy table ----
taxa_all = taxa_all %>% 
  select(taxon_code,taxon_name)

# Create site_code variable ----

# Join project info to plot_cover info
plot_cover = left_join(plot_cover,projects,by="PrimaryKey") # Can ignore many-to-many warning: both rows refer to sites in WY

# Standardize plot IDs
plot_cover = plot_cover %>%
  mutate(PlotID = case_when(PlotID == "Plot43_CPBWM_2000" ~ "Plot_43_CPBWM_2000",
                            PlotID == "Plot47_CPHCP_2000" ~ "Plot_47_CPHCP_2000",
                            PlotID == "Plot50_AFS_2000" ~ "Plot_50_AFS_2000",
                            PlotID == "plot133-cpbwm_507_1007" ~ "plot_133-cpbwm_507_1007",
                            PlotID == "Plot52_AFS-2000" ~ "Plot_52_AFS-2000",
                            .default = PlotID)) %>% 
  mutate(PlotID = case_when(ProjectName=="Alaska Arctic DO 2019" ~ 
                              str_replace_all(string=PlotID,pattern="-",replacement="_"),
                            .default=PlotID)) %>% 
  mutate(PlotID = case_when(ProjectName=="Alaska Arctic DO 2019" ~ 
                              str_to_upper(string=PlotID),
                            .default=PlotID)) %>% 
  mutate(PlotID = case_when(ProjectName=="ALASKA_GMT2_2021" ~
                              str_pad(PlotID,3,side="left",pad="0"),
                            .default = PlotID))

# Format GMT-2 and Alaska Arctic DO 2019 sites to match plot name conventions
plot_cover = plot_cover %>% 
  mutate(site_code = case_when(ProjectName=="Alaska Arctic DO 2019" & grepl(pattern="PLOT", x=PlotID) ~ 
                            str_c("GMT2",
                                  str_pad(str_split_i(string=PlotID,pattern="_",i=2),3,side="left",pad="0"),
                                  sep = "-"),
                            ProjectName == "ALASKA_GMT2_2021" ~ str_c("GMT2",PlotID,sep="-"),
                            .default = PlotID))

# Restrict data to relevant sites ----

# Select projects to keep
plot_cover = plot_cover %>% 
  filter(site_code %in% sites$site_code) %>% 
  left_join(sites,by="site_code")

# What sites are missing from the TerrADat database?
# No sites missing! #bless
sites %>% 
  filter(!(site_code %in% plot_cover$site_code))

# Obtain site_visit_id ----
# By joining to AKVEG site_visit_table using site as a key
plot_cover = plot_cover %>% 
  left_join(visit_all,by="site_code")

# Did every site code in the plot_cover table find a match in the AKVEG site_visit table?
plot_cover %>% 
  filter(is.na(site_visit_code))

# Format data ----

# Select columns to keep
cols_to_keep = c("site_visit_code",
                 "PointLoc","PointNbr",
                 "HeightWoody","SpeciesWoody",
                 "ChkboxWoody")

# Drop entries for which height is either NA or 0
# Drop data collected on dead vegetation (but keep entries with NA; assume those are alive)
shrub_data = plot_cover %>%
  filter(!(is.na(HeightWoody))) %>% 
  filter(HeightWoody != 0) %>% 
  filter(is.na(ChkboxWoody) | ChkboxWoody != 1) %>% 
  select(all_of(cols_to_keep)) %>% 
  arrange(site_visit_code,PointLoc) 

summary(shrub_data$ChkboxWoody) # Should be no 1s, but still NAs
summary(shrub_data$HeightWoody) # No zeroes, no NAs, values within a reasonable range

# Add shrub class
shrub_data = shrub_data %>% 
  mutate(shrub_class = case_when(HeightWoody >150 ~ "tall",
                                 HeightWoody >20 ~ "low",
                                 .default = "dwarf"))

# Correct taxon names ----

# Correct code for Vaccinium uliginosum (one entry written as VAULM instead of VAUL)
shrub_data = shrub_data %>% 
  mutate(SpeciesWoody = case_when(SpeciesWoody == "VAULM" ~ "VAUL",
            .default = SpeciesWoody))

# Add unknown codes for entries where SpeciesWoody is NA
# Merge with USDA Plants table to obtain species name from species code
shrub_data = shrub_data %>% 
  left_join(plant_codes,
          by=c("SpeciesWoody"="code_plants")) %>% 
  mutate(name_original = case_when(is.na(SpeciesWoody) & shrub_class == "dwarf" ~ "shrub dwarf",
                                   is.na(SpeciesWoody) & shrub_class != "dwarf" ~ "shrub",
                                   .default = name_plants)) %>% 
  select(-c(SpeciesWoody,name_plants))

# Are there any codes that returned a NULL value?
shrub_data %>% 
  filter(is.na(name_original))

# Obtain cover data ----

# Restrict cover data to live plants only
# Correct code is arcalp2
cover_all = cover_all %>%
  filter(dead_status==FALSE) %>% 
  select(site_visit_code,cover_type_id, 
         name_original,code_adjudicated, cover_percent)

# Merge to obtain cover_type & cover_percent
# Will be NA for "unknown shrub" and "unknown dwarf shrub" codes
shrub_data = shrub_data %>% 
  left_join(cover_all,by=c("site_visit_code", "name_original"))

# Which entries didn't return a match?
# Most of these are because there isn't an equivalent entry for that site_visit_code in the cover data (not because the species itself doesn't exist)
names_to_fill = shrub_data %>% 
  filter(is.na(code_adjudicated)) %>%
  distinct(name_original) %>% 
  arrange(name_original) %>% 
  print(n=100)

# Create a unique name_original to code_adjudicated table 
# Obtain multiple codes for Arctous alpina
# Keep the one associated with the accepted name
cover_codes = cover_all %>%
  mutate(name_original = str_to_sentence(cover_all$name_original)) %>% 
  distinct(name_original, code_adjudicated) %>% 
  filter(name_original %in% names_to_fill$name_original) %>% 
  filter(code_adjudicated != "arcalp2" & code_adjudicated != "arcalp3") %>% 
  rename(code_to_replace = code_adjudicated)

shrub_data = shrub_data %>% 
  left_join(cover_codes, by="name_original") %>% 
  mutate(code_adjudicated = case_when(is.na(code_adjudicated)&!is.na(code_to_replace) ~ code_to_replace,
                                .default = code_adjudicated)) %>% 
  select(-code_to_replace)
  

# Only ones that do not have a taxon code are the unknown codes
shrub_data %>% 
  filter(is.na(code_adjudicated)) %>%
  distinct(name_original)

# Fill in the unknown shrub / shrub dwarf with the appropriate taxon codes
shrub_data = shrub_data %>% 
  mutate(code_adjudicated = case_when(name_original == "shrub" ~ "ushrub",
                                      name_original == "shrub dwarf" ~ "ushrdwa",
                                      .default = code_adjudicated))

# Make sure there are no other NAs to address
shrub_data %>% filter(is.na(code_adjudicated))

# Obtain name_adjudicated
# Corrects Dryas octopetala with the appropriate taxon
shrub_data = shrub_data %>% 
  left_join(taxa_all,by=c("code_adjudicated"="taxon_code"))

shrub_data %>% 
  filter(is.na(taxon_name)) %>% 
  distinct(name_original)

# Rename taxon_name column and drop taxon_code column
shrub_data = shrub_data %>% 
  select(-code_adjudicated) %>% 
  rename(name_adjudicated = taxon_name)

# What entries have a name_original that does not match the name_adjudicated?
# Dryas octopetala and Arctostaphylos alpina
# That makes sense since both of those species are now called something else
shrub_data %>% 
  filter(name_original != name_adjudicated) %>% 
  distinct(name_original)

# Summarize heights data ----

# Summarize by species
# Use name_adjudicated in case e.g., there was a site with both Dryas ajanensis and D. hookeriana (name_original = Dryas octopetala for both)
shrub_summary = shrub_data %>% 
  group_by(site_visit_code, name_adjudicated) %>% 
  summarise(max_height = max(HeightWoody),
            mean_height = mean(HeightWoody),
            count = n())

# Are there any NULL values?
shrub_summary %>% filter(is.na(max_height))
shrub_summary %>% filter(is.na(mean_height))

# Convert to long form
shrub_summary = shrub_summary %>% 
  pivot_longer(cols = c("max_height","mean_height"),
               names_to = "height_type",
               values_to = "height_cm") %>% 
  mutate(height_type = case_when(height_type == "max_height" ~ "point-intercept 98th percentile",
                                 height_type == "mean_height" ~ "point-intercept mean"))

# Summarize by site (total for all shrub species)
shrub_totals = shrub_data %>% 
  group_by(site_visit_code) %>% 
  summarise(max_height = max(HeightWoody),
            mean_height = mean(HeightWoody),
            count = n())

# Convert to long form
shrub_totals = shrub_totals %>% 
  pivot_longer(cols = c("max_height","mean_height"),
               names_to = "height_type",
               values_to = "height_cm") %>% 
  mutate(height_type = case_when(height_type == "max_height" ~ "point-intercept 98th percentile",
                                 height_type == "mean_height" ~ "point-intercept mean"))


# Populate missing columns ----

# Collapse shrub data to unique site visit code / species combination
# To obtain cover percent, cover type, etc.
shrub_collapsed = shrub_data %>% 
  select(-c("PointLoc","PointNbr","HeightWoody")) %>% 
  group_by(site_visit_code) %>% 
  distinct(name_adjudicated, .keep_all = TRUE)

# Merge with shrub_summary data frame
shrub_summary = shrub_summary %>% 
  left_join(shrub_collapsed, by=c("site_visit_code","name_adjudicated"))

# Add totals calculation
# Create missing fields
shrub_totals = shrub_totals %>% 
  mutate(name_adjudicated = "shrub",
         shrub_class = "all",
         name_original = "shrub",
         cover_type_id = NA,
         cover_percent = -999)

shrub_summary = shrub_summary %>% 
  bind_rows(shrub_totals) %>% 
  arrange(site_visit_code)

# Complete remaining fields
# Drop individual shrub classes for unknown codes (keep only shrub_class = all)

# Before dropping classes, check to see how many rows you should be left with
shrub_summary %>% filter(name_adjudicated == "shrub dwarf") %>% nrow()
shrub_summary %>% filter(name_adjudicated == "shrub" & shrub_class != "all") %>% nrow()
# Should be dropping 94 rows, remaining dataframe should have 2162

shrub_summary = shrub_summary %>% 
  mutate(cover_type = case_when(cover_type_id==1 ~ "absolute foliar cover",
                                is.na(cover_type_id) ~ "NULL"),
         cover_percent = case_when(is.na(cover_percent) ~ -999,
                                   .default = cover_percent),
         mean_diameter_cm = -999,
         number_stems = -999,
         shrub_subplot_area_m2 = 2827.43,
         height_cm = round(height_cm,digits=2)) %>% 
  filter(name_adjudicated != "shrub dwarf") %>% 
  filter(!(name_adjudicated == "shrub" & shrub_class != "all")) %>% 
  select(all_of(template))

# Check that it worked - should be zero for both of these filter requests
shrub_summary %>% filter(name_adjudicated == "shrub dwarf")
shrub_summary %>% filter(name_adjudicated == "shrub" & shrub_class != "all")

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
# None of the columns should have NAs since NA value is NULL for cover_type and -999 for cover_percent
cbind(
  lapply(
    lapply(shrub_summary, is.na)
    , sum)
)

# Ensure categorical/single values make sense
unique(shrub_summary$shrub_subplot_area_m2) # Should be a single value
unique(shrub_summary$height_type)
unique(shrub_summary$cover_type) # Should be a single value (or NULL)

# Ensure there is no value for cover_percent that is less than 0 and not the null value (-999)
shrub_summary %>% filter(cover_percent < 0) %>% 
  ungroup() %>% 
  distinct(cover_percent)

# Verify range of height values
summary(shrub_summary$height_cm)
hist(shrub_summary$height_cm)
  
# Export data ----
write_csv(shrub_summary, output_heights)

# Clear workspace ----
rm(list=ls())