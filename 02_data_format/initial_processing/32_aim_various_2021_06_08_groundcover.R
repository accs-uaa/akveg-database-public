# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Ground Cover for AIM 2021 Data"
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-02-05
# Usage: Must be executed in R version 4.3.2+.
# Description: "Calculate Ground Cover for AIM 2021 Data" uses data from line-point intercept surveys to calculate plot-level percent ground cover for each ground element. The script also appends unique site visit identifiers, performs QA/QC checks to ensure values are within a reasonable range, and enforces formatting to match the AKVEG template.
# ---------------------------------------------------------------------------

# Load packages
library(RPostgres)
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tidyr)
library(tibble)

# Set root directory
drive = 'D:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'Projects/VegetationEcology/AKVEG_Database')
data_folder = path(project_folder, 'Data/Data_Plots/32_aim_various_2021')
source_folder = path(data_folder, 'source')

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Define input datasets
detail_input = path(source_folder, 'AIM_Terrestrial_Alaska_LPI.csv')
usda_input = path(project_folder, 'Data/Tables_Taxonomy/USDA_Plants/plants_20210611.csv')

# Define output datasets
ground_output = path(data_folder, '08_groundcover_aimvarious2021.csv')
abiotic_output = path(data_folder, '06_abiotictopcover_aimvarious2021.csv')

# Read in data
detail_data = read_csv(detail_input)
usda_data = read_csv(usda_input)

#### READ INPUT DATA
####------------------------------

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connect_database_postgresql.R',
                          sep = '/')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = paste(drive,
                       root_folder,
                       'Administrative/Credentials/akveg_private_read/authentication_akveg_private.csv',
                       sep = '/')
akveg_connection = connect_database_postgresql(authentication)

# Read PostgreSQL taxonomy tables
taxa_query = "SELECT taxon_all.taxon_code as taxon_code
  , taxon_all.taxon_name as taxon_name
  , taxon_accepted_name.taxon_name as taxon_name_accepted
  , taxon_habit.taxon_habit as taxon_habit
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_hierarchy ON taxon_accepted.taxon_genus_code = taxon_hierarchy.taxon_genus_code
  LEFT JOIN taxon_habit ON taxon_accepted.taxon_habit_id = taxon_habit.taxon_habit_id;"
query_all = 'SELECT taxon_code, taxon_name FROM taxon_all'
taxa_data = as_tibble(dbGetQuery(akveg_connection, taxa_query))

# Read PostgreSQL site visit table
query_visit = 'SELECT site_visit_code, site_code FROM site_visit'
site_visit_data = as_tibble(dbGetQuery(akveg_connection, query_visit))


#### FORMAT LPI DETAIL TABLE
####------------------------------

# Standardize PlotID
detail_data = detail_data %>%
  mutate(PlotID = case_when(PlotID == "Plot43_CPBWM_2000" ~ "Plot_43_CPBWM_2000",
                            PlotID == "Plot47_CPHCP_2000" ~ "Plot_47_CPHCP_2000",
                            PlotID == "Plot50_AFS_2000" ~ "Plot_50_AFS_2000",
                            PlotID == "plot133-cpbwm_507_1007" ~ "plot_133-cpbwm_507_1007",
                            PlotID == "Plot52_AFS-2000" ~ "Plot_52_AFS-2000",
                            TRUE ~ PlotID)) %>%
  mutate(PlotID = case_when(ProjectName=="Alaska Arctic DO 2019" ~ 
                              str_replace_all(string=PlotID, pattern="-", replacement="_"),
                            TRUE ~ PlotID)) %>% 
  mutate(PlotID = case_when(ProjectName=="Alaska Arctic DO 2019" ~ 
                              str_to_upper(string=PlotID),
                            TRUE ~ PlotID)) %>% 
  mutate(PlotID = case_when(ProjectName=="ALASKA_GMT2_2021" ~
                              str_pad(PlotID, 3, side="left", pad="0"),
                            TRUE ~ PlotID))

# Format GMT-2 and Alaska Arctic DO 2019 sites to match plot name conventions
detail_data = detail_data %>% 
  mutate(site_code = case_when(ProjectName=="Alaska Arctic DO 2019" & grepl(pattern="PLOT", x=PlotID) ~ 
                                 str_c("GMT2",
                                       str_pad(str_split_i(string=PlotID,pattern="_",i=2),3,side="left",pad="0"),
                                       sep = "-"),
                               ProjectName == "ALASKA_GMT2_2021" ~ str_c("GMT2",PlotID,sep="-"),
                               TRUE ~ PlotID))

# Join site visit code and format LPI data
lpi_data = detail_data %>%
  inner_join(site_visit_data, by = 'site_code') %>%
  mutate(point_code = case_when(PointNbr < 10 ~ paste(LineID, '_0', PointNbr, sep=''),
                                PointNbr >= 10 ~ paste(LineID, PointNbr, sep='_'),
                                TRUE ~ 'ERROR')) %>%
  select(site_visit_code, point_code, TopCanopy, Lower1, Lower2, Lower3, Lower4, Lower5, Lower6, Lower7,
         SoilSurface, ChkboxTop, ChkboxLower1, ChkboxLower2, ChkboxLower3, ChkboxLower4, ChkboxLower5, ChkboxLower6,
         ChkboxLower7, ChkboxSoil, DateLoadedInDb) %>%
  mutate(unique_id = paste(site_visit_code, point_code, sep='_')) %>%
  mutate(duplicate = duplicated(unique_id) | duplicated(unique_id, fromLast=TRUE))

# Identify correct record from erroneous duplicate entries
duplicate_data = lpi_data %>%
  filter(duplicate == 'TRUE' & site_visit_code != 'CPHCP-56_20140721') %>%
  mutate(entry_string = paste(unique_id, TopCanopy, Lower1, Lower2, Lower3, Lower4, Lower5,
                              Lower6, Lower7, SoilSurface, ChkboxTop, ChkboxLower1, ChkboxLower2,
                              ChkboxLower3, ChkboxLower4, ChkboxLower5, ChkboxLower6,
                              ChkboxLower7, ChkboxSoil, sep = '_')) %>%
  mutate(match = duplicated(entry_string) | duplicated(entry_string, fromLast=TRUE)) %>%
  filter(match == 'FALSE') %>%
  filter(DateLoadedInDb != '2014-09-01') %>%
  select(-match, -entry_string)

# Replace erroneous duplicate entries
lpi_data = lpi_data %>%
  # Remove erroneous duplicates
  anti_join(duplicate_data, by = 'unique_id') %>%
  # Replace with corrected duplicates
  rbind(duplicate_data) %>%
  # Enforce distinct rows
  distinct(site_visit_code, point_code, TopCanopy, Lower1, Lower2, Lower3, Lower4, Lower5, Lower6, Lower7,
         SoilSurface, ChkboxTop, ChkboxLower1, ChkboxLower2, ChkboxLower3, ChkboxLower4, ChkboxLower5, ChkboxLower6,
         ChkboxLower7, ChkboxSoil) %>%
  rowid_to_column('row_id')

# Check number of points per site
lpi_count = lpi_data %>%
  group_by(site_visit_code) %>%
  summarize(total = n()) %>%
  filter(total != 150)

# Convert LPI hits to long form
hit_data = lpi_data %>%
  select(row_id, site_visit_code, point_code, TopCanopy, Lower1, Lower2, Lower3, Lower4, Lower5,
         Lower6, Lower7, SoilSurface) %>%
  pivot_longer(cols = all_of(c('TopCanopy', 'Lower1', 'Lower2', 'Lower3', 'Lower4',
                               'Lower5', 'Lower6', 'Lower7', 'SoilSurface')), names_to = 'layer') %>%
  mutate(layer = case_when(layer == 'TopCanopy' ~ 'layer1',
                           layer == 'Lower1' ~ 'layer2',
                           layer == 'Lower2' ~ 'layer3',
                           layer == 'Lower3' ~ 'layer4',
                           layer == 'Lower4' ~ 'layer5',
                           layer == 'Lower5' ~ 'layer6',
                           layer == 'Lower6' ~ 'layer7',
                           layer == 'Lower7' ~ 'layer8',
                           layer == 'SoilSurface' ~ 'soil',
                           TRUE ~ '')) %>%
  rename(code_plants = value) %>%
  drop_na()

# Convert LPI checks (dead status) to long form
dead_data = lpi_data %>%
  select(row_id, ChkboxTop, ChkboxLower1, ChkboxLower2, ChkboxLower3, ChkboxLower4,
         ChkboxLower5, ChkboxLower6, ChkboxLower7, ChkboxSoil) %>%
  pivot_longer(cols = all_of(c('ChkboxTop', 'ChkboxLower1', 'ChkboxLower2', 'ChkboxLower3', 'ChkboxLower4', 'ChkboxLower5',
                               'ChkboxLower6', 'ChkboxLower7', 'ChkboxSoil')), names_to = 'layer') %>%
  mutate(layer = case_when(layer == 'ChkboxTop' ~ 'layer1',
                           layer == 'ChkboxLower1' ~ 'layer2',
                           layer == 'ChkboxLower2' ~ 'layer3',
                           layer == 'ChkboxLower3' ~ 'layer4',
                           layer == 'ChkboxLower4' ~ 'layer5',
                           layer == 'ChkboxLower5' ~ 'layer6',
                           layer == 'ChkboxLower6' ~ 'layer7',
                           layer == 'ChkboxLower7' ~ 'layer8',
                           layer == 'ChkboxSoil' ~ 'soil',
                           TRUE ~ '')) %>%
  rename(dead_status = value)

# Combine hits and dead status
long_data = hit_data %>%
  inner_join(dead_data, by = c('row_id' = 'row_id', 'layer' = 'layer'))

#### CREATE GROUND ELEMENTS
####------------------------------

# Append names
long_data = long_data %>%
  filter(code_plants != 'None'
         & code_plants != 'DN'
         & code_plants != 'DS'
         & code_plants != 'HT'
         & code_plants != 'N'
         & code_plants != 'NONE'
         & code_plants != 'NT') %>%
  left_join(usda_data, by = 'code_plants') %>%
  select(-plants_id, -link_plants, -auth_plants, -name_common, -symbol_accepted, -scientific_name_with_author,
         -common_name, -family) %>%
  left_join(taxa_data, by = c('name_plants' = 'taxon_name'))

# Create ground element field
long_data = long_data %>%
  mutate(ground_element = case_when(code_plants == '0' ~ 'OS',
                                code_plants == '2ALGA' ~ 'NV',
                                code_plants == '2FUNGI' ~ 'NV',
                                code_plants == '2LC' ~ 'NV',
                                code_plants == '2LCQ' ~ 'NV',
                                code_plants == '2LICHN' ~ 'NV',
                                code_plants == '2LVRWRT' ~ 'NV',
                                code_plants == '2LW' ~ 'NV',
                                code_plants == '2LWW' ~ 'NV',
                                code_plants == '2MOSS' ~ 'NV',
                                code_plants == 'ALBO86' ~ 'BA',
                                code_plants == 'ALGAE86' ~ 'NV',
                                code_plants == 'AM' ~ 'AL',
                                code_plants == 'ANPI7' ~ 'NV',
                                code_plants == 'ASALA9' ~ 'BA',
                                code_plants == 'BENG91' ~ 'BA',
                                code_plants == 'BR' ~ 'BR',
                                code_plants == 'BRCA70' ~ 'NV',
                                code_plants == 'BY' ~ 'BY',
                                code_plants == 'CAFU86' ~ 'BA',
                                code_plants == 'CALU27' ~ 'BA',
                                code_plants == 'CB' ~ 'CB',
                                code_plants == 'CEIS' ~ 'NV',
                                code_plants == 'CILA86' ~ 'NV',
                                code_plants == 'CLST86' ~ 'NV',
                                code_plants == 'CRUST' ~ 'NV',
                                code_plants == 'CY' ~ 'NV',
                                code_plants == 'D' ~ 'OS',
                                code_plants == 'DESUO86' ~ 'BA',
                                code_plants == 'DRAR86' ~ 'NV',
                                code_plants == 'DRINI9' ~ 'BA',
                                code_plants == 'EL' ~ 'L',
                                code_plants == 'FUNGUS' ~ 'NV',
                                code_plants == 'FUNGUS86' ~ 'NV',
                                code_plants == 'GR' ~ 'GR',
                                code_plants == 'HL' ~ 'L',
                                code_plants == 'L' ~ 'L',
                                code_plants == 'LC' ~ 'NV',
                                code_plants == 'LIVER86' ~ 'NV',
                                code_plants == 'LIVR86' ~ 'NV',
                                code_plants == 'M' ~ 'OS',
                                code_plants == 'MOS205' ~ 'NV',
                                code_plants == 'MOS211' ~ 'NV',
                                code_plants == 'MOS213' ~ 'NV',
                                code_plants == 'MOS214' ~ 'NV',
                                code_plants == 'MOSS' ~ 'NV',
                                code_plants == 'NL' ~ 'L',
                                code_plants == 'O' ~ 'OS',
                                code_plants == 'OROB86' ~ 'BA',
                                code_plants == 'P' ~ 'OS',
                                code_plants == 'PAHU86' ~ 'BA',
                                code_plants == 'PC' ~ 'OS',
                                code_plants == 'PESU' ~ 'BA',
                                code_plants == 'PF01' ~ 'BA',
                                code_plants == 'PF01001' ~ 'BA',
                                code_plants == 'PF04001' ~ 'BA',
                                code_plants == 'PF04002' ~ 'BA',
                                code_plants == 'PF04003' ~ 'BA',
                                code_plants == 'PF04004' ~ 'BA',
                                code_plants == 'PF04005' ~ 'BA',
                                code_plants == 'PF04006' ~ 'BA',
                                code_plants == 'PF04007' ~ 'BA',
                                code_plants == 'PF04008' ~ 'BA',
                                code_plants == 'PF04009' ~ 'BA',
                                code_plants == 'PF04010' ~ 'BA',
                                code_plants == 'PF04011' ~ 'BA',
                                code_plants == 'PF04012' ~ 'BA',
                                code_plants == 'PF05' ~ 'BA',
                                code_plants == 'PG01' ~ 'BA',
                                code_plants == 'PG02' ~ 'BA',
                                code_plants == 'PG04001' ~ 'BA',
                                code_plants == 'PG04002' ~ 'BA',
                                code_plants == 'PG04003' ~ 'BA',
                                code_plants == 'PG04004' ~ 'BA',
                                code_plants == 'PG04005' ~ 'BA',
                                code_plants == 'PG04006' ~ 'BA',
                                code_plants == 'PG04007' ~ 'BA',
                                code_plants == 'PG04008' ~ 'BA',
                                code_plants == 'PG04009' ~ 'BA',
                                code_plants == 'PG04010' ~ 'BA',
                                code_plants == 'PG101' ~ 'BA',
                                code_plants == 'POALA2' ~ 'BA',
                                code_plants == 'PG04001' ~ 'BA',
                                code_plants == 'POAR2R2' ~ 'BA',
                                code_plants == 'POBI' ~ 'BA',
                                code_plants == 'POJUA3' ~ 'BA',
                                code_plants == 'POSUB' ~ 'BA',
                                code_plants == 'PG04001' ~ 'BA',
                                code_plants == 'PT' ~ 'OS',
                                code_plants == 'PG04001' ~ 'BA',
                                code_plants == 'R' ~ 'R',
                                code_plants == 'S' ~ 'MS',
                                code_plants == 'SASP91' ~ 'BA',
                                code_plants == 'SH03001' ~ 'BA',
                                code_plants == 'SH04002' ~ 'BA',
                                code_plants == 'SH04003' ~ 'BA',
                                code_plants == 'SPAL_01' ~ 'NV',
                                code_plants == 'SPAL86' ~ 'NV',
                                code_plants == 'SPCO86' ~ 'NV',
                                code_plants == 'SPMO4' ~ 'NV',
                                code_plants == 'SPHA_06' ~ 'NV',
                                code_plants == 'ST' ~ 'ST',
                                code_plants == 'SU01' ~ 'BA',
                                code_plants == 'SU101' ~ 'BA',
                                code_plants == 'TR01' ~ 'NV',
                                code_plants == 'TR02' ~ 'NV',
                                code_plants == 'TR03' ~ 'NV',
                                code_plants == 'TR04' ~ 'NV',
                                code_plants == 'TR106' ~ 'NV',
                                code_plants == 'TR107' ~ 'NV',
                                code_plants == 'TR108' ~ 'NV',
                                code_plants == 'TR109' ~ 'NV',
                                code_plants == 'TR110' ~ 'NV',
                                code_plants == 'TR111' ~ 'NV',
                                code_plants == 'TR112' ~ 'NV',
                                code_plants == 'TR113' ~ 'NV',
                                code_plants == 'TR114' ~ 'NV',
                                code_plants == 'TR115' ~ 'NV',
                                code_plants == 'TR116' ~ 'NV',
                                code_plants == 'TR117' ~ 'NV',
                                code_plants == 'TR118' ~ 'NV',
                                code_plants == 'TR119' ~ 'NV',
                                code_plants == 'TR120' ~ 'NV',
                                code_plants == 'TR121' ~ 'NV',
                                code_plants == 'TR122' ~ 'NV',
                                code_plants == 'TR123' ~ 'NV',
                                code_plants == 'TR124' ~ 'NV',
                                code_plants == 'TR125' ~ 'NV',
                                code_plants == 'TR126' ~ 'NV',
                                code_plants == 'TR127' ~ 'NV',
                                code_plants == 'TR128' ~ 'NV',
                                code_plants == 'TR129' ~ 'NV',
                                code_plants == 'TR130' ~ 'NV',
                                code_plants == 'TR131' ~ 'NV',
                                code_plants == 'TR132' ~ 'NV',
                                code_plants == 'TR133' ~ 'NV',
                                code_plants == 'TR134' ~ 'NV',
                                code_plants == 'TR135' ~ 'NV',
                                code_plants == 'TR136' ~ 'NV',
                                code_plants == 'TR137' ~ 'NV',
                                code_plants == 'TR138' ~ 'NV',
                                code_plants == 'TR139' ~ 'NV',
                                code_plants == 'TR140' ~ 'NV',
                                code_plants == 'TR141' ~ 'NV',
                                code_plants == 'TR142' ~ 'NV',
                                code_plants == 'TR143' ~ 'NV',
                                code_plants == 'TR144' ~ 'NV',
                                code_plants == 'TR145' ~ 'NV',
                                code_plants == 'UN01001' ~ 'NV',
                                code_plants == 'UN01002' ~ 'NV',
                                code_plants == 'UN02007' ~ 'NV',
                                code_plants == 'UN02009' ~ 'NV',
                                code_plants == 'UN02011' ~ 'NV',
                                code_plants == 'UN02014' ~ 'NV',
                                code_plants == 'UN02015' ~ 'NV',
                                code_plants == 'UN02016' ~ 'NV',
                                code_plants == 'UN02017' ~ 'NV',
                                code_plants == 'UN02018' ~ 'NV',
                                code_plants == 'UN02019' ~ 'NV',
                                code_plants == 'UN02024' ~ 'NV',
                                code_plants == 'UN02029' ~ 'NV',
                                code_plants == 'UN02034' ~ 'NV',
                                code_plants == 'UN03001' ~ 'NV',
                                code_plants == 'UN04001' ~ 'NV',
                                code_plants == 'UN04002' ~ 'NV',
                                code_plants == 'UN04003' ~ 'NV',
                                code_plants == 'UN04004' ~ 'NV',
                                code_plants == 'UN04005' ~ 'NV',
                                code_plants == 'UN04006' ~ 'NV',
                                code_plants == 'UN04007' ~ 'NV',
                                code_plants == 'UN04008' ~ 'NV',
                                code_plants == 'UN04009' ~ 'NV',
                                code_plants == 'UN04010' ~ 'NV',
                                code_plants == 'UN04011' ~ 'NV',
                                code_plants == 'UN04012' ~ 'NV',
                                code_plants == 'UN04013' ~ 'NV',
                                code_plants == 'UN04014' ~ 'NV',
                                code_plants == 'UN04015' ~ 'NV',
                                code_plants == 'UN04016' ~ 'NV',
                                code_plants == 'UN04017' ~ 'NV',
                                code_plants == 'UN04018' ~ 'NV',
                                code_plants == 'UN04019' ~ 'NV',
                                code_plants == 'UN04020' ~ 'NV',
                                code_plants == 'UN04021' ~ 'NV',
                                code_plants == 'UN04022' ~ 'NV',
                                code_plants == 'UN04023' ~ 'NV',
                                code_plants == 'VL' ~ 'L',
                                code_plants == 'W' ~ 'WA',
                                code_plants == 'WA' ~ 'WA',
                                code_plants == 'WL' ~ 'WL',
                                taxon_habit == 'hornwort' ~ 'NV',
                                taxon_habit == 'liverwort' ~ 'NV',
                                taxon_habit == 'moss' ~ 'NV',
                                taxon_habit == 'lichen' ~ 'NV',
                                taxon_habit == 'crust' ~ 'NV',
                                taxon_habit == 'algae' ~ 'NV',
                                taxon_habit == 'cyanobacteria' ~ 'NV',
                                taxon_habit == 'fungus' ~ 'NV',
                                TRUE ~ 'BA')) %>%
  mutate(ground_element = case_when(taxon_habit == 'dwarf shrub, shrub'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'dwarf shrub'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'shrub'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'dwarf shrub, shrub, tree'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'coniferous tree'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'deciduous tree'
                                    & dead_status == 1 ~ 'DS',
                                    taxon_habit == 'shrub, deciduous tree'
                                    & dead_status == 1 ~ 'DS',
                                    TRUE ~ ground_element))

# Produce data to check ground element assignments manually
check_data = long_data %>%
  filter(ground_element == 'vascular basal area') %>%
  filter(is.na(taxon_habit)) %>%
  distinct(ground_element, code_plants)

check_data = long_data %>%
  filter(taxon_habit == 'shrub' & dead_status == 1)

ground_elements = long_data %>%
  distinct(ground_element)

long_count = long_data %>%
  distinct(site_visit_code, point_code) %>%
  group_by(site_visit_code) %>%
  summarize(total = n())

#### CALCULATE GROUND COVER
####------------------------------

# Create wide table of ground element hits
wide_data = long_data %>%
  select(row_id, site_visit_code, point_code, layer, ground_element) %>%
  pivot_wider(names_from = layer, values_from = ground_element) %>%
  mutate(ground_cover = case_when(!is.na(layer5)
                                  & layer5 != 'BA' & layer5 != 'DS' ~ layer5,
                                  !is.na(layer5)
                                  & (layer5 == 'BA' | layer5 == 'DS') ~ soil,
                                  is.na(layer5) & !is.na(layer4)
                                  & layer4 != 'BA' & layer4 != 'DS' ~ layer4,
                                  is.na(layer5) & !is.na(layer4)
                                  & (layer4 == 'BA' | layer4 == 'DS') ~ soil,
                                  is.na(layer5) & is.na(layer4) & !is.na(layer3)
                                  & layer3 != 'BA' & layer3 != 'DS' ~ layer3,
                                  is.na(layer5) & is.na(layer4) & !is.na(layer3)
                                  & (layer3 == 'BA' | layer3 == 'DS') ~ soil,
                                  is.na(layer5) & is.na(layer4) & is.na(layer3) & !is.na(layer2)
                                  & layer2 != 'BA' & layer2 != 'DS' ~ layer2,
                                  is.na(layer5) & is.na(layer4) & is.na(layer3) & !is.na(layer2)
                                  & (layer2 == 'BA' | layer2 == 'DS') ~ soil,
                                  is.na(layer5) & is.na(layer4) & is.na(layer3) & is.na(layer2) & !is.na(layer1)
                                  & layer1 != 'BA' & layer1 != 'DS' ~ layer1,
                                  is.na(layer5) & is.na(layer4) & is.na(layer3) & is.na(layer2) & !is.na(layer1)
                                  & (layer1 == 'BA' | layer1 == 'DS') ~ soil,
                                  is.na(layer5) & is.na(layer4) & is.na(layer3) & is.na(layer2) & is.na(layer1) ~ soil,
                                  is.na(layer1) & !is.na(layer2) & layer2 != 'R' ~ layer2,
                                  is.na(layer1) & layer2 == 'R' ~ soil,
                                  TRUE ~ 'ERROR')) %>%
  mutate(ground_cover = case_when(is.na(soil) & !is.na(layer2) & is.na(layer3) ~ layer2,
                                  (layer1 == 'NV' | layer2 == 'NV'
                                  | layer3 == 'NV' | layer4 == 'NV'
                                  | layer5 == 'NV') & soil != 'BA' ~ 'NV',
                                  soil == 'NV' ~ soil,
                                  soil == 'DS' ~ soil,
                                  soil == 'BA' ~ soil,
                                  TRUE ~ ground_cover)) %>%
  mutate(ground_cover = case_when(ground_cover == 'NV' ~ 'B',
                                  ground_cover == 'BA' ~ 'B',
                                  TRUE ~ ground_cover))

# Calculate ground cover
ground_template = wide_data %>%
  distinct(site_visit_code) %>%
  mutate(AL = 0, B = 0, BR = 0, BY = 0, CB = 0, DS = 0, GR = 0,
         L = 0, MS = 0, OS = 0, ST = 0, WA = 0, WL = 0, R = 0) %>%
  pivot_longer(!site_visit_code, names_to = 'ground_element', values_to = 'ground_cover_percent')
ground_total = wide_data %>%
  select(row_id, site_visit_code, point_code) %>%
  group_by(site_visit_code) %>%
  summarize(total = n())
ground_count = wide_data %>%
  select(row_id, site_visit_code, point_code, ground_cover) %>%
  group_by(site_visit_code, ground_cover) %>%
  summarize(count = n()) %>%
  rename(ground_element = ground_cover)
ground_data = ground_template %>%
  left_join(ground_total, by = 'site_visit_code') %>%
  left_join(ground_count, by = c('site_visit_code' = 'site_visit_code', 'ground_element' = 'ground_element')) %>%
  mutate(count = case_when(is.na(count) ~ 0,
                           TRUE ~ count)) %>%
  mutate(ground_cover_percent = round((count/total) * 100, 3)) %>%
  select(site_visit_code, ground_element, ground_cover_percent) %>%
  mutate(ground_element = case_when(ground_element == 'AL' ~ 'animal litter',
                                    ground_element == 'B' ~ 'biotic',
                                    ground_element == 'BR' ~ 'bedrock (exposed)',
                                    ground_element == 'BY' ~ 'boulder',
                                    ground_element == 'CB' ~ 'cobble',
                                    ground_element == 'DS' ~ 'dead standing woody vegetation',
                                    ground_element == 'GR' ~ 'gravel',
                                    ground_element == 'L' ~ 'litter (< 2 mm)',
                                    ground_element == 'MS' ~ 'mineral soil',
                                    ground_element == 'OS' ~ 'organic soil',
                                    ground_element == 'ST' ~ 'stone',
                                    ground_element == 'WA' ~ 'water',
                                    ground_element == 'WL' ~ 'dead down wood (≥ 2 mm)',
                                    ground_element == 'R' ~ 'rock fragments',
                                    TRUE ~ 'ERROR'))

# Export ground cover
write.csv(ground_data, file = ground_output, fileEncoding = 'UTF-8', row.names = FALSE)

#### CALCULATE ABIOTIC TOP COVER
####------------------------------

# Create wide table of top cover hits
top_data = long_data %>%
  select(row_id, site_visit_code, point_code, layer, ground_element) %>%
  pivot_wider(names_from = layer, values_from = ground_element) %>%
  mutate(top_cover = case_when(is.na(layer1) & !is.na(layer2) & layer2 != 'rock fragments' ~ layer2,
                               is.na(layer1) & layer2 == 'rock fragments' ~ soil,
                               is.na(layer1) & is.na(layer2) ~ soil,
                               !is.na(layer1) ~ layer1,
                               TRUE ~ 'ERROR')) %>%
  mutate(top_cover = case_when(top_cover == 'MS' ~ 'S',
                               top_cover == 'OS' ~ 'S',
                               top_cover == 'GR' ~ 'R',
                               top_cover == 'CB' ~ 'R',
                               top_cover == 'ST' ~ 'R',
                               top_cover == 'BY'  ~ 'R',
                               TRUE ~ top_cover))

# Calculate top cover
top_template = wide_data %>%
  distinct(site_visit_code) %>%
  mutate(AL = 0, BA = 0, BR = 0, DS = 0, L = 0, NV = 0, S = 0, WA = 0, WL = 0, R = 0) %>%
  pivot_longer(!site_visit_code, names_to = 'abiotic_element', values_to = 'abiotic_top_cover_percent')
top_total = top_data %>%
  select(row_id, site_visit_code, point_code) %>%
  group_by(site_visit_code) %>%
  summarize(total = n())
top_count = top_data %>%
  select(row_id, site_visit_code, point_code, top_cover) %>%
  group_by(site_visit_code, top_cover) %>%
  summarize(count = n()) %>%
  rename(abiotic_element = top_cover)
abiotic_data = top_template %>%
  left_join(top_total, by = 'site_visit_code') %>%
  left_join(top_count, by = c('site_visit_code' = 'site_visit_code', 'abiotic_element' = 'abiotic_element')) %>%
  mutate(count = case_when(is.na(count) ~ 0,
                           TRUE ~ count)) %>%
  mutate(abiotic_top_cover_percent = round((count/total) * 100, 3)) %>%
  select(site_visit_code, abiotic_element, abiotic_top_cover_percent) %>%
  filter(abiotic_element != 'BA' & abiotic_element != 'NV') %>%
  mutate(abiotic_element = case_when(abiotic_element == 'AL' ~ 'animal litter',
                                    abiotic_element == 'BR' ~ 'bedrock (exposed)',
                                    abiotic_element == 'DS' ~ 'dead standing woody vegetation',
                                    abiotic_element == 'L' ~ 'litter (< 2 mm)',
                                    abiotic_element == 'S' ~ 'soil',
                                    abiotic_element == 'WA' ~ 'water',
                                    abiotic_element == 'WL' ~ 'dead down wood (≥ 2 mm)',
                                    abiotic_element == 'R' ~ 'rock fragments',
                                    TRUE ~ 'ERROR'))

# Export ground cover
write.csv(abiotic_data, file = abiotic_output, fileEncoding = 'UTF-8', row.names = FALSE)
