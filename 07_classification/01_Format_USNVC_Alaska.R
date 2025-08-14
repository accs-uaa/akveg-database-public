# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format USNVC Hierarchy for Alaska
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2025-05-04
# Usage: Script should be executed in R 4.4.3+.
# Description: "Format USNVC Hierarchy for Alaska" formats the USNVC hierarchy from the national version maintained by NatureServe into a format that can be viewed for Alaska.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)
library(writexl)

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Define version
version = '3.0.3'

# Set root directory
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path('D:', root_folder, 'Projects/VegetationEcology/USNVC/Data', paste('version', version, sep = '_'))

# Define input files
usnvc_input = path(project_folder, 'unprocessed/NVC186d.xlsm')
alliance_input = path(project_folder, 'unprocessed/AKNVC_alliances_3.0.3.xlsx')
association_input = path(project_folder, 'unprocessed/AKNVC_associations_3.0.3.xlsx')

# Define output files
usnvc_output = path(project_folder, 'processed/AKNVC_3.0.3.xlsx')

#### REFORMAT DATA
####------------------------------

# Read USNVC data
usnvc_data = read_xlsx(usnvc_input, sheet = 'Hierarchy') %>%
  # Make programmatically readable column names
  rename(element_short = `Division Code`,
         elcode = ELCODE,
         parent_code = `parent elcode`,
         biome = Biome,
         subbiome = Subbiome,
         formation = Formation,
         macrogroup = macrogroup_name,
         group = group_name,
         element_type = `element type`,
         alliance = `alliance name`,
         association = Filters,
         common_name = `common name`,
         subnations = `Subnations \r\nfor filtering`) %>%
  select(subnations, element_short, scope, level, elcode, parent_code, biome, subbiome, formation,
         division, macrogroup, group, element_type, alliance, association, common_name) %>%
  mutate(subnations = case_when(elcode == 'A3902' ~ 'AK, BC, CA, OR, WA',
                                elcode == 'A3608' ~ 'AK, BC, CA, OR, WA',
                                elcode == 'A3605' ~ 'AK, BC, WA',
                                elcode == 'G855' ~ 'AK, YT',
                                elcode == 'A4458' ~ 'AK, YT',
                                elcode == 'A4457' ~ 'AK, YT',
                                elcode == 'A2126' ~ 'AK, YT',
                                elcode == 'G858' ~ 'AK, BC, NT, YT',
                                elcode == 'A2128' ~ 'AK, YT',
                                elcode == 'A4456' ~ 'AK, BC, NT, YT',
                                elcode == 'A4455' ~ 'AK, BC, NT, YT',
                                elcode == 'A2127' ~ 'AK, YT',
                                elcode == 'A3449' ~ 'AK, AB, BC, MB, NT, NU, SK, YT',
                                TRUE ~ subnations))

# Compile list of associations
association_data = usnvc_data %>%
  filter(level == '8Association') %>%
  filter(grepl('AK', subnations)) %>%
  select(elcode, parent_code, association) %>%
  rename(association_code = elcode,
         alliance_code = parent_code,
         association_name = association) %>%
  mutate(citation_primary = 'USNVC 3.0')
aknvc_data = read_xlsx(association_input, sheet = 'associations') %>%
  filter(alliance_code != 'x' & !is.na(alliance_code)) %>%
  select(association_code, alliance_code, association_name, citation_primary)
association_data = rbind(association_data, aknvc_data)
unassigned_data = read_xlsx(association_input, sheet = 'associations') %>%
  filter(alliance_code == 'x' | is.na(alliance_code)) %>%
  select(type, accs_aggregate, association_code, association_name, citation_primary)

# Compile list of alliances
alliance_data = usnvc_data %>%
  filter(level == '7Alliance') %>%
  filter(grepl('AK', subnations)) %>%
  select(element_short, elcode, parent_code, alliance, common_name, subnations) %>%
  rename(alliance_short = element_short,
         alliance_code = elcode,
         group_code = parent_code,
         alliance_common = common_name,
         alliance_usname = alliance) %>%
  mutate(group_code = case_when(alliance_code == 'A4256' ~ 'G548',
                                alliance_code == 'A4271' ~ 'G1194',
                                TRUE ~ group_code))

# Compile list of groups
group_data = usnvc_data %>%
  filter(level == '6Group') %>%
  filter(grepl('AK', subnations)) %>%
  select(element_short, elcode, parent_code, group, subnations) %>%
  rename(group_short = element_short,
         group_code = elcode,
         macrogroup_code = parent_code) %>%
  mutate(group = str_replace(group, 'North Pacific-Bering', 'Aleutian-Kamchatka'),
         group = str_replace(group, 'Alaskan-Yukon', 'Alaska-Yukon'),
         group = str_replace(group, 'Alaskan Pacific', 'Alaska Pacific'))

# Compile list of macrogroups
macrogroup_data = usnvc_data %>%
  filter(level == '5Macrogroup') %>%
  filter(grepl('AK', subnations)) %>%
  select(element_short, elcode, parent_code, macrogroup, subnations) %>%
  rename(macrogroup_short = element_short,
         macrogroup_code = elcode,
         division_code = parent_code) %>%
  mutate(akveg_biome = case_when(macrogroup_code == 'M024' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M025' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M035' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M058' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M059' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M073' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M081' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M101' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M106' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M109' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M156' ~ '3. Boreal',
                                 macrogroup_code == 'M172' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M173' ~ '4. Arctic',
                                 macrogroup_code == 'M175' ~ '4. Arctic',
                                 macrogroup_code == 'M179' ~ '3. Boreal',
                                 macrogroup_code == 'M299' ~ '3. Boreal',
                                 macrogroup_code == 'M300' ~ '3. Boreal',
                                 macrogroup_code == 'M402' ~ '4. Arctic',
                                 macrogroup_code == 'M403' ~ '4. Arctic',
                                 macrogroup_code == 'M404' ~ '3. Boreal',
                                 macrogroup_code == 'M537' ~ '3. Boreal',
                                 macrogroup_code == 'M539' ~ '2. Northern Subpolar Oceanic',
                                 macrogroup_code == 'M558' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M559' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M560' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M870' ~ '4. Arctic',
                                 macrogroup_code == 'M871' ~ '3. Boreal',
                                 macrogroup_code == 'M876' ~ '3. Boreal',
                                 macrogroup_code == 'M877' ~ '3. Boreal',
                                 macrogroup_code == 'M887' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M893' ~ '1. Alaska Pacific',
                                 macrogroup_code == 'M894' ~ '3. Boreal',
                                 macrogroup_code == 'M895' ~ '3. Boreal',
                                 TRUE ~ 'ERROR')) %>%
  mutate(macrogroup = str_replace(macrogroup, 'North Pacific-Bering', 'Aleutian-Kamchatka'),
         macrogroup = str_replace(macrogroup, 'Alaskan-Yukon', 'Alaska-Yukon'),
         macrogroup = str_replace(macrogroup, 'Alaskan Pacific', 'Alaska Pacific'))

# Compile list of divisions
division_data = usnvc_data %>%
  filter(level == '4Division') %>%
  separate_wider_delim(division, ' ', names = c('division_lead', 'division_name'), too_many = 'merge') %>%
  select(element_short, elcode, parent_code, division_name) %>%
  rename(division_short = element_short,
         division_code = elcode,
         division = division_name,
         formation_code = parent_code)

# Compile list of formations
formation_data = usnvc_data %>%
  filter(level == '3Formation') %>%
  separate_wider_delim(formation, ' ', names = c('formation_lead', 'formation_name'), too_many = 'merge') %>%
  select(element_short, elcode, parent_code, formation_name) %>%
  rename(formatioin_short = element_short,
         formation_code = elcode,
         formation = formation_name,
         subbiome_code = parent_code)

# Compile list of subbiomes
subbiome_data = usnvc_data %>%
  filter(level == '2Subbiome') %>%
  separate_wider_delim(subbiome, ' ', names = c('subbiome_lead', 'subbiome_name'), too_many = 'merge') %>%
  select(element_short, elcode, parent_code, subbiome_name) %>%
  rename(subbiome_short = element_short,
         subbiome_code = elcode,
         subbiome = subbiome_name,
         biome_code = parent_code)

# Compile list of biomes
biome_data = usnvc_data %>%
  filter(level == '1Biome') %>%
  separate_wider_delim(biome, ' ', names = c('biome_lead', 'biome_name'), too_many = 'merge') %>%
  select(element_short, elcode, biome_name) %>%
  rename(biome_short = element_short,
         biome_code = elcode,
         biome = biome_name)

# Compile full list of Alaska upper types
upper_data = macrogroup_data %>%
  left_join(division_data, by = 'division_code') %>%
  left_join(formation_data, by = 'formation_code') %>%
  left_join(subbiome_data, by = 'subbiome_code') %>%
  left_join(biome_data, by = 'biome_code') %>%
  select(biome_code, biome, subbiome_code, subbiome, formation_code, formation, division_short, division_code,
         division, macrogroup_short, macrogroup_code, macrogroup, subnations) %>%
  arrange(macrogroup_short)

# Compile full list of Alaska groups and alliances
# Read AKNVC data
aknvc_data = read_xlsx(alliance_input, sheet = 'alliances') %>%
  select(-group_code, -group)
mid_data = alliance_data %>%
  full_join(group_data, by = 'group_code') %>%
  full_join(macrogroup_data, by = 'macrogroup_code') %>%
  mutate(subnations = case_when(!is.na(subnations.x) ~ subnations.x,
                                is.na(subnations.x) ~ subnations.y,
                                TRUE ~ subnations)) %>%
  mutate(akveg_biome = case_when(group_code == 'G1193' ~ '2. Northern Subpolar Oceanic',
                                 alliance_code == 'A4283' ~ '2. Northern Subpolar Oceanic',
                                 group_code == 'G1207' ~ '2. Northern Subpolar Oceanic',
                                 group_code == 'G1192' ~ '2. Northern Subpolar Oceanic',
                                 alliance_code == 'A2441' ~ '2. Northern Subpolar Oceanic',
                                 TRUE ~ akveg_biome)) %>%
  mutate(floodplain = case_when(group_code == 'G1208' ~ 1,
                                group_code == 'G1210' ~ 1,
                                group_code == 'G1207' ~ 1,
                                group_code == 'G1190' ~ 1,
                                group_code == 'G1206' ~ 1,
                                group_code == 'G548' ~ 1,
                                group_code == 'G1205' ~ 1,
                                group_code == 'G1194' ~ 1,
                                group_code == 'G1197' ~ 1,
                                group_code == 'G1204' ~ 1,
                                TRUE ~ 0)) %>%
  mutate(dominant_lifeform = case_when(group_code == 'G240' ~ 'tree',
                                       group_code == 'G241' ~ 'tree',
                                       group_code == 'G750' ~ 'tree',
                                       group_code == 'G751' ~ 'tree',
                                       group_code == 'G850' ~ 'tree',
                                       group_code == 'G852' ~ 'tree',
                                       group_code == 'G854' ~ 'tree',
                                       group_code == 'G855' ~ 'tree',
                                       group_code == 'G858' ~ 'tree',
                                       group_code == 'G1193' ~ 'herbaceous',
                                       group_code == 'G498' ~ 'herbaceous',
                                       group_code == 'G1190' ~ 'barren',
                                       group_code == 'G1206' ~ 'shrub & graminoid',
                                       group_code == 'G322' ~ 'shrub',
                                       group_code == 'G499' ~ 'herbaceous',
                                       group_code == 'G1191' ~ 'herbaceous',
                                       group_code == 'G1203' ~ 'barren',
                                       group_code == 'G320' ~ 'herbaceous',
                                       group_code == 'G968' ~ 'shrub',
                                       group_code == 'G385' ~ 'algae',
                                       group_code == 'G544' ~ 'freshwater',
                                       group_code == 'G349' ~ 'tree',
                                       group_code == 'G579' ~ 'tree',
                                       group_code == 'G627' ~ 'tree',
                                       group_code == 'G354' ~ 'shrub',
                                       group_code == 'G355' ~ 'herbaceous',
                                       group_code == 'G1198' ~ 'shrub & graminoid',
                                       group_code == 'G1199' ~ 'herbaceous',
                                       group_code == 'G1200' ~ 'herbaceous',
                                       group_code == 'G896' ~ 'shrub',
                                       group_code == 'G897' ~ 'shrub',
                                       group_code == 'G863' ~ 'shrub & graminoid',
                                       group_code == 'G869' ~ 'barren',
                                       group_code == 'G1195' ~ 'tree',
                                       group_code == 'G546' ~ 'tree',
                                       group_code == 'G548' ~ 'tree',
                                       group_code == 'G611' ~ 'barren',
                                       group_code == 'G864' ~ 'shrub & graminoid',
                                       group_code == 'G535' ~ 'herbaceous',
                                       group_code == 'G613' ~ 'shrub',
                                       group_code == 'G747' ~ 'herbaceous',
                                       group_code == 'G785' ~ 'barren',
                                       group_code == 'G867' ~ 'shrub',
                                       group_code == 'G1194' ~ 'shrub & graminoid',
                                       group_code == 'G1196' ~ 'shrub',
                                       group_code == 'G356' ~ 'shrub',
                                       group_code == 'G358' ~ 'herbaceous',
                                       group_code == 'G359' ~ 'shrub & graminoid',
                                       group_code == 'G374' ~ 'shrub & graminoid',
                                       group_code == 'G362' ~ 'shrub',
                                       group_code == 'G860' ~ 'shrub',
                                       group_code == 'G861' ~ 'herbaceous',
                                       group_code == 'G1207' ~ 'shrub & graminoid',
                                       group_code == 'G1208' ~ 'herbaceous',
                                       group_code == 'G1209' ~ 'shrub',
                                       group_code == 'G1210' ~ 'shrub & graminoid',
                                       group_code == 'G284' ~ 'herbaceous',
                                       group_code == 'G610' ~ 'tree',
                                       group_code == 'G285' ~ 'herbaceous',
                                       group_code == 'G1204' ~ 'shrub & graminoid',
                                       group_code == 'G370' ~ 'herbaceous',
                                       group_code == 'G617' ~ 'herbaceous',
                                       group_code == 'G830' ~ 'shrub',
                                       group_code == 'G769' ~ 'freshwater',
                                       group_code == 'G360' ~ 'herbaceous',
                                       group_code == 'G515' ~ 'herbaceous',
                                       group_code == 'G361' ~ 'herbaceous',
                                       group_code == 'G318' ~ 'barren',
                                       group_code == 'G527' ~ 'herbaceous',
                                       group_code == 'G1205' ~ 'shrub & graminoid',
                                       group_code == 'G528' ~ 'herbaceous',
                                       group_code == 'G865' ~ 'shrub',
                                       group_code == 'G866' ~ 'shrub',
                                       group_code == 'G1192' ~ 'barren',
                                       group_code == 'G1197' ~ 'barren',
                                       group_code == 'G822' ~ 'barren',
                                       group_code == 'G554' ~ 'barren',
                                       TRUE ~ 'ERROR')) %>%
  mutate(coastal_saline = case_when(group_code == 'G1193' ~ 1,
                                    group_code == 'G498' ~ 1,
                                    group_code == 'G499' ~ 1,
                                    group_code == 'G385' ~ 1,
                                    group_code == 'G611' ~ 1,
                                    group_code == 'G864' ~ 1,
                                    group_code == 'G535' ~ 1,
                                    group_code == 'G554' ~ 1,
                                    TRUE ~ 0)) %>%
  mutate(wetland = case_when(group_code == 'G852' ~ 1,
                             group_code == 'G854' ~ 1,
                             group_code == 'G1190' ~ 1,
                             group_code == 'G1206' ~ 1,
                             group_code == 'G322' ~ 1,
                             group_code == 'G499' ~ 1,
                             group_code == 'G385' ~ 1,
                             group_code == 'G544' ~ 1,
                             group_code == 'G546' ~ 1,
                             group_code == 'G548' ~ 1,
                             group_code == 'G611' ~ 1,
                             group_code == 'G535' ~ 1,
                             group_code == 'G1194' ~ 1,
                             group_code == 'G1207' ~ 1,
                             group_code == 'G1208' ~ 1,
                             group_code == 'G1209' ~ 1,
                             group_code == 'G1210' ~ 1,
                             group_code == 'G284' ~ 1,
                             group_code == 'G610' ~ 1,
                             group_code == 'G285' ~ 1,
                             group_code == 'G1204' ~ 1,
                             group_code == 'G370' ~ 1,
                             group_code == 'G617' ~ 1,
                             group_code == 'G830' ~ 1,
                             group_code == 'G769' ~ 1,
                             group_code == 'G360' ~ 1,
                             group_code == 'G515' ~ 1,
                             group_code == 'G361' ~ 1,
                             group_code == 'G527' ~ 1,
                             group_code == 'G1205' ~ 1,
                             group_code == 'G528' ~ 1,
                             group_code == 'G865' ~ 1,
                             group_code == 'G866' ~ 1,
                             group_code =='G1197' ~ 1,
                             group_code == 'G554' ~ 1,
                             TRUE ~ 0)) %>%
  mutate(alpine = case_when(group_code == 'G1191' ~ 1,
                            group_code == 'G1203' ~ 1,
                            group_code == 'G320' ~ 1,
                            group_code == 'G968' ~ 1,
                            group_code == 'G613' ~ 1,
                            group_code == 'G747' ~ 1,
                            group_code == 'G785' ~ 1,
                            group_code == 'G867' ~ 1,
                            group_code == 'G527' ~ 1,
                            TRUE ~ 0)) %>%
  left_join(aknvc_data, by = 'alliance_code') %>%
  select(akveg_biome, dominant_lifeform, floodplain, coastal_saline, wetland, alpine,
         macrogroup_short, group_code, group, alliance_code, alliance_akname, subnations,
         status, comment_twn) %>%
  arrange(akveg_biome, macrogroup_short)

# Prepare missing and provisional alliances
aknvc_data = read_xlsx(alliance_input, sheet = 'alliances') %>%
  select(-group)
join_data = mid_data %>%
  distinct(akveg_biome, dominant_lifeform, floodplain, coastal_saline, wetland, alpine,
         macrogroup_short, group_code, group)
provisional_types = aknvc_data %>%
  filter(grepl('ak', alliance_code))
missing_types = aknvc_data %>%
  filter(alliance_code == 'A0000') %>%
  rbind(provisional_types) %>%
  left_join(join_data, by = 'group_code') %>%
  mutate(subnations = 'AK')

# Add missing and provisional alliances
mid_data = mid_data %>%
  rbind(missing_types) %>%
  filter(!is.na(alliance_code)) %>%
  arrange(akveg_biome, macrogroup_short, group_code, desc(alliance_code))
  
# Compile list of associations
lower_data = association_data %>%
  full_join(mid_data, by = 'alliance_code') %>%
  select(akveg_biome, macrogroup_short, group_code, alliance_code, alliance_akname,
         association_code, association_name, citation_primary) %>%
  mutate(association_name = case_when(is.na(association_name) ~ 'Missing Associations',
                                      TRUE ~ association_name)) %>%
  mutate(association_code = case_when(is.na(association_code) ~ 'CEGL000000',
                                      TRUE ~ association_code)) %>%
  mutate(citation_primary = case_when(is.na(citation_primary) ~ 'None',
                                      TRUE ~ citation_primary)) %>%
  filter(alliance_code != 'A0000') %>%
  arrange(akveg_biome, macrogroup_short, group_code, desc(alliance_code))

# Read citation data
citation_data = read_xlsx(association_input, sheet = 'citations') %>%
  select(-notes)

# Write data to excel
export_list = list(upper_levels = upper_data, 
                   mid_levels = mid_data,
                   assctns_assigned = lower_data,
                   assctns_unassigned = unassigned_data,
                   citations = citation_data)
write_xlsx(export_list,
           path = usnvc_output,
           col_names=TRUE,
           format_headers = FALSE)
