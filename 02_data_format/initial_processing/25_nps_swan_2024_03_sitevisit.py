# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 NPS SWAN Site Visit Data
# Author: Amanda Droghini
# Last Updated: 2025-10-28
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 NPS SWAN Site Visit Data" formats information about site visits for ingestion into the
# AKVEG Database. The script formats dates, creates site visit codes,
# re-classifies structural class data, and populates required metadata. The output is a CSV
# table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries
import polars as pl
from pathlib import Path

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '25_nps_swan_2024'

# Define inputs
template_input = project_folder / 'Data_Entry' / '03_site_visit.xlsx'
site_input = plot_folder / '02_site_npsswan2024.csv'
visit_input = plot_folder / 'source' / 'SWAN_Vegetation_Database' / 'SWAN_Veg_PlotSample.csv'
vegetation_input = plot_folder / 'source' / 'SWAN_Vegetation_Database' / 'SWAN_Veg_PointIntercept.csv'

# Define output
visit_output = plot_folder / '03_sitevisit_npsswan2024.csv'

# Read in data
template = pl.read_excel(template_input)
site_original = pl.read_csv(site_input, columns='site_code')
visit_original = pl.read_csv(visit_input, columns=['Plot', 'Sample_Date', 'Vegetation_Class_Code',
                                                   'Forest_Type_Code', 'Plot_Sample_Comments'], try_parse_dates=False)
vegetation_original = pl.read_csv(vegetation_input, columns=['Plot', 'Sample_Date'],
                                  try_parse_dates=False,
                                  encoding='utf8-lossy')

# Format date
visit = visit_original.with_columns(observe_date = pl.col("Sample_Date").str.to_date("%m/%d/%Y"))
print(visit.select('observe_date').describe())
print(visit['observe_date'].dt.month().unique())  ## Date range is reasonable

# Create site visit code
visit = visit.with_columns(date_string = pl.col('observe_date').dt.to_string().str.replace_all("-", ""))
visit = visit.with_columns(site_visit_code = pl.concat_str([pl.col('Plot'),pl.col('date_string')], separator="_"))

# Drop plots not included in Site table
visit = visit.join(site_original, how='right', left_on='Plot', right_on='site_code')

# Drop site visits not included in Vegetation Cover table
vegetation = vegetation_original.unique(subset=['Plot', 'Sample_Date'])
vegetation = vegetation.with_columns(observe_date = pl.col('Sample_Date').str.to_date("%m/%d/%Y"))
vegetation = vegetation.with_columns(date_string = pl.col('observe_date').dt.to_string().str.replace_all("-", ""))
vegetation = vegetation.with_columns(site_visit_code = pl.concat_str([pl.col('Plot'),pl.col('date_string')],
                                                                     separator="_"))
vegetation = vegetation.select('site_visit_code')
visit = visit.join(vegetation, how='right', on='site_visit_code')
print(visit.null_count())

# Re-classify structural class data
visit = visit.with_columns(structural_class = pl.col('Vegetation_Class_Code').str.to_lowercase())

replacement_classes = {'alpine':"not available",
                       'null': 'no data',
                       'dwarf birch moraine': 'dwarf shrub',
                       'dwarf shrub tundra': 'dwarf shrub',
                       'young open spruce': 'dwarf shrub',
                       "treeline spruce": "dwarf shrub",
                       'beetle kill spruce': "needleleaf forest",
                       'mature closed spruce': 'needleleaf forest',
                       'mature open spruce': "needleleaf forest",
                       "spruce woodland": "needleleaf forest",
                       "young closed spruce": "needleleaf forest"
                       }

visit = visit.with_columns(pl.col("structural_class").replace(replacement_classes))

# Populate remaining columns
visit = visit.with_columns(project_code = pl.lit('nps_swan_2024'),
                           data_tier = pl.lit('vegetation classification'),
                           veg_observer = pl.lit('unknown'),
                           veg_recorder = pl.lit('unknown'),
                           env_observer = pl.lit('unknown'),
                           soils_observer = pl.lit('none'),
                           scope_vascular = pl.lit('exhaustive'),
                           scope_bryophyte = pl.lit('category'),
                           scope_lichen = pl.lit('category'),
                           homogeneous = pl.lit('TRUE')
)

# Reorder/drop columns to match data entry template
visit = visit[template.columns]

# Export to CSV
visit.write_csv(visit_output)
