# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Soil Horizons for 2024 ACCS NWI Southcentral data"
# Author: Amanda Droghini
# Last Updated: 2025-09-25
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Soil Horizons for 2024 ACCS NWI Southcentral data" formats information about soil horizons
# for ingestion into AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT
# statement.
# -------------------

# Import libraries
import polars as pl
import polars.selectors as cs
from pathlib import Path
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database'
plot_folder = project_folder / 'Data' / 'Data_Plots' / '54_accs_nwisouthcentral_2024'
credential_folder = project_folder / "Credentials"

# Define inputs
visit_input = plot_folder / '03_sitevisit_accsnwisouthcentral2024.csv'
horizons_input = plot_folder / 'source' / '14_soilhorizons_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data' / 'Data_Entry' / '14_soil_horizons.xlsx'
akveg_credentials = (credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv")

# Define output
horizons_output = plot_folder / '14_soilhorizons_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input)
visit_original = pl.read_csv(visit_input, columns='site_visit_code')
horizons_original = pl.read_excel(horizons_input,
                                  engine='xlsx2csv',
                                  read_options={'null_values': ["NULL"]})

# Query AKVEG Database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

## Query horizon suffixes
suffix_query = """SELECT soil_horizon_suffix_code
                    FROM soil_horizon_suffix;"""

suffix_list = query_to_dataframe(akveg_db_connection, suffix_query)
suffix_list = pl.from_pandas(suffix_list)['soil_horizon_suffix_code'].to_list()

## Query master horizons
master_query = """SELECT soil_horizon_type_code
                    FROM soil_horizon_type;"""

master_list = query_to_dataframe(akveg_db_connection, master_query)
master_list = pl.from_pandas(master_list)['soil_horizon_type_code'].to_list()

## Query texture
texture_query = """SELECT soil_texture
                    FROM soil_texture;"""

texture_list = query_to_dataframe(akveg_db_connection, texture_query)
texture_list = pl.from_pandas(texture_list)['soil_texture'].to_list()

## Query matrix hue
hue_query = """SELECT soil_hue
                    FROM soil_hue;"""

hue_list = query_to_dataframe(akveg_db_connection, hue_query)
hue_list = pl.from_pandas(hue_list)['soil_hue'].to_list()

## Query non-matrix features
features_query = """SELECT nonmatrix_feature
                    FROM soil_nonmatrix_features;"""

features_list = query_to_dataframe(akveg_db_connection, features_query)
features_list = pl.from_pandas(features_list)['nonmatrix_feature'].to_list()

## Close the database connection
akveg_db_connection.close()

# Ensure that all site visit codes are included in the Site Visit table
print(horizons_original.join(visit_original, on='site_visit_code', how='anti').shape[0])
print(visit_original.join(horizons_original, on='site_visit_code', how='anti').shape[0])

# Drop sites for which all values in all columns except site_visit_code are null
## https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.drop_nulls.html
horizons = horizons_original.filter(~pl.all_horizontal(pl.all().exclude('site_visit_code').is_null()))

# Rename non_matrix columns
rename_columns = [c for c in horizons.columns if 'non_matrix' in c]
rename_map = {c: c.replace('non_matrix', 'nonmatrix') for c in rename_columns}

horizons = horizons.rename(rename_map)

# Correct constrained values

## Correct soil texture
replace_texture = {"sand clay": "sandy clay",
                   "sand loam": "sandy loam",
                   "loam sand": "loamy sand",
                   "silt clay": "silty clay"}

horizons = horizons.with_columns(pl.when(pl.col('texture').str.contains(r"^[^c][a-z]*$"))
                                 .then(pl.col('texture').str.strip_suffix('y'))
                                 .otherwise(pl.col('texture').str.replace_many(replace_texture))
                                 .alias('texture'))

## Correct matrix hue
horizons = horizons.with_columns(pl.col('matrix_hue').str.replace("n_gley", "N").alias('matrix_hue'))

## Correct non-matrix features
horizons = horizons.with_columns(pl.when(pl.col('nonmatrix_feature').str.contains(r"^redox.*$"))
                                 .then(pl.col('nonmatrix_feature').str.strip_suffix('s'))
                                 .otherwise(pl.col('nonmatrix_feature'))
                                 .alias('nonmatrix_feature'))
# Verify constrained values

## Horizon suffixes
suffix_columns = horizons.select(pl.col(r"^horizon_suffix.*$")).columns
unique_suffixes = (horizons.select(pl.col(suffix_columns))
            .unpivot()
            .filter(pl.col("value").is_not_null())
            .select(pl.col("value").unique())
            .to_series()
            .to_list())

## Master horizons
horizon_columns = ['horizon_primary', 'horizon_secondary']
unique_master = (horizons.select(pl.col(horizon_columns))
                .unpivot()
                .filter(pl.col("value").is_not_null())
                .select(pl.col("value").unique())
                .to_series()
                .to_list())


## Verify columns: texture, matrix_hue, nonmatrix_feature
column_names = ['texture', 'matrix_hue', 'nonmatrix_feature']
unique_values = {}

for col_name in column_names:
    unique_values[f'unique_{col_name}'] = (
        horizons.select(pl.col(col_name))
        .filter(pl.col(col_name).is_not_null())
        .unique()
        .to_series()
        .to_list()
    )

## Determine whether any values in horizons df do not exist in constrained values list
data_pairs = [
    (unique_suffixes, suffix_list),
    (unique_master, master_list),
    (unique_values['unique_texture'], texture_list),
    (unique_values['unique_matrix_hue'], hue_list),
    (unique_values['unique_nonmatrix_feature'], features_list)
]

for unique_set, database_values in data_pairs:
    nonmatches = set(unique_set).difference(database_values)
    if (len(nonmatches) > 0):
        print(f'Values not in database: {nonmatches}')
    else:
        print(f'All values present in database.')

# Verify values for numeric columns
numeric_columns = ['horizon_order', 'thickness_cm', 'depth_upper', 'depth_lower', 'clay_percent',
                   'total_coarse_fragment_percent', 'gravel_percent', 'cobble_percent', 'stone_percent',
                   'boulder_percent', 'matrix_value', 'matrix_chroma', 'nonmatrix_value', 'nonmatrix_chroma']

numeric_df = horizons.select(pl.col(numeric_columns))
numeric_df = numeric_df[[s.name for s in numeric_df if not (s.null_count() == numeric_df.height)]]
print(numeric_df.describe())  ## Ensure value and chroma values are between 0-10 and 0-8, respectively
print(horizons['matrix_chroma'].unique())  ## Ensure numbers are integers
# Encode null values
## Process numeric columns first

horizons = horizons.with_columns(pl.col(numeric_columns).fill_null(-999).cast(pl.Float64),
                                 pl.col('depth_extend').fill_null(pl.lit("FALSE")))  ## Assume FALSE: Not in
# permafrost region and no hard rock horizons
# region
horizons = horizons.with_columns(pl.col(pl.Utf8).fill_null(pl.lit("NULL")))  ## Process remaining string columns


# Ensure column names & order match the template
horizons = horizons.select(template.columns)

# Export as CSV
horizons.write_csv(horizons_output)
