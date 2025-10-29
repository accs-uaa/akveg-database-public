# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# utils.py
# Author: Amanda Droghini
# Last Updated: 2025-10-29
# ---------------------------------------------------------------------------

"""
This module provides a collection of utility functions to support the processing and ingestion of datasets into the
AKVEG Database.

Functions included:
1. filter_sites_in_alaska: Filter plots that aren't within the map boundary (Alaska and adjacent Canada)
and re-project coordinates of included sites to NAD83.
"""

# Import packages
import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
from typing import Union
import os

# Define file path for the map boundary
BOUNDARY_PATH = os.path.join("C:/", "ACCS_Work", "Projects", "AKVEG_Map", "Data", "region_data",
                              "AlaskaYukon_MapDomain_3338.shp")

# --- Function 1 ---
def filter_sites_in_alaska(
        site_df: pl.DataFrame,
        input_crs: str,
        longitude_col: str = "longitude_dd",
        latitude_col: str = "latitude_dd",
) -> Union[pl.DataFrame, None]:
    """
    Filters a Polars DataFrame of sites, keeping only those
    that fall within the map boundary, and re-projects the coordinates to NAD83.

    Args:
        site_df: The input Polars DataFrame containing site coordinates.
        input_crs: The EPSG code (as a string, e.g., "EPSG:4269") of the
                   input latitude and longitude columns.
        longitude_col: The name of the column with longitude.
        latitude_col: The name of the column with latitude.

    Returns:
        A Polars DataFrame containing only the sites inside the boundary with coordinates in NAD83,
        or None if the boundary file cannot be loaded.
    """

    # --- Validate input ---
    if not os.path.exists(BOUNDARY_PATH):
        print(f"ERROR: Map boundary file not found at: {BOUNDARY_PATH}")
        return None

    # --- Setup CRSs ---
    # CRS of map boundary
    TARGET_CRS_INTERSECT = "EPSG:3338"
    # CRS of final output
    TARGET_CRS_NAD83 = "EPSG:4269"

    # 1. Load the region boundary
    try:
        region_boundary = gpd.read_file(BOUNDARY_PATH).to_crs(TARGET_CRS_INTERSECT)
    except Exception as e:
        print(f"ERROR loading or reprojecting region boundary: {e}")
        return None

    # 2. Convert Polars DF to GeoPandas GeoDataFrame
    site_pd = site_df.to_pandas()

    site_spatial = gpd.GeoDataFrame(
        site_pd,
        geometry=gpd.points_from_xy(site_pd[longitude_col], site_pd[latitude_col]),
        crs=input_crs
    )

    print(f"Input CRS of site df: {input_crs}")

    # 3. Project sites to match the region boundary
    site_spatial = site_spatial.to_crs(crs=TARGET_CRS_INTERSECT)

    # 4. Find sites within the map boundary
    ## Spatial query returns an ndarray with shape 2 (input_geometries, tree_geometries). Index
    # 0 will contain row indices for the input geometries (i.e., sites) that had at least one intersection with the
    # region_boundary polygon.
    sites_inside_idx = region_boundary.sindex.query(
        geometry=site_spatial.geometry, predicate="intersects"
    )[0]

    # 5. Filter the original data using the resulting indices
    unique_idx = np.unique(sites_inside_idx)
    sites_inside_gdf = site_spatial.iloc[unique_idx]

    # 6. Log the results
    sites_outside_count = site_df.shape[0] - sites_inside_gdf.shape[0]
    print(f"Total input sites: {site_df.shape[0]}")
    print(f"Sites remaining after filtering (inside boundary): {sites_inside_gdf.shape[0]}")
    print(f"Sites filtered out (outside boundary): {sites_outside_count}")

    # 7. Reproject to NAD83 for Final Output
    sites_inside_nad83 = sites_inside_gdf.to_crs(TARGET_CRS_NAD83)

    # Add new lat/long columns from the reprojected geometry
    sites_inside_nad83['longitude_dd'] = sites_inside_nad83.geometry.x
    sites_inside_nad83['latitude_dd'] = sites_inside_nad83.geometry.y

    # 6. Convert the filtered GeoDataFrame back to a Polars DataFrame
    # Drop the internal geometry column and the intermediate EPSG:3338 geometry
    sites_inside_df = (
        pl.from_pandas(sites_inside_nad83.drop(columns=['geometry']))
        # The original columns, plus the new NAD83 columns, are preserved here
    )

    return sites_inside_df
