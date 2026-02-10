import polars as pl

def generate_taxon_codes(taxon_df: pl.DataFrame) -> pl.DataFrame:
    """
    Takes a dataframe with a 'taxon_name' column and generates
    a 'taxon_code' based on hierarchical taxonomic rules.
    """

    processed_df = (taxon_df.with_columns(
        pl.col('taxon_name')
        .str.to_lowercase()
        .str.splitn(by=" ", n=4)
        .struct.rename_fields(["genus", "species", "infratype", "infraspecies"])
        .alias("taxon_split")
    )
    .with_columns(
        genus6 = pl.col("taxon_split").struct.field("genus").str.slice(0, 6),
        genus3 = pl.col("taxon_split").struct.field("genus").str.slice(0, 3),
        species3 = pl.col("taxon_split").struct.field("species").str.slice(0, 3),
        infratype1 = pl.col("taxon_split").struct.field("infratype").str.slice(0, 1),
        infraspecies3 = pl.col("taxon_split").struct.field("infraspecies").str.slice(0, 3)
    )
    .with_columns(pl.when(pl.col('species3').is_null())
                  .then(pl.col('genus6'))
                  .otherwise(pl.coalesce(pl.concat_str(['genus3', 'species3','infratype1','infraspecies3']),
                                         pl.concat_str(['genus3', 'species3']))).alias('taxon_code'))
    .drop(["taxon_split", "genus6", "genus3", "species3", "infratype1", "infraspecies3"])
                    )

    return processed_df

