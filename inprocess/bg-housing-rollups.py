## ZCTA TO STATE; 
## CORE ADAPTED FROM 'zcta-to-county.r'

import polars as pl
import duckdb
from pathlib import Path
import os
from dotenv import load_dotenv

# Set working directory
os.chdir("/home/joel")

# Load environment variables
load_dotenv()

### DUCKDB CONNECTION

conh = duckdb.connect('./data/plhousing.duckdb')
print(conh.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

conmeta = duckdb.connect('./data/duckdb_metadata.duckdb')
print(conmeta.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

congref = duckdb.connect('./data/georeference.duckdb', read_only=False)
print(congref.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

congeo = duckdb.connect(
    './data/spatial_storage.duckdb',
    read_only=False
)
congeo.execute("INSTALL spatial; LOAD spatial;")

print(congeo.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

metadata = pl.from_pandas(conmeta.execute("SELECT * FROM column_metadata").df())
tables = pl.from_pandas(conmeta.execute("SELECT * FROM table_summary").df())

# Load block groups
hu_block_group = pl.from_pandas(conh.execute("SELECT * FROM hu_block_group").df())

# Match block groups to cbsa via county
county_to_cbsa = pl.read_csv(
    "./data/county-cbsa-lookup.csv",
    dtypes={"county": pl.Utf8},
    encoding="latin1"
)

## Write to georeference duckdb
congref.execute("DROP TABLE IF EXISTS county_to_cbsa")
congref.register("county_to_cbsa", county_to_cbsa)
congref.execute("CREATE TABLE county_to_cbsa AS SELECT * FROM county_to_cbsa")
congref.unregister("county_to_cbsa")

### THIS IS A 1:1 ZCTA TO STATE CROSSWALK; WE WILL USE THIS TO ROLLUP 
### FOR BENCHMARKS OF ZCTAS COMPARED TO PARENT STATE, AND THE US

## Get hu_state, hu_zcta, etc.

hu_block_group = pl.from_pandas(conh.execute("SELECT * FROM hu_block_group").df())
hu_county = pl.from_pandas(conh.execute("SELECT * FROM hu_county").df())
hu_cbsa = pl.from_pandas(conh.execute("SELECT * FROM hu_cbsa").df())
hu_state = pl.from_pandas(conh.execute("SELECT * FROM hu_state").df())
hu_us = pl.from_pandas(conh.execute("SELECT * FROM hu_us").df())

#################################### COUNTY LEVEL BG ROLLUP #############

county_to_cbsa = pl.from_pandas(congref.execute("SELECT * FROM county_to_cbsa").df())

## Compute county fips
hu_block_group = hu_block_group.with_columns(
    pl.col("block_group").str.slice(0, 5).alias("county_fips")
)

hu_block_group_cbsa = hu_block_group.join(
    county_to_cbsa,
    left_on="county_fips",
    right_on="county",
    how="left"
)

hu_block_group_cbsa = hu_block_group_cbsa.join(
    hu_cbsa,
    on="cbsa23",
    how="left",
    suffix="_cbsa"
)

############### RELATIVE INDEX COMPUTATIONS -- CBSA LEVEL

hu_block_group_cbsa = hu_block_group_cbsa.with_columns([
    ((pl.col("hgi_20_24") - 1) / (pl.col("hgi_20_24_cbsa") - 1) * 100).alias("idx_20_24_cbsa"),
    ((pl.col("hgi_20_25") - 1) / (pl.col("hgi_20_25_cbsa") - 1) * 100).alias("idx_20_25_cbsa"),
    (pl.col("agr_25") / pl.col("agr_25_cbsa") * 100).alias("idx_25_cbsa")
])

## NULL CBSA ARE CONNECTICUT BLOCK GROUPS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR BLOCK GROUP
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_block_group_cbsa = hu_block_group_cbsa.filter(pl.col("block_group").is_not_null())

hu_block_group_cbsa = hu_block_group_cbsa.with_columns(
    pl.when(pl.col("cbsa23").is_null())
    .then(pl.lit("CT000"))
    .otherwise(pl.col("cbsa23"))
    .alias("cbsa23")
)

## TRIM FILE

hu_block_group_cbsa = hu_block_group_cbsa.select([
    "block_group",
    "cbsa23",
    "idx_20_24_cbsa",
    "idx_20_25_cbsa",
    "idx_25_cbsa"
])

#################################### COUNTY LEVEL BG ROLLUP #############

hu_block_group_county = hu_block_group.join(
    hu_county,
    left_on="county_fips",
    right_on="co_fips",
    how="left",
    suffix="_county"
)

################# RELATIVE INDEX COMPUTATIONS -- COUNTY LEVEL

hu_block_group_county = hu_block_group_county.with_columns([
    ((pl.col("hgi_20_24") - 1) / (pl.col("hgi_20_24_county") - 1) * 100).alias("idx_20_24_county"),
    ((pl.col("hgi_20_25") - 1) / (pl.col("hgi_20_25_county") - 1) * 100).alias("idx_20_25_county"),
    (pl.col("agr_25") / pl.col("agr_25_county") * 100).alias("idx_25_county")
])

hu_block_group_county = hu_block_group_county.filter(pl.col("block_group").is_not_null())

## TRIM FILE

hu_block_group_county = hu_block_group_county.select([
    "block_group",
    "county_fips",
    "idx_20_24_county",
    "idx_20_25_county",
    "idx_25_county"
])

################################## STATE LEVEL BG ROLLUP #############

hu_block_group = hu_block_group.with_columns(
    pl.col("block_group").str.slice(0, 2).alias("state_fips")
)

hu_block_group_state = hu_block_group.join(
    hu_state,
    left_on="state_fips",
    right_on="state_code",
    how="left",
    suffix="_state"
)

############### RELATIVE INDEX COMPUTATIONS -- STATE LEVEL

hu_block_group_state = hu_block_group_state.with_columns([
    ((pl.col("hgi_20_24") - 1) / (pl.col("hgi_20_24_state") - 1) * 100).alias("idx_20_24_state"),
    ((pl.col("hgi_20_25") - 1) / (pl.col("hgi_20_25_state") - 1) * 100).alias("idx_20_25_state"),
    (pl.col("agr_25") / pl.col("agr_25_state") * 100).alias("idx_25_state")
])

hu_block_group_state = hu_block_group_state.filter(pl.col("block_group").is_not_null())

## TRIM FILE

hu_block_group_state = hu_block_group_state.select([
    "block_group",
    "state_fips",
    "idx_20_24_state",
    "idx_20_25_state",
    "idx_25_state"
])

################################## US LEVEL BG ROLLUP #############

hu_block_group = hu_block_group.with_columns(pl.lit(1).alias("matchid"))
hu_us = hu_us.with_columns(pl.lit(1).alias("matchid"))

hu_block_group_us = hu_block_group.join(
    hu_us,
    on="matchid",
    how="left",
    suffix="_us"
)

############# RELATIVE INDEX COMPUTATIONS -- US LEVEL

hu_block_group_us = hu_block_group_us.with_columns([
    ((pl.col("hgi_20_24") - 1) / (pl.col("hgi_20_24_us") - 1) * 100).alias("idx_20_24_us"),
    ((pl.col("hgi_20_25") - 1) / (pl.col("hgi_20_25_us") - 1) * 100).alias("idx_20_25_us"),
    (pl.col("agr_25") / pl.col("agr_25_us") * 100).alias("idx_25_us")
])

### FINALLY: COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# Compute percentile column for each growth index
# where the highest percentiles are the highest values
hu_block_group_us = hu_block_group_us.with_columns([
    (pl.col("hgi_20_24").rank(method="min") / pl.col("hgi_20_24").count() * 100)
        .ceil().cast(pl.Int64).alias("pctl_20_24_us"),
    (pl.col("hgi_20_25").rank(method="min") / pl.col("hgi_20_25").count() * 100)
        .ceil().cast(pl.Int64).alias("pctl_20_25_us"),
    (pl.col("agr_25").rank(method="min") / pl.col("agr_25").count() * 100)
        .ceil().cast(pl.Int64).alias("pctl_25_us")
])

hu_block_group_us = hu_block_group_us.select([
    "block_group",
    "idx_20_24_us",
    "idx_20_25_us",
    "idx_25_us",
    "pctl_20_24_us",
    "pctl_20_25_us",
    "pctl_25_us"
])

### WRITE TO DUCKDB

# Merge all indexes together
hu_block_group_indexes = hu_block_group_cbsa.join(
    hu_block_group_county, on="block_group", how="full", coalesce=True
).join(
    hu_block_group_state, on="block_group", how="full", coalesce=True
).join(
    hu_block_group_us, on="block_group", how="full", coalesce=True
)

hu_block_group_indexes = hu_block_group_indexes.filter(pl.col("block_group").is_not_null())

conh.execute("DROP TABLE IF EXISTS hu_block_group_indexes")
conh.register("hu_block_group_indexes", hu_block_group_indexes)
conh.execute("CREATE TABLE hu_block_group_indexes AS SELECT * FROM hu_block_group_indexes")
conh.unregister("hu_block_group_indexes")

#### BLOCK GROUP INDEXES COMPLETE ###############################

print(congeo.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

block_group_names = pl.from_pandas(congeo.execute("SELECT * FROM geo_block_group").df())
block_group_hu = pl.from_pandas(conh.execute("SELECT * FROM hu_block_group").df())
block_group_indexes = pl.from_pandas(conh.execute("SELECT * FROM hu_block_group_indexes").df())

block_group_hu = block_group_names.select(["GEOID", "NAMELSAD", "STATEFP"]).join(
    block_group_hu,
    left_on="GEOID",
    right_on="block_group",
    how="left"
)

block_group_indexes = block_group_hu.join(
    block_group_indexes,
    left_on="GEOID",
    right_on="block_group",
    how="left"
)

# Filter out null HU_20
block_group_indexes = block_group_indexes.filter(pl.col("HU_20").is_not_null())

# Filter for Minnesota (STATEFP == '27')
block_group_mn = block_group_indexes.filter(pl.col("STATEFP") == "27")
print(block_group_mn)

# Close connections
conh.close()
conmeta.close()
congref.close()
congeo.close()

print("Processing complete!")

