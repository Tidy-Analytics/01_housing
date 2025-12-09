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

conh = duckdb.connect('./data/housing.duckdb')
print(conh.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

# Test query
print(conh.execute("SELECT * FROM hu_tract LIMIT 5").df())


### THIS SUPPLIES THE BLOCK TO COUNTY SUBDIVISION, BLOCK TO PLACE, AND BLOCK TO URBAN AREA LOOKUPS

congref = duckdb.connect(
    './data/georeference.duckdb',
    read_only=False
)
congref.execute("INSTALL spatial; LOAD spatial;")

congeo = duckdb.connect(
    './data/spatial_storage.duckdb',
    read_only=False
)
congeo.execute("INSTALL spatial; LOAD spatial;")

print(congeo.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

place_look = pl.from_pandas(
    congref.execute("SELECT geoid, place, placename, county FROM block_place").df()
)

place_unique = place_look.group_by("placename").agg(
    pl.col("place").unique()
)

cousub_look = pl.from_pandas(
    congref.execute(
        "SELECT geoid, cousub20 AS cousub, mcdname, county AS co_fips FROM block_cosub"
    ).df()
)

ua_look = pl.from_pandas(
    congref.execute("SELECT geoid, ua, uaname, county FROM block_ua").df()
)

### END OF BLOCK LOOKUPS ################################################################

# Read county metadata
# use adjacency file for convenience
co_id = pl.from_pandas(
    conh.execute(
        """SELECT * FROM read_csv_auto(
        './data/geo/county-adjacency.txt', header=True, normalize_names=True
        )"""
    ).df()
)

county_ids = co_id.group_by("county_name").agg(
    pl.col("county_geoid").max().alias("fipscode")
)

### LOAD BLOCK MAPPING DATA FOR CONNECTICUT FIPS '09'

ct_remap_id = pl.from_pandas(
    congref.execute(
        """SELECT block_fips_2020, block_fips_2022 FROM read_csv_auto(
        './data/ct-2022blockcrosswalk.csv', header=True, normalize_names=True
        )"""
    ).df()
)

#### BLOCK TO ZCTA RELATIONSHIPS

block_to_zcta = pl.from_pandas(
    congref.execute(
        """SELECT GEOID_TABBLOCK_20 as block_geoid, GEOID_ZCTA5_20 as zcta_20
        FROM read_csv_auto('./data/geo/tab20_zcta520_tabblock20_natl.txt',
        header=True, normalize_names=True)"""
    ).df()
).filter(pl.col("zcta_20").is_not_null())


######################################################################################

# Function to process data for a single state
def state_HU_data(state_code, state_name):
    """Process housing unit data for a single state"""
    
    # Read 2020 data
    HU_20 = pl.from_pandas(
        conh.execute(
            f"""SELECT * FROM read_csv_auto(
                './data/geo/{state_code}_{state_name}_AddressBlockCountList_062022.txt',
                header=True,
                normalize_names=True,
                all_varchar=True)"""
        ).df()
    ).select(["block_geoid", "total_housing_units", "total_group_quarters"])
    
    HU_20 = HU_20.sort("total_housing_units", descending=True)
    
    # Read 2023 data; represents growth April 2020 - November 2023 (published 2024)
    HU_24 = pl.from_pandas(
        conh.execute(
            f"""SELECT * FROM read_csv_auto(
                './data/geo/{state_code}_{state_name}_AddressBlockCountList_072024.txt',
                header=True,
                normalize_names=True,
                all_varchar=True)"""
        ).df()
    ).select(["block_geoid", "total_housing_units", "total_group_quarters"])
    
    HU_24 = HU_24.sort("total_housing_units", descending=True)

    ## LATEST, RELEASED SEPTEMBER 2025
    HU_25 = pl.from_pandas(
        conh.execute(
            f"""SELECT * FROM read_csv_auto(
                './data/geo/{state_code}_{state_name}_AddressBlockCountList_072025.txt',
                header=True,
                normalize_names=True,
                all_varchar=True)"""
        ).df()
    ).select(["block_geoid", "total_housing_units", "total_group_quarters"])
    
    HU_25 = HU_25.sort("total_housing_units", descending=True)

    # Rename columns
    HU_20 = HU_20.rename({"total_housing_units": "HU_20", "total_group_quarters": "gq_20"})
    HU_24 = HU_24.rename({"total_housing_units": "HU_24", "total_group_quarters": "gq_24"})
    HU_25 = HU_25.rename({"total_housing_units": "HU_25", "total_group_quarters": "gq_25"})

    # Filter records where 'block_geoid' contains the exact string "TOTAL"
    HU_20 = HU_20.filter(~pl.col("block_geoid").str.contains("TOTAL"))
    HU_24 = HU_24.filter(~pl.col("block_geoid").str.contains("TOTAL"))
    HU_25 = HU_25.filter(~pl.col("block_geoid").str.contains("TOTAL"))

    # Convert to integer
    HU_20 = HU_20.with_columns([
        pl.col("HU_20").cast(pl.Int64),
        pl.col("gq_20").cast(pl.Int64)
    ])
    HU_24 = HU_24.with_columns([
        pl.col("HU_24").cast(pl.Int64),
        pl.col("gq_24").cast(pl.Int64)
    ])
    HU_25 = HU_25.with_columns([
        pl.col("HU_25").cast(pl.Int64),
        pl.col("gq_25").cast(pl.Int64)
    ])

    # Merge data from all three years
    HU_merged = HU_20.join(HU_24, on="block_geoid", how="full")
    HU_merged = HU_merged.join(HU_25, on="block_geoid", how="full")
    
    # Block aggregation - required because of block-part records
    HU_block = HU_merged.with_columns(
        pl.col("block_geoid").str.slice(0, 15).alias("block_geoid_15")
    ).group_by("block_geoid_15").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_part_recs")
    ]).rename({"block_geoid_15": "block_geoid"})

    # Merge with block_to_zcta
    HU_block = HU_block.join(
        block_to_zcta,
        on="block_geoid",
        how="left"
    )

    HU_block = HU_block.join(
        ct_remap_id,
        left_on="block_geoid",
        right_on="block_fips_2020",
        how="left"
    )
 
    # INSERT AUG 2025:
    # HIERARCHIES FOR COUNTY SUB, PLACE, URBAN AREA

    HU_block = HU_block.join(
        ua_look,
        left_on="block_geoid",
        right_on="geoid",
        how="left"
    )

    HU_block = HU_block.join(
        place_look,
        left_on="block_geoid",
        right_on="geoid",
        how="left",
        suffix="_place"
    )

    HU_block = HU_block.join(
        cousub_look,
        left_on="block_geoid",
        right_on="geoid",
        how="left",
        suffix="_cousub"
    )

    # CONNECTICUT ONLY
    # remap block_geoid to block_fips_2022
    HU_block = HU_block.with_columns(
        pl.when(pl.col("block_geoid").str.slice(0, 2) == "09")
        .then(pl.col("block_fips_2022"))
        .otherwise(pl.col("block_geoid"))
        .alias("block_geoid")
    )

    # Block group level aggregation
    HU_bg = HU_block.with_columns(
        pl.col("block_geoid").str.slice(0, 12).alias("block_group")
    ).group_by("block_group").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Add state information to block group
    HU_bg = HU_bg.with_columns(pl.lit(state_code).alias("state_code"))

    # Tract level aggregation
    HU_tract = HU_block.with_columns(
        pl.col("block_geoid").str.slice(0, 11).alias("tract")
    ).group_by("tract").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Add state information to tract
    HU_tract = HU_tract.with_columns(pl.lit(state_code).alias("state_code"))

    # County level aggregation
    HU_co = HU_block.with_columns(
        pl.col("block_geoid").str.slice(0, 5).alias("co_fips")
    ).group_by("co_fips").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Attach county names
    HU_co = HU_co.join(county_ids, left_on="co_fips", right_on="fipscode", how="left")

    # Add state information
    HU_co = HU_co.with_columns([
        pl.lit(state_code).alias("state_code"),
        pl.lit(state_name).alias("state_name")
    ])

    # ZCTA level aggregation
    HU_zcta = HU_block.group_by("zcta_20").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Add state information to ZCTA
    HU_zcta = HU_zcta.with_columns(pl.lit(state_code).alias("state_code"))

    ## COUSUB ########################################################################

    HU_cousub = HU_block.group_by("cousub").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Add state information to county subdivision
    HU_cousub = HU_cousub.with_columns(pl.lit(state_code).alias("state_code"))

    ## PLACE ########################################################################

    HU_place = HU_block.group_by("place").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])

    # Add state information to place
    HU_place = HU_place.with_columns(pl.lit(state_code).alias("state_code"))

    ## URBAN AREA ########################################################################

    HU_ua = HU_block.group_by("ua").agg([
        pl.col("HU_20").sum(),
        pl.col("gq_20").sum(),
        pl.col("HU_24").sum(),
        pl.col("gq_24").sum(),
        pl.col("HU_25").sum(),
        pl.col("gq_25").sum(),
        pl.count().alias("block_recs")
    ])
    
    # Add state information to urban area
    HU_ua = HU_ua.with_columns(pl.lit(state_code).alias("state_code"))

    # Return all data tables
    return {
        "block": HU_block,
        "block_group": HU_bg,
        "tract": HU_tract,
        "county": HU_co,
        "zcta": HU_zcta,
        "cousub": HU_cousub,
        "place": HU_place,
        "ua": HU_ua
    }


# List of states and their codes
states = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut",
    "10": "Delaware", "11": "DistrictofColumbia", "12": "Florida",
    "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
    "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
    "22": "Louisiana", "23": "Maine", "24": "Maryland", "25": "Massachusetts",
    "26": "Michigan", "27": "Minnesota", "28": "Mississippi",
    "29": "Missouri", "30": "Montana", "31": "Nebraska", "32": "Nevada",
    "33": "NewHampshire", "34": "NewJersey", "35": "NewMexico",
    "36": "NewYork", "37": "NorthCarolina", "38": "NorthDakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
    "44": "RhodeIsland", "45": "SouthCarolina", "46": "SouthDakota",
    "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont",
    "51": "Virginia", "53": "Washington", "54": "WestVirginia",
    "55": "Wisconsin", "56": "Wyoming"
}

# Initialize dictionaries to store results
all_results = {}
block_data = {}
block_group_data = {}
tract_data = {}
county_data = {}
zcta_data = {}
cousub_data = {}
place_data = {}
ua_data = {}

# Process data for each state
for state_code, state_name in states.items():
    print(f"Processing {state_name} ...")
    
    state_results = state_HU_data(state_code, state_name)
    
    all_results[state_name] = state_results
    block_data[state_name] = state_results["block"]
    block_group_data[state_name] = state_results["block_group"]
    tract_data[state_name] = state_results["tract"]
    county_data[state_name] = state_results["county"]
    zcta_data[state_name] = state_results["zcta"]
    cousub_data[state_name] = state_results["cousub"]
    place_data[state_name] = state_results["place"]
    ua_data[state_name] = state_results["ua"]


# Function to calculate ratios and CAGR for housing unit data
def calculate_hu_indices(df):
    """Calculate housing unit growth indices and CAGR"""
    return df.with_columns([
        # 20 TO 24
        (pl.col("HU_24") - pl.col("HU_20")).alias("hg_20_24"),
        (pl.col("HU_24") / pl.col("HU_20")).alias("hgi_20_24"),
        ((pl.col("HU_24") / pl.col("HU_20")) ** (1 / 4.33) - 1).alias("cagr_20_24"),
        # 24 To 25, 1 YEAR; SIMPLIFIED AGR FORMULA
        (pl.col("HU_25") - pl.col("HU_24")).alias("hg_24_25"),
        (pl.col("HU_25") / pl.col("HU_24")).alias("hgi_25"),
        (pl.col("HU_25") / pl.col("HU_24") - 1).alias("agr_25"),
        # TOTAL PERIOD, 4.5 YEARS APRIL 2020 TO NOV 2024
        (pl.col("HU_25") - pl.col("HU_20")).alias("hg_20_25"),
        (pl.col("HU_25") / pl.col("HU_20")).alias("hgi_20_25"),
        ((pl.col("HU_25") / pl.col("HU_20")) ** (1 / 5.33) - 1).alias("cagr_20_25")
    ])


# Combine data for each entity type
combined_block_data = pl.concat(list(block_data.values()))
combined_block_group_data = pl.concat(list(block_group_data.values()))
combined_tract_data = pl.concat(list(tract_data.values()))
combined_county_data = pl.concat(list(county_data.values()))

# For state-spanning layers, aggregate before ratio calculations
combined_zcta_data_raw = pl.concat(list(zcta_data.values()))
combined_cousub_data_raw = pl.concat(list(cousub_data.values()))
combined_place_data_raw = pl.concat(list(place_data.values()))
combined_ua_data_raw = pl.concat(list(ua_data.values()))

# Aggregate ZCTA data by zcta_20 (unique key)
combined_zcta_data = combined_zcta_data_raw.filter(
    pl.col("zcta_20").is_not_null()
).group_by("zcta_20").agg([
    pl.col("HU_20").sum(),
    pl.col("gq_20").sum(),
    pl.col("HU_24").sum(),
    pl.col("gq_24").sum(),
    pl.col("HU_25").sum(),
    pl.col("gq_25").sum(),
    pl.col("block_recs").sum(),
    pl.col("state_code").n_unique().alias("n_states")
])

# Apply ratio and CAGR calculations to combined datasets
combined_block_group_data = calculate_hu_indices(combined_block_group_data)
combined_tract_data = calculate_hu_indices(combined_tract_data)
combined_county_data = calculate_hu_indices(combined_county_data)
combined_zcta_data = calculate_hu_indices(combined_zcta_data)
combined_cousub_data = calculate_hu_indices(combined_cousub_data_raw)
combined_place_data = calculate_hu_indices(combined_place_data_raw)
combined_ua_data = calculate_hu_indices(combined_ua_data_raw)

os.chdir("./data")

# Save combined data to Parquet files (Python equivalent of RDS)
combined_block_data.write_parquet("combined_block_data.parquet")
combined_block_group_data.write_parquet("combined_block_group_data.parquet")
combined_tract_data.write_parquet("combined_tract_data.parquet")
combined_county_data.write_parquet("combined_county_data.parquet")
combined_zcta_data.write_parquet("combined_zcta_data.parquet")
combined_cousub_data.write_parquet("combined_cousub_data.parquet")
combined_place_data.write_parquet("combined_place_data.parquet")
combined_ua_data.write_parquet("combined_ua_data.parquet")

# Save data to DuckDB with new naming convention
conh.execute("DROP TABLE IF EXISTS hu_block")
conh.execute("CREATE TABLE hu_block AS SELECT * FROM combined_block_data")

conh.execute("DROP TABLE IF EXISTS hu_block_group")
conh.execute("CREATE TABLE hu_block_group AS SELECT * FROM combined_block_group_data")

conh.execute("DROP TABLE IF EXISTS hu_tract")
conh.execute("CREATE TABLE hu_tract AS SELECT * FROM combined_tract_data")

conh.execute("DROP TABLE IF EXISTS hu_county")
conh.execute("CREATE TABLE hu_county AS SELECT * FROM combined_county_data")

conh.execute("DROP TABLE IF EXISTS hu_zcta")
conh.execute("CREATE TABLE hu_zcta AS SELECT * FROM combined_zcta_data")

conh.execute("DROP TABLE IF EXISTS hu_cousub")
conh.execute("CREATE TABLE hu_cousub AS SELECT * FROM combined_cousub_data")

conh.execute("DROP TABLE IF EXISTS hu_place")
conh.execute("CREATE TABLE hu_place AS SELECT * FROM combined_place_data")

conh.execute("DROP TABLE IF EXISTS hu_ua")
conh.execute("CREATE TABLE hu_ua AS SELECT * FROM combined_ua_data")

print(conh.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

####################### MAIN SECTION COMPLETE ##########################################################

## ADDENDUM: STATE AND US TOTALS

hu_county = pl.from_pandas(conh.execute("SELECT * FROM hu_county").df())

hu_state = hu_county.group_by(["state_code", "state_name"]).agg([
    pl.col("HU_20").sum(),
    pl.col("gq_20").sum(),
    pl.col("HU_24").sum(),
    pl.col("gq_24").sum(),
    pl.col("HU_25").sum(),
    pl.col("gq_25").sum(),
    pl.col("block_recs").sum()
])

# National total
hu_us = hu_county.select([
    pl.col("HU_20").sum(),
    pl.col("gq_20").sum(),
    pl.col("HU_24").sum(),
    pl.col("gq_24").sum(),
    pl.col("HU_25").sum(),
    pl.col("gq_25").sum(),
    pl.col("block_recs").sum()
])

## COMPUTE PERCENTS AND RATES
hu_state = calculate_hu_indices(hu_state)
hu_us = calculate_hu_indices(hu_us)

# Write hu_state and hu_us to duckdb
conh.execute("DROP TABLE IF EXISTS hu_state")
conh.execute("CREATE TABLE hu_state AS SELECT * FROM hu_state")

conh.execute("DROP TABLE IF EXISTS hu_us")
conh.execute("CREATE TABLE hu_us AS SELECT * FROM hu_us")

hu_us.write_parquet("hu_us.parquet")
hu_state.write_parquet("hu_state.parquet")

############## ADDENDUM 2: CBSA ROLLUP TOTALS FOR DUCK DB

hu_block_group = pl.from_pandas(
    conh.execute("SELECT * FROM hu_block_group").df()
)

hu_block_group = hu_block_group.with_columns(
    pl.col("block_group").str.slice(0, 5).alias("county_fips")
)

county_to_cbsa = pl.read_csv('./data/county-cbsa-lookup.csv', dtypes={"county": pl.Utf8})

hu_block_group = hu_block_group.join(
    county_to_cbsa,
    left_on="county_fips",
    right_on="county",
    how="left"
)

hu_cbsa = hu_block_group.group_by("cbsa23").agg([
    pl.col("HU_20").sum(),
    pl.col("gq_20").sum(),
    pl.col("HU_24").sum(),
    pl.col("gq_24").sum(),
    pl.col("HU_25").sum(),
    pl.col("gq_25").sum(),
    pl.col("block_recs").sum()
])

hu_cbsa = calculate_hu_indices(hu_cbsa)

conh.execute("DROP TABLE IF EXISTS hu_cbsa")
conh.execute("CREATE TABLE hu_cbsa AS SELECT * FROM hu_cbsa")

print(conh.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

hu_cbsa.write_parquet("./data/hu_cbsa.parquet")

## Generate metadata table
metadata = pl.from_pandas(
    conh.execute(
        """SELECT table_name, column_name, data_type, column_default, 
        is_nullable, ordinal_position as cid 
        FROM information_schema.columns 
        WHERE table_schema = 'main'"""
    ).df()
).with_columns(
    pl.lit(None).alias("column_description")
)

# Save metadata table back to DuckDB
conh.execute("DROP TABLE IF EXISTS metadata")
conh.execute("CREATE TABLE metadata AS SELECT * FROM metadata")

# Close the database connection
conh.close()

print("Processing complete!")
