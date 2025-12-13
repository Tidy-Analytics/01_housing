library(duckdb)
library(data.table)
library(DBI)
library(dplyr)
#install.packages("reactable")
library(reactable)
library(plotly)
library(classInt)
library(RColorBrewer)
library(dotenv)
library(sf)

setwd("/home/joel")

dotenv::load_dot_env()

### DUCKDB CONNECTION

drv <- duckdb('./data/housing.duckdb')
conh <- dbConnect(drv)
dbListTables(conh)

dbGetQuery(conh, "SELECT * FROM hu_cousub where co_fips = '27053' LIMIT 20")


### THIS SUPPLIES THE BLOCK TO COUNTY SUBDIVISION, BLOCK TO PLACE, AND BLOCK TO URBAN AREA LOOKUPS

congref <- dbConnect(duckdb::duckdb(),
                    dbdir = "./data/georeference.duckdb",
                    read_only = FALSE,
                    extensions = c("spatial"))

congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,
  extensions = c("spatial")
)

dbListTables(congeo)

place_look <- data.table(
  dbGetQuery(congref, "select geoid, place, placename, county from block_place;")
)

place_unique <- place_look[, .(place = unique(place)), by = placename]

cousub_look <- data.table(
  dbGetQuery(congref, "select geoid,  cousub20 AS cousub, 
    mcdname, county as co_fips from block_cosub;")
)

ua_look <- data.table(
  dbGetQuery(congref, "select geoid, ua, uaname, county from block_ua;")
)

### END OF BLOCK LOOKUPS ################################################################

# Read county metadata
# use adjacency file for convenience
co_id <- data.table(dbGetQuery(
  conh, "SELECT * FROM read_csv_auto(
  './data/geo/county-adjacency.txt', header=True, normalize_names=True
  );"
)
)

county_ids <- co_id[, .(fipscode = max(county_geoid)), by = county_name]

### LOAD BLOCK MAPPING DATA FOR CONNECTICUT FIPS '09'

ct_remap_id <- data.table(dbGetQuery(
  congref, "SELECT block_fips_2020, block_fips_2022 FROM read_csv_auto(
  './data/ct-2022blockcrosswalk.csv', header=True, normalize_names=True
  );"
)
)

#### BLOCK TO ZCTA RELATIONSHIPS

block_to_zcta <- data.table(
   dbGetQuery(
     congref, "SELECT GEOID_TABBLOCK_20 as block_geoid, GEOID_ZCTA5_20 as zcta_20
     FROM read_csv_auto('./data/geo/tab20_zcta520_tabblock20_natl.txt',
     header=True, normalize_names=True);"
 )
)[!is.na(zcta_20), ]


# nrow(block_to_zcta[, .N, by = zcta_20])

# block_to_zcta <- merge(
#    block_to_zcta, 
#    ct_remap_id,
#    by.x = "block_geoid",
#    by.y = "block_fips_2020",
#    all.x = TRUE
#  )

# IF STATE IS CONNECTICUT THEN MAP OLD BLOCK ID TO NEW BLOCK ID BASED ON PLANNING REGION
#block_to_zcta[substr(block_geoid, 1, 2) == "09", block_geoid := block_fips_2022]


######################################################################################

# Function to process data for a single state
state_HU_data <- function(state_code, state_name) {
  # Read 2020 data
  HU_20 <- data.table(
    dbGetQuery(
      conh,
      sprintf(
        "SELECT * FROM read_csv_auto(
          './data/geo/%s_%s_AddressBlockCountList_062022.txt',
          header=True,
          normalize_names=True,
          all_varchar=True);",
        state_code, state_name
      )
    )
  )[, .(block_geoid, total_housing_units, total_group_quarters)]
  
  HU_20 <- setorder(HU_20, -total_housing_units)
  
  # Read 2023 data; represents growth April 2020 - November 2023 (published 2024)
  HU_24 <- data.table(
    dbGetQuery(
      conh,
      sprintf(
    "SELECT * FROM read_csv_auto(
    './data/geo/%s_%s_AddressBlockCountList_072024.txt',
    header=True,
    normalize_names=True,
    all_varchar=True);",
    state_code, state_name)))[
      ,
      .(block_geoid, total_housing_units, total_group_quarters)
      ]
  
  HU_24 <- setorder(HU_24, -total_housing_units)

## LATEST, RELEASED SEPTEMBER 2025

  HU_25 <- data.table(
    dbGetQuery(
      conh,
      sprintf(
    "SELECT * FROM read_csv_auto(
    './data/geo/%s_%s_AddressBlockCountList_072025.txt',
    header=True,
    normalize_names=True,
    all_varchar=True);",
    state_code, state_name)))[
      ,
      .(block_geoid, total_housing_units, total_group_quarters)
      ]
  
  HU_25 <- setorder(HU_25, -total_housing_units)

  # Rename columns
  HU_20 <- setnames(HU_20, c("total_housing_units", "total_group_quarters"), c("HU_20", "gq_20"))
  HU_24 <- setnames(HU_24, c("total_housing_units", "total_group_quarters"), c("HU_24", "gq_24"))
  HU_25 <- setnames(HU_25, c("total_housing_units", "total_group_quarters"), c("HU_25", "gq_25"))

  # Filter records where 'block_geoid' contains the exact string "TOTAL"
  HU_20 <- HU_20[!grepl("TOTAL", block_geoid)]
  HU_24 <- HU_24[!grepl("TOTAL", block_geoid)]
  HU_25 <- HU_25[!grepl("TOTAL", block_geoid)]


  # Convert 'HU_20', 'HU_24', 'gq_20', 'gq_25' to integer
  HU_20[, c("HU_20", "gq_20") := lapply(.SD, as.integer), .SDcols = c("HU_20", "gq_20")]
  HU_24[, c("HU_24", "gq_24") := lapply(.SD, as.integer), .SDcols = c("HU_24", "gq_24")]
  HU_25[, c("HU_25", "gq_25") := lapply(.SD, as.integer), .SDcols = c("HU_25", "gq_25")]

  # Merge data from all three years
  HU_merged <- merge(HU_20, HU_24, by = "block_geoid", all = TRUE)
  HU_merged <- merge(HU_merged, HU_25, by = "block_geoid", all = TRUE)
  
  # Block aggregation - required because of block-part records
  HU_block <- HU_merged[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_part_recs = .N)
    ), 
    by = .(block_geoid = substring(block_geoid, 1, 15)),
    .SDcols = -"block_geoid"
  ]

  # IF STATE IS CONNECTICUT THEN MAP OLD BLOCK ID TO NEW BLOCK ID BASED ON PLANNING REGION

  # Merge with block_to_zcta
  HU_block <- merge(
    HU_block,
    block_to_zcta,
    by.x = "block_geoid",
    by.y = "block_geoid",
    all.x = TRUE
  )

  HU_block <- merge(
    HU_block, 
    ct_remap_id,
    by.x = "block_geoid",
    by.y = "block_fips_2020",
    all.x = TRUE
  )
 
  # INSERT AUG 2025:
  # HIERARCHIES FOR COUNTY SUB, PLACE, URBAN AREA

  HU_block <- merge(
    HU_block, 
    ua_look,
    by.x = "block_geoid",
    by.y = "geoid",
    all.x = TRUE
  )

  HU_block <- merge(
    HU_block, 
    place_look,
    by.x = "block_geoid",
    by.y = "geoid",
    all.x = TRUE
  )

  HU_block <- merge(
    HU_block, 
    cousub_look,
    by.x = "block_geoid",
    by.y = "geoid",
    all.x = TRUE
  )

# CONNECTICUT ONLY
# remap block_geoid to block_fips_2022
# this ID is based on new CT planning regions
# this assures match to geo ID in boundary files, 
# which are based on 2022 block IDs
    
  HU_block[substr(block_geoid, 1, 2) == "09", block_geoid := block_fips_2022]

  # Block group level aggregation
  
  
  HU_bg <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ), 
    by = .(block_group = substring(block_geoid, 1, 12)),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # Add state information to block group
  HU_bg[, state_code := state_code]


  # Tract level aggregation
  HU_tract <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    )
    ,
    by = .(tract = substring(block_geoid, 1, 11)),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # Add state information to tract
  HU_tract[, state_code := state_code]

  # County level aggregation
  HU_co <- HU_block[, 
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ), 
    by = .(co_fips = substring(block_geoid, 1, 5)),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # Attach county names
  HU_co <- merge(HU_co, county_ids, by.x = "co_fips", by.y = "fipscode", all.x = TRUE)

  # Add state information
  HU_co[, state_code := state_code]
  HU_co[, state_name := state_name]


  # add zcta level aggregation in same format as other levels
  HU_zcta <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ),
    by = .(zcta_20),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # Add state information to ZCTA
  HU_zcta[, state_code := state_code]

  #################################################################################################

  ## COUSUB ########################################################################

  ## FIX KEY CONSTRUCTION ERROR 12/8/25:
  ## SHOULD BE CO_FIPS + COUSUB NOT COUSUB + STATE FIPS
  ## UNIQUE KEY IS 10 CHARACTERS
  
  HU_cousub  <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ),
    by = .(co_fips, cousub),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # wrong key; added co_fips to aggregation
  # Add state information to county subdivision
  #HU_cousub[, state_code := state_code]
  
  
  ## PLACE ########################################################################

  HU_place  <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ),
    by = .(place),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]

  # Add state information to place
  HU_place[, state_code := state_code]

  ## URBAN AREA  ########################################################################

  HU_ua  <- HU_block[
    ,
    c(
      setNames(lapply(.SD, function(x) sum(x, na.rm = TRUE)), names(.SD)),
      .(block_recs = .N)
    ),
    by = .(ua),
    .SDcols = c("HU_20", "gq_20", "HU_24", "gq_24", "HU_25", "gq_25")
  ]
  # Add state information to urban area
  HU_ua[, state_code := state_code]


  # Return all data tables###################################################################
  list(
    block = HU_block,
    block_group = HU_bg,
    tract = HU_tract,
    county = HU_co,
    zcta = HU_zcta,
    cousub = HU_cousub,
    place = HU_place,
    ua = HU_ua
  )
}

List of states and their codes
states <- list(
  "01" = "Alabama", "02" = "Alaska", "04" = "Arizona", "05" = "Arkansas",
  "06" = "California", "08" = "Colorado", "09" = "Connecticut",
  "10" = "Delaware", "11" = "DistrictofColumbia", "12" = "Florida",
  "13" = "Georgia", "15" = "Hawaii", "16" = "Idaho", "17" = "Illinois",
  "18" = "Indiana", "19" = "Iowa", "20" = "Kansas", "21" = "Kentucky",
  "22" = "Louisiana", "23" = "Maine", "24" = "Maryland", "25" = "Massachusetts",
  "26" = "Michigan", "27" = "Minnesota", "28" = "Mississippi",
  "29" = "Missouri", "30" = "Montana", "31" = "Nebraska", "32" = "Nevada",
  "33" = "NewHampshire", "34" = "NewJersey", "35" = "NewMexico",
  "36" = "NewYork", "37" = "NorthCarolina", "38" = "NorthDakota", "39" = "Ohio",
  "40" = "Oklahoma", "41" = "Oregon", "42" = "Pennsylvania",
  "44" = "RhodeIsland", "45" = "SouthCarolina", "46" = "SouthDakota",
  "47" = "Tennessee", "48" = "Texas", "49" = "Utah", "50" = "Vermont",
  "51" = "Virginia", "53" = "Washington", "54" = "WestVirginia",
  "55" = "Wisconsin", "56" = "Wyoming"
)

# Initialize lists to store results
all_results <- list()
block_data <- list()
block_group_data <- list()
tract_data <- list()
county_data <- list()
zcta_data <- list()
# Initialize lists for cousub, place, and ua
cousub_data <- list()
place_data <- list()
ua_data <- list()

# Process data for each state
for (state_code in names(states)) {
  state_name <- states[[state_code]] 
  cat("Processing", state_name, "...\n")

  state_results <- state_HU_data(state_code, state_name)
  
  all_results[[state_name]] <- state_results
  block_data[[state_name]] <- state_results$block
  block_group_data[[state_name]] <- state_results$block_group
  tract_data[[state_name]] <- state_results$tract
  county_data[[state_name]] <- state_results$county
  zcta_data[[state_name]] <- state_results$zcta
  cousub_data[[state_name]] <- state_results$cousub
  place_data[[state_name]] <- state_results$place
  ua_data[[state_name]] <- state_results$ua
}

# Optimize calculate_hu_indices to do all calculations in one pass
calculate_hu_indices <- function(dt) {
  dt[, `:=`(
    # 20 TO 24
    hg_20_24 = HU_24 - HU_20,
    hgi_20_24 = HU_24 / HU_20,
    cagr_20_24 = ((HU_24 / HU_20)^(1 / 4.25)) - 1,
    # 24 To 25
    hg_24_25 = HU_25 - HU_24,
    hgi_25 = HU_25 / HU_24,
    agr_25 = (HU_25 / HU_24) - 1,
    # TOTAL PERIOD
    hg_20_25 = HU_25 - HU_20,
    hgi_20_25 = HU_25 / HU_20,
    cagr_20_25 = ((HU_25 / HU_20)^(1 / 5.25)) - 1
  )]
  return(dt)
}

# Combine data for each entity type
combined_block_data <- rbindlist(block_data)
combined_block_group_data <- rbindlist(block_group_data)
combined_tract_data <- rbindlist(tract_data)
combined_county_data <- rbindlist(county_data)

# For state-spanning layers, aggregate before ratio calculations
combined_zcta_data_raw <- rbindlist(zcta_data)
combined_cousub_data_raw <- rbindlist(cousub_data)
combined_place_data_raw <- rbindlist(place_data)
combined_ua_data_raw <- rbindlist(ua_data)

# Aggregate ZCTA data by zcta_20 (unique key)
combined_zcta_data <- combined_zcta_data_raw[
  !is.na(zcta_20),
  .(
    HU_20 = sum(HU_20, na.rm = TRUE),
    gq_20 = sum(gq_20, na.rm = TRUE),
    HU_24 = sum(HU_24, na.rm = TRUE),
    gq_24 = sum(gq_24, na.rm = TRUE),
    HU_25 = sum(HU_25, na.rm = TRUE),
    gq_25 = sum(gq_25, na.rm = TRUE),
    block_recs = sum(block_recs, na.rm = TRUE),
    n_states = length(unique(state_code))
  ),
  by = zcta_20
]


# Apply ratio and CAGR calculations to combined datasets
combined_block_group_data <- calculate_hu_indices(combined_block_group_data)
combined_tract_data <- calculate_hu_indices(combined_tract_data)
combined_county_data <- calculate_hu_indices(combined_county_data)
combined_zcta_data <- calculate_hu_indices(combined_zcta_data)
combined_cousub_data <- calculate_hu_indices(combined_cousub_data_raw)
combined_place_data <- calculate_hu_indices(combined_place_data_raw)
combined_ua_data <- calculate_hu_indices(combined_ua_data_raw)


setwd("./data")

# Save combined data to RDS files
saveRDS(all_results, "all_state_results.rds")
saveRDS(combined_block_data, "combined_block_data.rds")
saveRDS(combined_block_group_data, "combined_block_group_data.rds")
saveRDS(combined_tract_data, "combined_tract_data.rds")
saveRDS(combined_county_data, "combined_county_data.rds")
saveRDS(combined_zcta_data, "combined_zcta_data.rds")
saveRDS(combined_cousub_data, "combined_cousub_data.rds")
saveRDS(combined_place_data, "combined_place_data.rds")
saveRDS(combined_ua_data, "combined_ua_data.rds")


# Save data to DuckDB with new naming convention
dbWriteTable(conh, "hu_block", combined_block_data, overwrite = TRUE)
dbWriteTable(conh, "hu_block_group", combined_block_group_data, overwrite = TRUE)
dbWriteTable(conh, "hu_tract", combined_tract_data, overwrite = TRUE)
dbWriteTable(conh, "hu_county", combined_county_data, overwrite = TRUE)
dbWriteTable(conh, "hu_zcta", combined_zcta_data, overwrite = TRUE)
dbWriteTable(conh, "hu_cousub", combined_cousub_data, overwrite = TRUE)
dbWriteTable(conh, "hu_place", combined_place_data, overwrite = TRUE)
dbWriteTable(conh, "hu_ua", combined_ua_data, overwrite = TRUE)

dbListTables(conh)
#dbDisconnect(conh)

####################### MAIN SECTION COMPLETE ##########################################################

## ADDENDUM: STATE AND US TOTALS

### we need to create base index computations at the state and us level from the master housing data
### these are not computed yet, and we will just rerun indexing code once we have the base data
### we will use county level data to start, from the existing housing data

hu_county <- data.table(dbGetQuery(conh, "SELECT * FROM hu_county;"))

hu_state <- hu_county[, .(
  HU_20 = sum(HU_20, na.rm = TRUE),
  gq_20 = sum(gq_20, na.rm = TRUE),
  HU_24 = sum(HU_24, na.rm = TRUE),
  gq_24 = sum(gq_24, na.rm = TRUE),
  HU_25 = sum(HU_25, na.rm = TRUE),
  gq_25 = sum(gq_25, na.rm = TRUE),
  block_recs = sum(block_recs, na.rm = TRUE)
), by = .(state_code, state_name)]

# national total

hu_us <- hu_county[, .(
  HU_20 = sum(HU_20, na.rm = TRUE),
  gq_20 = sum(gq_20, na.rm = TRUE),
  HU_24 = sum(HU_24, na.rm = TRUE),
  gq_24 = sum(gq_24, na.rm = TRUE),
  HU_25 = sum(HU_25, na.rm = TRUE),
  gq_25 = sum(gq_25, na.rm = TRUE),
  block_recs = sum(block_recs, na.rm = TRUE)
)]

## COMPUTE PERCENTS AND RATES

hu_state <- calculate_hu_indices(hu_state)
hu_us <- calculate_hu_indices(hu_us)

# write hu_state and hu_us to duckdb

dbWriteTable(conh, "hu_state", hu_state, overwrite = TRUE)
dbWriteTable(conh, "hu_us", hu_us, overwrite = TRUE)

saveRDS(hu_us, "hu_us.rds")
saveRDS(hu_state, "hu_state.rds")

############## ADDENDUM 2: CBSA ROLLUP TOTALS FOR DUCK DB; RETRIEVE lATER WITH INDEX CREATION

hu_block_group <- as.data.table(
  dbGetQuery(conh, "select * from hu_block_group")
)
hu_block_group[, county_fips := substr(block_group, 1, 5)]

county_to_cbsa <- fread('./data/county-cbsa-lookup.csv', colClasses = "character")

hu_block_group <- merge(
  hu_block_group,
  county_to_cbsa,
  by.x = "county_fips",
  by.y = "county",
  all.x = TRUE
)

hu_cbsa <- hu_block_group[, .(
  HU_20 = sum(HU_20, na.rm = TRUE),
  gq_20 = sum(gq_20, na.rm = TRUE),
  HU_24 = sum(HU_24, na.rm = TRUE),
  gq_24 = sum(gq_24, na.rm = TRUE),
  HU_25 = sum(HU_25, na.rm = TRUE),
  gq_25 = sum(gq_25, na.rm = TRUE),
  block_recs = sum(block_recs, na.rm = TRUE)
), by = .(cbsa23)]

## CAUSING UTF ERRORS IN DUCKDB; NEED TO INVESTIGATE/FIX
## add CBSAName23 from hu_block_group to hu_cbsa
##hu_cbsa <- merge(
##  hu_cbsa,
##  unique(hu_block_group[, .(cbsa23, CBSAName23)]),
##  by = "cbsa23",
##  all.x = TRUE
##)

hu_cbsa <- calculate_hu_indices(hu_cbsa)  

dbWriteTable(conh, "hu_cbsa", hu_cbsa, overwrite = TRUE)

dbListTables(conh)

saveRDS(hu_cbsa, "./data/hu_cbsa.rds")

## read the list of tables in DuckDB, generate a single metadata table for all tables, consisting of
## table name, column name, column type, and column description
## leave column description blank for now

metadata <- data.table(dbGetQuery(conh, "SELECT table_name, column_name, data_type, column_default, is_nullable, ordinal_position as cid FROM information_schema.columns WHERE table_schema = 'main'"))
metadata <- metadata[, .(table_name, column_name, data_type, column_default, is_nullable, cid)]
metadata[, column_description := NA]

# Save metadata table back to DuckDB
dbWriteTable(conh, "metadata", metadata, overwrite = TRUE)

####################################################################################################################

#dbGetQuery(conh, "select * from metadata")

# Close the database connection
dbDisconnect(conh)


