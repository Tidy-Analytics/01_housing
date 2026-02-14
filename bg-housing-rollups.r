## BLOCK GROUP TO GEOGRAPHIC ROLLUPS
## CORE ADAPTED FROM 'zcta-to-county.r'

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

conh <- dbConnect(duckdb('./data/housing.duckdb'))
dbListTables(conh)

congref <- dbConnect(duckdb('./data/georeference.duckdb'))
dbListTables(congref)

congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,
  extensions = c("spatial")
)

dbListTables(congeo)

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

# load block groups

hu_block_group <- dbGetQuery(conh, "select * from hu_block_group")

# match block groups to cbsa via county

county_to_cbsa <- fread("./data/county-cbsa-lookup.csv", 
                        colClasses = "character",
                        encoding = "Latin-1")

## write to georeference duckdb

dbWriteTable(congref, "county_to_cbsa", county_to_cbsa, overwrite = TRUE)

## get comparison geographies for index computations
## COUNTY, CBSA, STATE, US 

hu_block_group <- as.data.table(dbGetQuery(conh, "select * from hu_block_group"))
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))
hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### CBSA LEVEL BG ROLLUP #############

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

## compute county fips
hu_block_group[, county_fips := substr(block_group, 1, 5)]

hu_block_group_cbsa <- merge(hu_block_group, county_to_cbsa, by.x = "county_fips", by.y = "county", all.x = TRUE)

hu_block_group_cbsa <- as.data.table(
  merge(hu_block_group_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

# hgi_* indexes (housing growth index)
hu_block_group_cbsa[, idx_cbsa_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_block_group_cbsa[, idx_cbsa_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_block_group_cbsa[, idx_cbsa_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_block_group_cbsa[, idx_cbsa_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_block_group_cbsa[, idx_cbsa_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_block_group_cbsa[, idx_cbsa_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_block_group_cbsa[, idx_cbsa_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_block_group_cbsa[, idx_cbsa_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_block_group_cbsa[, idx_cbsa_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_block_group_cbsa[, idx_cbsa_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_block_group_cbsa[, idx_cbsa_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_block_group_cbsa[, idx_cbsa_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## NULL CBSA ARE CONNECTICUT BLOCK GROUPS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR BLOCK GROUP
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_block_group_cbsa <- hu_block_group_cbsa[!is.na(block_group), ]

hu_block_group_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_block_group_cbsa <- hu_block_group_cbsa[, .(
  block_group,
  cbsa23,
  idx_cbsa_hgi_20_apr_24_jul,
  idx_cbsa_hgi_24_jul_25_jul,
  idx_cbsa_hgi_24_jul_25_nov,
  idx_cbsa_hgi_25_jul_25_nov,
  idx_cbsa_hgi_20_apr_25_jul,
  idx_cbsa_hgi_20_apr_25_nov,
  idx_cbsa_cagr_20_apr_24_jul,
  idx_cbsa_cagr_24_jul_25_nov,
  idx_cbsa_cagr_25_jul_25_nov,
  idx_cbsa_cagr_20_apr_25_jul,
  idx_cbsa_cagr_20_apr_25_nov,
  idx_cbsa_agr_24_jul_25_jul
)]

#################################### COUNTY LEVEL BG ROLLUP #############

hu_block_group_county <- as.data.table(
  merge(hu_block_group, hu_county, by.x = "county_fips", by.y = "co_fips", all.x = TRUE)
)

################# RELATIVE INDEX COMPUTATIONS  -- COUNTY LEVEL

# hgi_* indexes (housing growth index)
hu_block_group_county[, idx_county_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_block_group_county[, idx_county_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_block_group_county[, idx_county_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_block_group_county[, idx_county_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_block_group_county[, idx_county_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_block_group_county[, idx_county_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_block_group_county[, idx_county_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_block_group_county[, idx_county_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_block_group_county[, idx_county_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_block_group_county[, idx_county_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_block_group_county[, idx_county_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_block_group_county[, idx_county_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_block_group_county <- hu_block_group_county[!is.na(block_group), ]

## TRIM FILE

hu_block_group_county <- hu_block_group_county[, .(
  block_group,
  county_fips,
  idx_county_hgi_20_apr_24_jul,
  idx_county_hgi_24_jul_25_jul,
  idx_county_hgi_24_jul_25_nov,
  idx_county_hgi_25_jul_25_nov,
  idx_county_hgi_20_apr_25_jul,
  idx_county_hgi_20_apr_25_nov,
  idx_county_cagr_20_apr_24_jul,
  idx_county_cagr_24_jul_25_nov,
  idx_county_cagr_25_jul_25_nov,
  idx_county_cagr_20_apr_25_jul,
  idx_county_cagr_20_apr_25_nov,
  idx_county_agr_24_jul_25_jul
)]

################################## STATE LEVEL BG ROLLUP #############

hu_block_group[, state_fips := substr(block_group, 1, 2)]

hu_block_group_state <- as.data.table(
  merge(hu_block_group, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_block_group_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_block_group_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_block_group_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_block_group_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_block_group_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_block_group_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_block_group_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_block_group_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_block_group_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_block_group_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_block_group_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_block_group_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_block_group_state <- hu_block_group_state[!is.na(block_group), ]

## TRIM FILE

hu_block_group_state <- hu_block_group_state[, .(
  block_group,
  state_fips,
  idx_state_hgi_20_apr_24_jul,
  idx_state_hgi_24_jul_25_jul,
  idx_state_hgi_24_jul_25_nov,
  idx_state_hgi_25_jul_25_nov,
  idx_state_hgi_20_apr_25_jul,
  idx_state_hgi_20_apr_25_nov,
  idx_state_cagr_20_apr_24_jul,
  idx_state_cagr_24_jul_25_nov,
  idx_state_cagr_25_jul_25_nov,
  idx_state_cagr_20_apr_25_jul,
  idx_state_cagr_20_apr_25_nov,
  idx_state_agr_24_jul_25_jul
)]

################################## US LEVEL BG ROLLUP #############

hu_block_group[, matchid := 1]
hu_us[, matchid := 1]

hu_block_group_us <- as.data.table(
  merge(hu_block_group, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

# hgi_* indexes (housing growth index)
hu_block_group_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_block_group_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_block_group_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_block_group_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_block_group_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_block_group_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_block_group_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_block_group_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_block_group_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_block_group_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_block_group_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_block_group_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# Percentiles for hgi_* metrics
hu_block_group_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for cagr_* metrics
hu_block_group_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_block_group_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for agr_* metrics
hu_block_group_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_block_group_us <- hu_block_group_us[, .(
  block_group,
  idx_us_hgi_20_apr_24_jul,
  idx_us_hgi_24_jul_25_jul,
  idx_us_hgi_24_jul_25_nov,
  idx_us_hgi_25_jul_25_nov,
  idx_us_hgi_20_apr_25_jul,
  idx_us_hgi_20_apr_25_nov,
  idx_us_cagr_20_apr_24_jul,
  idx_us_cagr_24_jul_25_nov,
  idx_us_cagr_25_jul_25_nov,
  idx_us_cagr_20_apr_25_jul,
  idx_us_cagr_20_apr_25_nov,
  idx_us_agr_24_jul_25_jul,
  pctl_us_hgi_20_apr_24_jul,
  pctl_us_hgi_24_jul_25_jul,
  pctl_us_hgi_24_jul_25_nov,
  pctl_us_hgi_25_jul_25_nov,
  pctl_us_hgi_20_apr_25_jul,
  pctl_us_hgi_20_apr_25_nov,
  pctl_us_cagr_20_apr_24_jul,
  pctl_us_cagr_24_jul_25_nov,
  pctl_us_cagr_25_jul_25_nov,
  pctl_us_cagr_20_apr_25_jul,
  pctl_us_cagr_20_apr_25_nov,
  pctl_us_agr_24_jul_25_jul
)]


### WRITE TO DUCKDB

## CLEANUP FIX FOR DUCKDB BARFING DUE TO UTF ISSUES
## HAT TIP TO https://github.com/duckdb/duckdb-r/issues/12#issuecomment-2419681433
## FOR THESE STRINGI FUNCTIONS

hu_block_group_state <- hu_block_group_state |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_block_group_county <- hu_block_group_county |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_block_group_cbsa <- hu_block_group_cbsa |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_block_group_us <- hu_block_group_us |> mutate(across(where(is.character), stringi::stri_enc_tonative))

# us count is one more than the others
# all NAs, remove
hu_block_group_us <- hu_block_group_us[!is.na(block_group), ]

length(unique(hu_block_group_county$block_group))
length(unique(hu_block_group_cbsa$block_group))
length(unique(hu_block_group_state$block_group))
length(unique(hu_block_group_us$block_group))

dbWriteTable(conh, "hu_block_group_county", hu_block_group_county, overwrite = TRUE)
dbWriteTable(conh, "hu_block_group_cbsa", hu_block_group_cbsa, overwrite = TRUE)
dbWriteTable(conh, "hu_block_group_state", hu_block_group_state, overwrite = TRUE)
dbWriteTable(conh, "hu_block_group_us", hu_block_group_us, overwrite = TRUE)

dbListTables(conh)
