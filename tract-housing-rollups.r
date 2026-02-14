## TRACT TO GEOGRAPHIC ROLLUPS
## CORE ADAPTED FROM 'bg-housing-rollups.r'

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

metadata <- dbGetQuery(conh, "select * from metadata")


reactable(metadata, filterable = TRUE, searchable = TRUE, pagination = TRUE, defaultPageSize = 20)

# load tracts

hu_tract <- dbGetQuery(conh, "select * from hu_tract")

# match tracts to cbsa via county

county_to_cbsa <- fread("./data/county-cbsa-lookup.csv", 
                        colClasses = c("character", "character", "character"),
                        col.names = c("county", "cbsa23", "cbsa_title"))

## write to georeference duckdb

dbWriteTable(congref, "county_to_cbsa", county_to_cbsa, overwrite = TRUE)

## get comparison geographies for index computations
## COUNTY, CBSA, STATE, US 

hu_tract <- as.data.table(dbGetQuery(conh, "select * from hu_tract"))
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))
hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### CBSA LEVEL TRACT ROLLUP #############

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

## compute county fips
hu_tract[, county_fips := substr(tract, 1, 5)]

hu_tract_cbsa <- merge(hu_tract, county_to_cbsa, by.x = "county_fips", by.y = "county", all.x = TRUE)

hu_tract_cbsa <- as.data.table(
  merge(hu_tract_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

# hgi_* indexes (housing growth index)
hu_tract_cbsa[, idx_cbsa_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_tract_cbsa[, idx_cbsa_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_tract_cbsa[, idx_cbsa_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_tract_cbsa[, idx_cbsa_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_tract_cbsa[, idx_cbsa_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_tract_cbsa[, idx_cbsa_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_tract_cbsa[, idx_cbsa_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_tract_cbsa[, idx_cbsa_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_tract_cbsa[, idx_cbsa_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_tract_cbsa[, idx_cbsa_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_tract_cbsa[, idx_cbsa_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_tract_cbsa[, idx_cbsa_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## NULL CBSA ARE CONNECTICUT TRACTS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR TRACT
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_tract_cbsa <- hu_tract_cbsa[!is.na(tract), ]

hu_tract_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_tract_cbsa <- hu_tract_cbsa[, .(
  tract,
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

#################################### COUNTY LEVEL TRACT ROLLUP #############

hu_tract_county <- as.data.table(
  merge(hu_tract, hu_county, by.x = "county_fips", by.y = "co_fips", all.x = TRUE)
)

################# RELATIVE INDEX COMPUTATIONS  -- COUNTY LEVEL

# hgi_* indexes (housing growth index)
hu_tract_county[, idx_county_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_tract_county[, idx_county_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_tract_county[, idx_county_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_tract_county[, idx_county_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_tract_county[, idx_county_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_tract_county[, idx_county_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_tract_county[, idx_county_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_tract_county[, idx_county_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_tract_county[, idx_county_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_tract_county[, idx_county_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_tract_county[, idx_county_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_tract_county[, idx_county_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_tract_county <- hu_tract_county[!is.na(tract), ]

## TRIM FILE

hu_tract_county <- hu_tract_county[, .(
  tract,
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

################################## STATE LEVEL TRACT ROLLUP #############

hu_tract[, state_fips := substr(tract, 1, 2)]

hu_tract_state <- as.data.table(
  merge(hu_tract, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_tract_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_tract_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_tract_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_tract_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_tract_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_tract_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_tract_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_tract_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_tract_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_tract_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_tract_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_tract_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_tract_state <- hu_tract_state[!is.na(tract), ]

## TRIM FILE

hu_tract_state <- hu_tract_state[, .(
  tract,
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

################################## US LEVEL TRACT ROLLUP #############

hu_tract[, matchid := 1]
hu_us[, matchid := 1]

hu_tract_us <- as.data.table(
  merge(hu_tract, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

# hgi_* indexes (housing growth index)
hu_tract_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_tract_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_tract_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_tract_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_tract_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_tract_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_tract_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_tract_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_tract_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_tract_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_tract_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_tract_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR TRACTS

# Percentiles for hgi_* metrics
hu_tract_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for cagr_* metrics
hu_tract_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_tract_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for agr_* metrics
hu_tract_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_tract_us <- hu_tract_us[, .(
  tract,
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

hu_tract_state <- hu_tract_state |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_tract_county <- hu_tract_county |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_tract_cbsa <- hu_tract_cbsa |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_tract_us <- hu_tract_us |> mutate(across(where(is.character), stringi::stri_enc_tonative))

# us count is one more than the others
# all NAs, remove
hu_tract_us <- hu_tract_us[!is.na(tract), ]

length(unique(hu_tract_county$tract))
length(unique(hu_tract_cbsa$tract))
length(unique(hu_tract_state$tract))
length(unique(hu_tract_us$tract))

dbWriteTable(conh, "hu_tract_county", hu_tract_county, overwrite = TRUE)
dbWriteTable(conh, "hu_tract_cbsa", hu_tract_cbsa, overwrite = TRUE)
dbWriteTable(conh, "hu_tract_state", hu_tract_state, overwrite = TRUE)
dbWriteTable(conh, "hu_tract_us", hu_tract_us, overwrite = TRUE)

#### TRACT INDEXES COMPLETE ###############################
