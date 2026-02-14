## COUNTY TO GEOGRAPHIC ROLLUPS
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

conmeta <- dbConnect(duckdb('./data/duckdb_metadata.duckdb'))
dbListTables(conmeta)

congref <- dbConnect(duckdb('./data/georeference.duckdb'))
dbListTables(congref)

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

# load counties
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))

## get indexes for baseline geographies

hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### COUNTY LEVEL TRACT ROLLUPS ############

### COUNTY TO CBSA

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

hu_county_cbsa <- merge(hu_county, county_to_cbsa, by.x = "co_fips", by.y = "county", all.x = TRUE)

hu_county_cbsa <- as.data.table(
  merge(hu_county_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

# hgi_* indexes (housing growth index)
hu_county_cbsa[, idx_cbsa_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_county_cbsa[, idx_cbsa_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_county_cbsa[, idx_cbsa_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_county_cbsa[, idx_cbsa_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_county_cbsa[, idx_cbsa_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_county_cbsa[, idx_cbsa_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_county_cbsa[, idx_cbsa_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_county_cbsa[, idx_cbsa_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_county_cbsa[, idx_cbsa_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_county_cbsa[, idx_cbsa_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_county_cbsa[, idx_cbsa_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_county_cbsa[, idx_cbsa_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## NULL CBSA ARE CONNECTICUT COUNTIES; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR COUNTY
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_county_cbsa <- hu_county_cbsa[!is.na(co_fips), ]

hu_county_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_county_cbsa <- hu_county_cbsa[, .(
  co_fips,
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


#### COUNTY TO STATE ################################

hu_county[, state_fips := substr(co_fips, 1, 2)]

hu_county_state <- as.data.table(
  merge(hu_county, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_county_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_county_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_county_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_county_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_county_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_county_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_county_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_county_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_county_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_county_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_county_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_county_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_county_state <- hu_county_state[!is.na(co_fips), ]

## TRIM FILE

hu_county_state <- hu_county_state[, .(
  co_fips,
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

########### COUNTY TO US #############################################

hu_county[, matchid := 1]
hu_us[, matchid := 1]

hu_county_us <- as.data.table(
  merge(hu_county, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

# hgi_* indexes (housing growth index)
hu_county_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_county_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_county_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_county_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_county_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_county_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_county_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_county_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_county_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_county_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_county_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_county_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR COUNTIES

# Percentiles for hgi_* metrics
hu_county_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for cagr_* metrics
hu_county_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_county_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for agr_* metrics
hu_county_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_county_us <- hu_county_us[, .(
  co_fips,
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

hu_county_cbsa <- hu_county_cbsa |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_county_state <- hu_county_state |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_county_us <- hu_county_us |> mutate(across(where(is.character), stringi::stri_enc_tonative))

hu_county_cbsa <- hu_county_cbsa[!is.na(co_fips), ]
hu_county_state <- hu_county_state[!is.na(co_fips), ]
hu_county_us <- hu_county_us[!is.na(co_fips), ]

dbWriteTable(conh, "hu_county_cbsa", hu_county_cbsa, overwrite = TRUE)
dbWriteTable(conh, "hu_county_state", hu_county_state, overwrite = TRUE)
dbWriteTable(conh, "hu_county_us", hu_county_us, overwrite = TRUE)

dbListTables(conh)

#### COUNTY INDEXES COMPLETE ###############################