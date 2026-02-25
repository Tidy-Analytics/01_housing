## ZCTA TO STATE; 
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

### HOUSING 

conh <- dbConnect(duckdb('./data/housing.duckdb'))
dbListTables(conh)

## GEOGRAPHIC REFERENCE DATABASE - FOR CROSSWALK OVERLAYS

congref <- dbConnect(duckdb('./data/georeference.duckdb'))
dbListTables(congref)

congeo <- dbConnect(duckdb('./data/spatial_storage.duckdb'))
dbListTables(congeo)

## BASE GEOGRAPHY - ENTITY UNIVERSE FROM SPATIAL STORAGE

hu_cousub_ids <- as.data.table(dbGetQuery(congeo, "select co_fips as county_fips, cousubfp, name, namelsad as full_name, stusps from geo_cosub_23"))
setnames(hu_cousub_ids, tolower)


## CORE DATA FOR BASE GEOGRAPHY

hu_cousub <- as.data.table(dbGetQuery(conh, "select * from hu_cousub"))

## get comparison geographies for index computations
## COUNTY, CBSA, STATE, US

hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))
hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

# # Rename columns from old naming convention to new naming convention
# old_names <- c("HU_20", "HU_24", "gq_20", "gq_24", 
#                "hg_20_24", "hg_24_25_jul", "hg_24_25_nov", "hg_jul_nov_25", "hg_20_25_jul", "hg_20_25_nov",
#                "hgi_20_24", "hgi_24_25_jul", "hgi_24_25_nov", "hgi_jul_nov_25", "hgi_20_25_jul", "hgi_20_25_nov",
#                "cagr_20_24", "cagr_24_25_nov", "cagr_jul_nov_25", "cagr_20_25_jul", "cagr_20_25_nov",
#                "agr_24_25_jul")
               
# new_names <- c("HU_20_apr", "HU_24_jul", "gq_20_apr", "gq_24_jul",
#                "hg_20_apr_24_jul", "hg_24_jul_25_jul", "hg_24_jul_25_nov", "hg_25_jul_25_nov", "hg_20_apr_25_jul", "hg_20_apr_25_nov",
#                "hgi_20_apr_24_jul", "hgi_24_jul_25_jul", "hgi_24_jul_25_nov", "hgi_25_jul_25_nov", "hgi_20_apr_25_jul", "hgi_20_apr_25_nov",
#                "cagr_20_apr_24_jul", "cagr_24_jul_25_nov", "cagr_25_jul_25_nov", "cagr_20_apr_25_jul", "cagr_20_apr_25_nov",
#                "agr_24_jul_25_jul")

# # Apply renaming to each dataset
# setnames(hu_cousub, old = old_names, new = new_names, skip_absent = TRUE)
# setnames(hu_state, old = old_names, new = new_names, skip_absent = TRUE)
# setnames(hu_county, old = old_names, new = new_names, skip_absent = TRUE)
# setnames(hu_cbsa, old = old_names, new = new_names, skip_absent = TRUE)
# setnames(hu_us, old = old_names, new = new_names, skip_absent = TRUE) 

### COUNTY LEVEL COUSUB ROLLUP ##########################################

hu_cousub_base <- as.data.table(
  merge(hu_cousub_ids, hu_cousub, by.x = c("county_fips","cousubfp"), by.y = c("county_fips", "cousub"), all.x = TRUE)
  
)

hu_cousub_county <- as.data.table(
  merge(hu_cousub_base, hu_county, by.x = c("county_fips"), by.y = c("county_fips"), all.x = TRUE)
)


############### RELATIVE INDEX COMPUTATIONS  -- COUNTY LEVEL

# hgi_* indexes (housing growth index)
hu_cousub_county[, idx_county_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_cousub_county[, idx_county_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_cousub_county[, idx_county_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_cousub_county[, idx_county_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_cousub_county[, idx_county_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_cousub_county[, idx_county_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_cousub_county[, idx_county_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_cousub_county[, idx_county_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_cousub_county[, idx_county_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_cousub_county[, idx_county_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_cousub_county[, idx_county_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_cousub_county[, idx_county_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## TRIM FILE

hu_cousub_county <- hu_cousub_county[, .(  
  cousubfp,
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


### CBSA LEVEL COUSUB ROLLUP ###########################################

# county to metro area crosswalk

county_to_cbsa <- fread(
  "./data/county-cbsa-lookup.csv",
  colClasses = "character",
  encoding = "UTF-8"
)

hu_cousub_base <- as.data.table(
  merge(hu_cousub_ids, hu_cousub, by.x = c("county_fips","cousubfp"), by.y = c("county_fips", "cousub"), all.x = TRUE)
  
)

# match county_to_cbsa to get cbsa23 code for each cbsa, match on county_fips

hu_cousub_cbsa_id <- as.data.table(
  merge(hu_cousub_base, county_to_cbsa, by.x = c("county_fips"), by.y = c("county"), all.x = TRUE)
)

hu_cousub_cbsa <- as.data.table(
  merge(hu_cousub_cbsa_id, hu_cbsa, by.x = c("cbsa23"), by.y = c("cbsa23"), all.x = TRUE)
)

# only carry rows with cbsa association
hu_cousub_cbsa <- hu_cousub_cbsa[!is.na(cbsa23), ]

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

# hgi_* indexes (housing growth index)
hu_cousub_cbsa[, idx_cbsa_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_cousub_cbsa[, idx_cbsa_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_cousub_cbsa[, idx_cbsa_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_cousub_cbsa[, idx_cbsa_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_cousub_cbsa[, idx_cbsa_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_cousub_cbsa[, idx_cbsa_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_cousub_cbsa[, idx_cbsa_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_cousub_cbsa[, idx_cbsa_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_cousub_cbsa[, idx_cbsa_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_cousub_cbsa[, idx_cbsa_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_cousub_cbsa[, idx_cbsa_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_cousub_cbsa[, idx_cbsa_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## TRIM FILE

hu_cousub_cbsa <- hu_cousub_cbsa[, .(
  cousubfp,
  county_fips,
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


################################## STATE LEVEL COUSUB ROLLUP #############

hu_cousub[, state_code := substr(county_fips, 1, 2)]

hu_cousub_state <- as.data.table(
  merge(hu_cousub, hu_state, by.x = "state_code", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_cousub_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_cousub_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_cousub_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_cousub_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_cousub_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_cousub_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_cousub_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_cousub_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_cousub_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_cousub_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_cousub_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_cousub_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_cousub_state <- hu_cousub_state[!is.na(cousub), ]

## TRIM FILE

hu_cousub_state <- hu_cousub_state[, .(
  cousub,
  county_fips,
  state_code,
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



################################## US LEVEL COUSUB ROLLUP #############

hu_cousub[, matchid := 1]
hu_us[, matchid := 1]

hu_cousub_us <- as.data.table(
  merge(hu_cousub, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

# hgi_* indexes (housing growth index)
hu_cousub_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_cousub_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_cousub_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_cousub_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_cousub_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_cousub_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_cousub_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_cousub_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_cousub_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_cousub_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_cousub_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_cousub_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR COUNTY SUBS

# Percentiles for hgi_* metrics
hu_cousub_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_24_jul.x)) * 100)
)]
hu_cousub_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_jul.x)) * 100)
)]
hu_cousub_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_nov.x)) * 100)
)]
hu_cousub_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_25_jul_25_nov.x)) * 100)
)]
hu_cousub_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_jul.x)) * 100)
)]
hu_cousub_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_nov.x)) * 100)
)]

# Percentiles for cagr_* metrics
hu_cousub_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_24_jul.x)) * 100)
)]
hu_cousub_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_24_jul_25_nov.x)) * 100)
)]
hu_cousub_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_25_jul_25_nov.x)) * 100)
)]
hu_cousub_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_jul.x)) * 100)
)]
hu_cousub_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_nov.x)) * 100)
)]

# Percentiles for agr_* metrics
hu_cousub_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(agr_24_jul_25_jul.x)) * 100)
)]

hu_cousub_us <- hu_cousub_us[, .(
  cousub,
  county_fips,
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

hu_cousub_state <- hu_cousub_state |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_cousub_county <- hu_cousub_county |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_cousub_cbsa <- hu_cousub_cbsa |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_cousub_us <- hu_cousub_us |> mutate(across(where(is.character), stringi::stri_enc_tonative))


dbWriteTable(conh, "hu_cousub_county", hu_cousub_county, overwrite = TRUE)
dbWriteTable(conh, "hu_cousub_cbsa", hu_cousub_cbsa, overwrite = TRUE)
dbWriteTable(conh, "hu_cousub_state", hu_cousub_state, overwrite = TRUE)
dbWriteTable(conh, "hu_cousub_us", hu_cousub_us, overwrite = TRUE)

dbListTables(conh)


