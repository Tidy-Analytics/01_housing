## URBAN AREA TO GEOGRAPHIC ROLLUPS
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

congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,
  extensions = c("spatial")
)

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

## BASE GEOGRAPHY

hu_ua <- as.data.table(dbGetQuery(conh, "select * from hu_ua"))

## get comparison geographies

hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))


################################## STATE LEVEL UA ROLLUP #############

hu_ua_state <- as.data.table(
  merge(hu_ua, hu_state, by = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_ua_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_ua_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_ua_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_ua_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_ua_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_ua_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_ua_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_ua_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_ua_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_ua_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_ua_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_ua_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

hu_ua_state <- hu_ua_state[!is.na(ua), ]

## TRIM FILE

hu_ua_state <- hu_ua_state[, .(
  ua,
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

################################## US LEVEL UA ROLLUP #############

hu_ua[, matchid := 1]
hu_us[, matchid := 1]

hu_ua_us <- as.data.table(
  merge(hu_ua, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

# hgi_* indexes (housing growth index)
hu_ua_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_ua_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_ua_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_ua_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_ua_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_ua_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_ua_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_ua_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_ua_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_ua_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_ua_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_ua_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR URBAN AREAS

# Percentiles for hgi_* metrics
hu_ua_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_24_jul.x)) * 100)
)]
hu_ua_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_jul.x)) * 100)
)]
hu_ua_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_nov.x)) * 100)
)]
hu_ua_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_25_jul_25_nov.x)) * 100)
)]
hu_ua_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_jul.x)) * 100)
)]
hu_ua_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_nov.x)) * 100)
)]

# Percentiles for cagr_* metrics
hu_ua_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_24_jul.x)) * 100)
)]
hu_ua_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_24_jul_25_nov.x)) * 100)
)]
hu_ua_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_25_jul_25_nov.x)) * 100)
)]
hu_ua_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_jul.x)) * 100)
)]
hu_ua_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_nov.x)) * 100)
)]

# Percentiles for agr_* metrics
hu_ua_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(agr_24_jul_25_jul.x)) * 100)
)]

hu_ua_us <- hu_ua_us[, .(
  ua,
  state_code,
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

hu_ua_state <- hu_ua_state |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_ua_us <- hu_ua_us |> mutate(across(where(is.character), stringi::stri_enc_tonative))

# remove NAs
hu_ua_us <- hu_ua_us[!is.na(ua), ]

length(unique(hu_ua_state$ua))
length(unique(hu_ua_us$ua))

dbWriteTable(conh, "hu_ua_state", hu_ua_state, overwrite = TRUE)
dbWriteTable(conh, "hu_ua_us", hu_ua_us, overwrite = TRUE)

dbListTables(conh)


#### UA INDEXES COMPLETE ###############################



####### OPTIONAL: MERGE WITH UA NAMES FOR REVIEW #######

ua_names <- as.data.table(dbGetQuery(congref, "select distinct ua, uaname from block_ua"))
ua_hu <- as.data.table(dbGetQuery(conh, "select * from hu_ua"))
ua_state <- as.data.table(dbGetQuery(conh, "select * from hu_ua_state"))
ua_us <- as.data.table(dbGetQuery(conh, "select * from hu_ua_us"))

# merge indexes
ua_indexes <- Reduce(function(x, y) merge(x, y, by = c("ua", "state_code"), all = TRUE), 
                     list(ua_state, ua_us)
)

ua_hu <- merge(
  ua_names,
  ua_hu,
  by = "ua",
  all.x = TRUE
)

ua_indexes <- merge(
  ua_hu,
  ua_indexes,
  by = c("ua", "state_code"),
  all.x = TRUE
)

ua_indexes <- ua_indexes[order(HU_20_apr, decreasing = TRUE), ]
ua_indexes <- ua_indexes[!is.na(HU_20_apr), ]
