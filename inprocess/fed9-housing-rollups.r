## FEDERAL RESERVE 9TH DISTRICT HOUSING AGGREGATE AND ROLLUP INDEXES
##
## PART 1: Build hu_fed9 aggregate row from hu_county joined to district membership
## PART 2: Compute comparative indexes (vs 9th district baseline) for:
##         block_group, tract, zcta, cousub, county

library(duckdb)
library(data.table)
library(DBI)
library(dplyr)
library(dotenv)

setwd("/home/joel")

dotenv::load_dot_env()

### DUCKDB CONNECTIONS ########################################################

conh  <- dbConnect(duckdb('./data/housing.duckdb'))
congref <- dbConnect(duckdb('./data/georeference.duckdb'))
congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,
  extensions = c("spatial")
)

dbListTables(congeo)

# CALCULATE HU INDICES (same function as prd-housing-units-2026.R) ##########

calculate_hu_indices <- function(dt) {
  dt[, `:=`(
    # 20 TO 24 (Apr 2020 - Jul 2024 = 4.25 years)
    hg_20_apr_24_jul       = HU_24_jul - HU_20_apr,
    hgi_20_apr_24_jul      = ifelse(HU_20_apr > 0, HU_24_jul / HU_20_apr, NA_real_),
    cagr_20_apr_24_jul     = ifelse(HU_20_apr > 0, ((HU_24_jul / HU_20_apr)^(1 / 4.25)) - 1, NA_real_),
    # 24 To 25 JUL (Jul 2024 - Jul 2025 = 1.0 year)
    hg_24_jul_25_jul       = HU_25_jul - HU_24_jul,
    hgi_24_jul_25_jul      = ifelse(HU_24_jul > 0, HU_25_jul / HU_24_jul, NA_real_),
    agr_24_jul_25_jul      = ifelse(HU_24_jul > 0, (HU_25_jul / HU_24_jul) - 1, NA_real_),
    # 24 To 25 NOV (Jul 2024 - Nov 2025 = 1.33 years)
    hg_24_jul_25_nov       = HU_25_nov - HU_24_jul,
    hgi_24_jul_25_nov      = ifelse(HU_24_jul > 0, HU_25_nov / HU_24_jul, NA_real_),
    cagr_24_jul_25_nov     = ifelse(HU_24_jul > 0, ((HU_25_nov / HU_24_jul)^(1 / 1.33)) - 1, NA_real_),
    # JUL TO NOV 2025 (Jul 2025 - Nov 2025 = 0.33 years, 4 months)
    hg_25_jul_25_nov       = HU_25_nov - HU_25_jul,
    hgi_25_jul_25_nov      = ifelse(HU_25_jul > 0, HU_25_nov / HU_25_jul, NA_real_),
    cagr_25_jul_25_nov     = ifelse(HU_25_jul > 0, ((HU_25_nov / HU_25_jul)^(1 / 0.33)) - 1, NA_real_),
    # TOTAL PERIOD TO JUL 2025 (Apr 2020 - Jul 2025 = 5.25 years)
    hg_20_apr_25_jul       = HU_25_jul - HU_20_apr,
    hgi_20_apr_25_jul      = ifelse(HU_20_apr > 0, HU_25_jul / HU_20_apr, NA_real_),
    cagr_20_apr_25_jul     = ifelse(HU_20_apr > 0, ((HU_25_jul / HU_20_apr)^(1 / 5.25)) - 1, NA_real_),
    # TOTAL PERIOD TO NOV 2025 (Apr 2020 - Nov 2025 = 5.58 years)
    hg_20_apr_25_nov       = HU_25_nov - HU_20_apr,
    hgi_20_apr_25_nov      = ifelse(HU_20_apr > 0, HU_25_nov / HU_20_apr, NA_real_),
    cagr_20_apr_25_nov     = ifelse(HU_20_apr > 0, ((HU_25_nov / HU_20_apr)^(1 / 5.58)) - 1, NA_real_)
  )]
  return(dt)
}

### PART 1: BUILD hu_fed9 AGGREGATE ###########################################

## Load county membership file
fed9_counties <- fread('./data/fed-reserve-district-9-counties.csv', colClasses = "character")

## Load county-level housing data
hu_county <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_county;"))

## Filter to 9th district counties only
hu_fed9_counties <- hu_county[county_fips %in% fed9_counties$county_fips]

cat("9th district county count:", nrow(hu_fed9_counties), "\n")

## Sum to single district aggregate (same pattern as hu_us)
hu_fed9 <- hu_fed9_counties[, .(
  HU_20_apr  = sum(HU_20_apr,  na.rm = TRUE),
  gq_20_apr  = sum(gq_20_apr,  na.rm = TRUE),
  HU_24_jul  = sum(HU_24_jul,  na.rm = TRUE),
  gq_24_jul  = sum(gq_24_jul,  na.rm = TRUE),
  HU_25_jul  = sum(HU_25_jul,  na.rm = TRUE),
  gq_25_jul  = sum(gq_25_jul,  na.rm = TRUE),
  HU_25_nov  = sum(HU_25_nov,  na.rm = TRUE),
  gq_25_nov  = sum(gq_25_nov,  na.rm = TRUE),
  block_recs = sum(block_recs, na.rm = TRUE),
  county_count = .N
)]

## Compute indexes for the district aggregate itself
hu_fed9 <- calculate_hu_indices(hu_fed9)

cat("9th district HU totals:\n")
print(hu_fed9[, .(HU_20_apr, HU_24_jul, HU_25_jul, HU_25_nov,
                   hgi_20_apr_25_nov, cagr_20_apr_25_nov)])

## Write to DuckDB
dbWriteTable(conh, "hu_fed9", hu_fed9, overwrite = TRUE)

cat("hu_fed9 written to DuckDB\n")

### PART 2: ROLLUP INDEXES VS 9TH DISTRICT BASELINE ##########################

## Helper: compute idx_fed9_* and pctl_fed9_* columns on a merged data.table
## .x suffix = the geo-level metric; .y suffix = the fed9 benchmark metric

compute_fed9_indexes <- function(dt) {
  # hgi_* indexes (housing growth index)
  dt[, idx_fed9_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
  dt[, idx_fed9_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
  dt[, idx_fed9_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
  dt[, idx_fed9_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
  dt[, idx_fed9_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
  dt[, idx_fed9_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]
  # cagr_* indexes (compound annual growth rate)
  dt[, idx_fed9_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
  dt[, idx_fed9_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
  dt[, idx_fed9_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
  dt[, idx_fed9_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
  dt[, idx_fed9_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]
  # agr_* indexes (annual growth rate)
  dt[, idx_fed9_agr_24_jul_25_jul  := (agr_24_jul_25_jul.x)  / (agr_24_jul_25_jul.y)  * 100]
  return(dt)
}

compute_fed9_percentiles <- function(dt) {
  dt[, pctl_fed9_hgi_20_apr_24_jul := as.integer(ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_24_jul.x)) * 100))]
  dt[, pctl_fed9_hgi_24_jul_25_jul := as.integer(ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_jul.x)) * 100))]
  dt[, pctl_fed9_hgi_24_jul_25_nov := as.integer(ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_24_jul_25_nov.x)) * 100))]
  dt[, pctl_fed9_hgi_25_jul_25_nov := as.integer(ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_25_jul_25_nov.x)) * 100))]
  dt[, pctl_fed9_hgi_20_apr_25_jul := as.integer(ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_jul.x)) * 100))]
  dt[, pctl_fed9_hgi_20_apr_25_nov := as.integer(ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(hgi_20_apr_25_nov.x)) * 100))]
  dt[, pctl_fed9_cagr_20_apr_24_jul := as.integer(ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_24_jul.x)) * 100))]
  dt[, pctl_fed9_cagr_24_jul_25_nov := as.integer(ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_24_jul_25_nov.x)) * 100))]
  dt[, pctl_fed9_cagr_25_jul_25_nov := as.integer(ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_25_jul_25_nov.x)) * 100))]
  dt[, pctl_fed9_cagr_20_apr_25_jul := as.integer(ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_jul.x)) * 100))]
  dt[, pctl_fed9_cagr_20_apr_25_nov := as.integer(ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / sum(!is.na(cagr_20_apr_25_nov.x)) * 100))]
  dt[, pctl_fed9_agr_24_jul_25_jul  := as.integer(ceiling(frank(agr_24_jul_25_jul.x,  ties.method = "min", na.last = "keep") / sum(!is.na(agr_24_jul_25_jul.x)) * 100))]
  return(dt)
}

## Add matchid to hu_fed9 for cross-join (same technique as hu_us rollups)
hu_fed9[, matchid := 1]

### 2A: BLOCK GROUP vs FED9 ###################################################

hu_block_group <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group;"))
hu_block_group[, county_fips := substr(block_group, 1, 5)]

## Keep only block groups whose county is in the 9th district
hu_block_group_fed9_sub <- hu_block_group[county_fips %in% fed9_counties$county_fips]

hu_block_group_fed9_sub[, matchid := 1]
hu_bg_fed9 <- as.data.table(merge(hu_block_group_fed9_sub, hu_fed9, by = "matchid", all.x = TRUE))

hu_bg_fed9 <- compute_fed9_indexes(hu_bg_fed9)
hu_bg_fed9 <- compute_fed9_percentiles(hu_bg_fed9)
hu_bg_fed9 <- hu_bg_fed9[!is.na(block_group)]

hu_bg_fed9 <- hu_bg_fed9[, .(
  block_group,
  county_fips,
  idx_fed9_hgi_20_apr_24_jul,
  idx_fed9_hgi_24_jul_25_jul,
  idx_fed9_hgi_24_jul_25_nov,
  idx_fed9_hgi_25_jul_25_nov,
  idx_fed9_hgi_20_apr_25_jul,
  idx_fed9_hgi_20_apr_25_nov,
  idx_fed9_cagr_20_apr_24_jul,
  idx_fed9_cagr_24_jul_25_nov,
  idx_fed9_cagr_25_jul_25_nov,
  idx_fed9_cagr_20_apr_25_jul,
  idx_fed9_cagr_20_apr_25_nov,
  idx_fed9_agr_24_jul_25_jul,
  pctl_fed9_hgi_20_apr_24_jul,
  pctl_fed9_hgi_24_jul_25_jul,
  pctl_fed9_hgi_24_jul_25_nov,
  pctl_fed9_hgi_25_jul_25_nov,
  pctl_fed9_hgi_20_apr_25_jul,
  pctl_fed9_hgi_20_apr_25_nov,
  pctl_fed9_cagr_20_apr_24_jul,
  pctl_fed9_cagr_24_jul_25_nov,
  pctl_fed9_cagr_25_jul_25_nov,
  pctl_fed9_cagr_20_apr_25_jul,
  pctl_fed9_cagr_20_apr_25_nov,
  pctl_fed9_agr_24_jul_25_jul
)]

cat("Block groups in 9th district:", nrow(hu_bg_fed9), "\n")

### 2B: TRACT vs FED9 #########################################################

hu_tract <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract;"))
hu_tract[, county_fips := substr(tract, 1, 5)]

hu_tract_fed9_sub <- hu_tract[county_fips %in% fed9_counties$county_fips]
hu_tract_fed9_sub[, matchid := 1]
hu_tr_fed9 <- as.data.table(merge(hu_tract_fed9_sub, hu_fed9, by = "matchid", all.x = TRUE))

hu_tr_fed9 <- compute_fed9_indexes(hu_tr_fed9)
hu_tr_fed9 <- compute_fed9_percentiles(hu_tr_fed9)
hu_tr_fed9 <- hu_tr_fed9[!is.na(tract)]

hu_tr_fed9 <- hu_tr_fed9[, .(
  tract,
  county_fips,
  idx_fed9_hgi_20_apr_24_jul,
  idx_fed9_hgi_24_jul_25_jul,
  idx_fed9_hgi_24_jul_25_nov,
  idx_fed9_hgi_25_jul_25_nov,
  idx_fed9_hgi_20_apr_25_jul,
  idx_fed9_hgi_20_apr_25_nov,
  idx_fed9_cagr_20_apr_24_jul,
  idx_fed9_cagr_24_jul_25_nov,
  idx_fed9_cagr_25_jul_25_nov,
  idx_fed9_cagr_20_apr_25_jul,
  idx_fed9_cagr_20_apr_25_nov,
  idx_fed9_agr_24_jul_25_jul,
  pctl_fed9_hgi_20_apr_24_jul,
  pctl_fed9_hgi_24_jul_25_jul,
  pctl_fed9_hgi_24_jul_25_nov,
  pctl_fed9_hgi_25_jul_25_nov,
  pctl_fed9_hgi_20_apr_25_jul,
  pctl_fed9_hgi_20_apr_25_nov,
  pctl_fed9_cagr_20_apr_24_jul,
  pctl_fed9_cagr_24_jul_25_nov,
  pctl_fed9_cagr_25_jul_25_nov,
  pctl_fed9_cagr_20_apr_25_jul,
  pctl_fed9_cagr_20_apr_25_nov,
  pctl_fed9_agr_24_jul_25_jul
)]

cat("Tracts in 9th district:", nrow(hu_tr_fed9), "\n")

### 2C: ZCTA vs FED9 ##########################################################
## Use zcta_to_county crosswalk (1:1 ZCTA -> county assignment by area score)
## A ZCTA is "in" the 9th district if its assigned county is a member county

zcta_to_county <- as.data.table(
  dbGetQuery(
    congref,
    "SELECT * FROM read_csv_auto(
     './data/tab20_zcta520_county20_natl.txt',
     header=True, normalize_names=True
     );"
  )
)

zcta_to_county <- zcta_to_county[
  !is.na(geoid_zcta5_20) & !is.na(geoid_county_20),
  .(geoid_zcta5_20, arealand_zcta5_20, geoid_county_20, arealand_county_20, arealand_part)
]

zcta_to_county[, area_share_pct  := arealand_part / sum(arealand_part), by = geoid_zcta5_20]
zcta_to_county[, area_county_pct := arealand_part / arealand_county_20]
zcta_to_county[, score           := area_share_pct * area_county_pct]
zcta_to_county[, rank            := frank(-score), by = geoid_zcta5_20]
zcta_to_county <- zcta_to_county[rank == 1, .(geoid_zcta5_20, geoid_county_20)]

hu_zcta <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_zcta;"))

## Join ZCTA to their primary county, then filter to 9th district counties
hu_zcta_with_county <- merge(hu_zcta, zcta_to_county,
                              by.x = "zcta_20", by.y = "geoid_zcta5_20", all.x = TRUE)

hu_zcta_fed9_sub <- hu_zcta_with_county[geoid_county_20 %in% fed9_counties$county_fips]

hu_zcta_fed9_sub[, matchid := 1]
hu_zc_fed9 <- as.data.table(merge(hu_zcta_fed9_sub, hu_fed9, by = "matchid", all.x = TRUE))

hu_zc_fed9 <- compute_fed9_indexes(hu_zc_fed9)
hu_zc_fed9 <- compute_fed9_percentiles(hu_zc_fed9)
hu_zc_fed9 <- hu_zc_fed9[!is.na(zcta_20)]

hu_zc_fed9 <- hu_zc_fed9[, .(
  zcta_20,
  county_fips = geoid_county_20,
  idx_fed9_hgi_20_apr_24_jul,
  idx_fed9_hgi_24_jul_25_jul,
  idx_fed9_hgi_24_jul_25_nov,
  idx_fed9_hgi_25_jul_25_nov,
  idx_fed9_hgi_20_apr_25_jul,
  idx_fed9_hgi_20_apr_25_nov,
  idx_fed9_cagr_20_apr_24_jul,
  idx_fed9_cagr_24_jul_25_nov,
  idx_fed9_cagr_25_jul_25_nov,
  idx_fed9_cagr_20_apr_25_jul,
  idx_fed9_cagr_20_apr_25_nov,
  idx_fed9_agr_24_jul_25_jul,
  pctl_fed9_hgi_20_apr_24_jul,
  pctl_fed9_hgi_24_jul_25_jul,
  pctl_fed9_hgi_24_jul_25_nov,
  pctl_fed9_hgi_25_jul_25_nov,
  pctl_fed9_hgi_20_apr_25_jul,
  pctl_fed9_hgi_20_apr_25_nov,
  pctl_fed9_cagr_20_apr_24_jul,
  pctl_fed9_cagr_24_jul_25_nov,
  pctl_fed9_cagr_25_jul_25_nov,
  pctl_fed9_cagr_20_apr_25_jul,
  pctl_fed9_cagr_20_apr_25_nov,
  pctl_fed9_agr_24_jul_25_jul
)]

cat("ZCTAs in 9th district:", nrow(hu_zc_fed9), "\n")

### 2D: COUSUB vs FED9 ########################################################

hu_cousub <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub;"))

hu_cousub_fed9_sub <- hu_cousub[county_fips %in% fed9_counties$county_fips]
hu_cousub_fed9_sub[, matchid := 1]
hu_cs_fed9 <- as.data.table(merge(hu_cousub_fed9_sub, hu_fed9, by = "matchid", all.x = TRUE))

hu_cs_fed9 <- compute_fed9_indexes(hu_cs_fed9)
hu_cs_fed9 <- compute_fed9_percentiles(hu_cs_fed9)
hu_cs_fed9 <- hu_cs_fed9[!is.na(cousub)]

hu_cs_fed9 <- hu_cs_fed9[, .(
  cousub,
  county_fips,
  idx_fed9_hgi_20_apr_24_jul,
  idx_fed9_hgi_24_jul_25_jul,
  idx_fed9_hgi_24_jul_25_nov,
  idx_fed9_hgi_25_jul_25_nov,
  idx_fed9_hgi_20_apr_25_jul,
  idx_fed9_hgi_20_apr_25_nov,
  idx_fed9_cagr_20_apr_24_jul,
  idx_fed9_cagr_24_jul_25_nov,
  idx_fed9_cagr_25_jul_25_nov,
  idx_fed9_cagr_20_apr_25_jul,
  idx_fed9_cagr_20_apr_25_nov,
  idx_fed9_agr_24_jul_25_jul,
  pctl_fed9_hgi_20_apr_24_jul,
  pctl_fed9_hgi_24_jul_25_jul,
  pctl_fed9_hgi_24_jul_25_nov,
  pctl_fed9_hgi_25_jul_25_nov,
  pctl_fed9_hgi_20_apr_25_jul,
  pctl_fed9_hgi_20_apr_25_nov,
  pctl_fed9_cagr_20_apr_24_jul,
  pctl_fed9_cagr_24_jul_25_nov,
  pctl_fed9_cagr_25_jul_25_nov,
  pctl_fed9_cagr_20_apr_25_jul,
  pctl_fed9_cagr_20_apr_25_nov,
  pctl_fed9_agr_24_jul_25_jul
)]

cat("County subdivisions in 9th district:", nrow(hu_cs_fed9), "\n")

### 2E: COUNTY vs FED9 ########################################################

hu_county_fed9_sub <- hu_county[county_fips %in% fed9_counties$county_fips]
hu_county_fed9_sub[, matchid := 1]
hu_co_fed9 <- as.data.table(merge(hu_county_fed9_sub, hu_fed9, by = "matchid", all.x = TRUE))

hu_co_fed9 <- compute_fed9_indexes(hu_co_fed9)
## No percentiles for county — small N; index values are sufficient

hu_co_fed9 <- hu_co_fed9[!is.na(county_fips)]

hu_co_fed9 <- hu_co_fed9[, .(
  county_fips,
  county_name,
  state_code,
  state_name,
  idx_fed9_hgi_20_apr_24_jul,
  idx_fed9_hgi_24_jul_25_jul,
  idx_fed9_hgi_24_jul_25_nov,
  idx_fed9_hgi_25_jul_25_nov,
  idx_fed9_hgi_20_apr_25_jul,
  idx_fed9_hgi_20_apr_25_nov,
  idx_fed9_cagr_20_apr_24_jul,
  idx_fed9_cagr_24_jul_25_nov,
  idx_fed9_cagr_25_jul_25_nov,
  idx_fed9_cagr_20_apr_25_jul,
  idx_fed9_cagr_20_apr_25_nov,
  idx_fed9_agr_24_jul_25_jul
)]

cat("Counties in 9th district:", nrow(hu_co_fed9), "\n")

### WRITE ALL OUTPUTS TO DUCKDB ###############################################

## UTF encoding fix (consistent with other rollup scripts)
hu_bg_fed9  <- hu_bg_fed9  |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_tr_fed9  <- hu_tr_fed9  |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_zc_fed9  <- hu_zc_fed9  |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_cs_fed9  <- hu_cs_fed9  |> mutate(across(where(is.character), stringi::stri_enc_tonative))
hu_co_fed9  <- hu_co_fed9  |> mutate(across(where(is.character), stringi::stri_enc_tonative))

dbWriteTable(conh, "hu_block_group_fed9", hu_bg_fed9, overwrite = TRUE)
dbWriteTable(conh, "hu_tract_fed9",       hu_tr_fed9, overwrite = TRUE)
dbWriteTable(conh, "hu_zcta_fed9",        hu_zc_fed9, overwrite = TRUE)
dbWriteTable(conh, "hu_cousub_fed9",      hu_cs_fed9, overwrite = TRUE)
dbWriteTable(conh, "hu_county_fed9",      hu_co_fed9, overwrite = TRUE)

cat("\nAll 9th district rollup tables written to DuckDB:\n")
cat("  hu_fed9               (", nrow(hu_fed9),       "row )\n")
cat("  hu_block_group_fed9   (", nrow(hu_bg_fed9),    "rows)\n")
cat("  hu_tract_fed9         (", nrow(hu_tr_fed9),    "rows)\n")
cat("  hu_zcta_fed9          (", nrow(hu_zc_fed9),    "rows)\n")
cat("  hu_cousub_fed9        (", nrow(hu_cs_fed9),    "rows)\n")
cat("  hu_county_fed9        (", nrow(hu_co_fed9),    "rows)\n")

dbListTables(conh)

dbDisconnect(conh)
dbDisconnect(congref)