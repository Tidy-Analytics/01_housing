library(duckdb)
library(data.table)
library(DBI)

setwd("/home/joel")

### CONNECTIONS ---------------------------------------------------------------

conh <- dbConnect(duckdb(), "./data/housing.duckdb")

congeo <- dbConnect(
  duckdb::duckdb(),
  dbdir = "./data/spatial_storage.duckdb"
)

congref <- dbConnect(
  duckdb::duckdb(),
  dbdir = "./data/georeference.duckdb"
)

### GEO NAME LOOKUPS ----------------------------------------------------------

block_group_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid AS block_group_id, namelsad AS block_group_name FROM geo_block_group")
)

census_tract_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid AS census_tract_id, namelsad AS census_tract_name FROM geo_tract")
)

county_subdivision_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid AS county_sub_id, namelsad AS county_sub_name, namelsadco AS county, stusps AS state FROM geo_cosub_23")
)

county_names <- as.data.table(
  dbGetQuery(congeo, "SELECT statefp || countyfp AS county_id, namelsad AS county_name, stusps AS state FROM geo_county_23")
)

zcta_names <- as.data.table(
  dbGetQuery(congeo, "SELECT zcta5ce20 AS zcta_id, 'ZCTA ' || zcta5ce20 AS zcta_name FROM geo_zcta")
)

place_names <- as.data.table(
  dbGetQuery(congeo, "SELECT statefp || placefp AS place_id, namelsad AS place_name, stusps AS state FROM geo_place_23")
)

cbsa_names <- as.data.table(
  dbGetQuery(congeo, "SELECT cbsafp AS cbsa_id, namelsad AS cbsa_name FROM geo_cbsa_23")
)

ua_names <- as.data.table(
  dbGetQuery(congref,
    "SELECT ua AS ua_id, uaname AS ua_name, substr(geoid, 1, 2) AS state_code
     FROM block_ua
     GROUP BY ua, uaname, substr(geoid, 1, 2)
     ORDER BY ua")
)

state_names <- as.data.table(
  dbGetQuery(congeo, "SELECT name AS state_name, statefp AS state_code FROM geo_state")
)

### OUTPUT DATABASE -----------------------------------------------------------

out_path <- "./data/housing_distro.duckdb"
if (file.exists(out_path)) file.remove(out_path)
cono <- dbConnect(duckdb(), out_path)

unlink("./data/parquet", recursive = TRUE)
unlink("./data/csv",     recursive = TRUE)
dir.create("./data/parquet", showWarnings = FALSE, recursive = TRUE)
dir.create("./data/csv",     showWarnings = FALSE, recursive = TRUE)

### HELPERS -------------------------------------------------------------------

write_layer <- function(dt, table_name) {
  dbWriteTable(cono, table_name, as.data.frame(dt), overwrite = TRUE)
  dbExecute(cono, sprintf("COPY %s TO './data/parquet/%s.parquet' (FORMAT PARQUET)", table_name, table_name))
  fwrite(dt, sprintf("./data/csv/%s.csv", table_name))
  cat(sprintf("  Written: %s (%d rows x %d cols)\n", table_name, nrow(dt), ncol(dt)))
}

# Add baseline label to an index table immediately after the baseline ID col.
# id_col   : name of the baseline foreign-key column in the index table
# name_lut : lookup data.table with columns c(id_col, label_col)
# label_col: name to use for the new label column in the output
add_base_label <- function(idx_dt, id_col, name_lut, label_col) {
  lut <- copy(name_lut)
  setnames(lut, names(lut), c(id_col, label_col))
  result <- merge(idx_dt, lut, by = id_col, all.x = TRUE)
  # reorder so label immediately follows the id column
  other_cols <- setdiff(names(result), c(id_col, label_col))
  result[, c(id_col, label_col, other_cols), with = FALSE]
}


### ===========================================================================
### 1. BLOCK — base measures only, carried forward as-is
### ===========================================================================

cat("Building block...\n")

block <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block"))
write_layer(block, "block")


### ===========================================================================
### 2. BLOCK GROUP
###    Indexes: county, cbsa, state, us (in that order)
### ===========================================================================

cat("Building block_group...\n")

bg_base  <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group"))
bg_cty   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group_county"))
bg_cbsa  <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group_cbsa"))
bg_st    <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group_state"))
bg_us    <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_block_group_us"))

# names: join on block_group (first 12 chars = census tract geoid)
setkey(block_group_names, block_group_id)
setkey(bg_base, block_group)

bg <- merge(bg_base, block_group_names, by.x = "block_group", by.y = "block_group_id", all.x = TRUE)
# reorder: id, name, then remaining base cols
bg <- bg[, c("block_group", "block_group_name",
             setdiff(names(bg), c("block_group", "block_group_name"))),
         with = FALSE]

# add baseline labels; label col inserted after baseline ID, before metrics
bg_cty  <- add_base_label(bg_cty,  "county_fips", county_names[, .(county_id, county_name)],  "idx_county_base")
bg_cbsa <- add_base_label(bg_cbsa, "cbsa23",       cbsa_names[, .(cbsa_id, cbsa_name)],        "idx_cbsa_base")
bg_st   <- add_base_label(bg_st,   "state_fips",   state_names[, .(state_code, state_name)],   "idx_state_base")
bg_us_cols <- setdiff(names(bg_us), "block_group")

bg <- merge(bg, bg_cty,  by = "block_group", all.x = TRUE)
bg <- merge(bg, bg_cbsa, by = "block_group", all.x = TRUE)
bg <- merge(bg, bg_st,   by = "block_group", all.x = TRUE)
bg <- merge(bg, bg_us[, c("block_group", bg_us_cols), with = FALSE],
            by = "block_group", all.x = TRUE)

write_layer(bg, "block_group")


### ===========================================================================
### 3. CENSUS TRACT
###    Indexes: county, cbsa, state, us
### ===========================================================================

cat("Building census_tract...\n")

tr_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract"))
tr_cty  <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract_county"))
tr_cbsa <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract_cbsa"))
tr_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract_state"))
tr_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_tract_us"))

tr <- merge(tr_base, census_tract_names, by.x = "tract", by.y = "census_tract_id", all.x = TRUE)
tr <- tr[, c("tract", "census_tract_name",
             setdiff(names(tr), c("tract", "census_tract_name"))),
         with = FALSE]

tr_cty  <- add_base_label(tr_cty,  "county_fips", county_names[, .(county_id, county_name)],  "idx_county_base")
tr_cbsa <- add_base_label(tr_cbsa, "cbsa23",       cbsa_names[, .(cbsa_id, cbsa_name)],        "idx_cbsa_base")
tr_st   <- add_base_label(tr_st,   "state_fips",   state_names[, .(state_code, state_name)],   "idx_state_base")

tr <- merge(tr, tr_cty,  by = "tract", all.x = TRUE)
tr <- merge(tr, tr_cbsa, by = "tract", all.x = TRUE)
tr <- merge(tr, tr_st,   by = "tract", all.x = TRUE)
tr <- merge(tr, tr_us,   by = "tract", all.x = TRUE)

write_layer(tr, "census_tract")


### ===========================================================================
### 4. ZCTA
###    Indexes: state, us (no county or cbsa index)
### ===========================================================================

cat("Building zcta...\n")

zcta_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_zcta"))
zcta_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_zcta_state"))
zcta_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_zcta_us"))

zcta <- merge(zcta_base, zcta_names, by.x = "zcta_20", by.y = "zcta_id", all.x = TRUE)
zcta <- zcta[, c("zcta_20", "zcta_name",
                 setdiff(names(zcta), c("zcta_20", "zcta_name"))),
             with = FALSE]

zcta_st <- add_base_label(zcta_st, "state_fips", state_names[, .(state_code, state_name)], "idx_state_base")

zcta <- merge(zcta, zcta_st, by = "zcta_20", all.x = TRUE)
zcta <- merge(zcta, zcta_us, by = "zcta_20", all.x = TRUE)

write_layer(zcta, "zcta")


### ===========================================================================
### 5. COUNTY SUBDIVISION
###    Key: county_fips + cousub
###    Indexes: county, cbsa, state, us
### ===========================================================================

cat("Building county_subdivision...\n")

cs_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub"))
cs_cty  <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub_county"))   # cousubfp + county_fips
cs_cbsa <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub_cbsa"))     # cousubfp + county_fips + cbsa23
cs_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub_state"))    # cousub + county_fips + state_code
cs_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cousub_us"))       # cousub + county_fips

# cousub = cousubfp confirmed by inspection; use county_sub_id (statefp+countyfp+cousubfp) for name lookup
# county_sub_id in geo is full 10-char GEOID (statefp[2] + countyfp[3] + cousubfp[5])
# cousub in hu_cousub is 5-char cousubfp; county_fips is 5-char statefp+countyfp
# construct full geoid for name join
cs_base[, geo_id := paste0(county_fips, cousub)]
county_subdivision_names_sub <- county_subdivision_names[, .(county_sub_id, county_sub_name, county, state)]

cs <- merge(cs_base, county_subdivision_names_sub,
            by.x = "geo_id", by.y = "county_sub_id", all.x = TRUE)
cs[, geo_id := NULL]

# reorder: id cols first, name, then the rest
cs <- cs[, c("county_fips", "cousub", "county_sub_name", "county", "state",
             setdiff(names(cs),
                     c("county_fips", "cousub", "county_sub_name", "county", "state"))),
         with = FALSE]

# county index: add label, rename cousubfp -> cousub for join
cs_cty <- add_base_label(cs_cty, "county_fips", county_names[, .(county_id, county_name)], "idx_county_base")
setnames(cs_cty, "cousubfp", "cousub")
cs <- merge(cs, cs_cty, by = c("cousub", "county_fips"), all.x = TRUE)

# cbsa index: add label, drop cbsa23, rename cousubfp -> cousub
cs_cbsa <- add_base_label(cs_cbsa, "cbsa23", cbsa_names[, .(cbsa_id, cbsa_name)], "idx_cbsa_base")
cs_cbsa <- cs_cbsa[, setdiff(names(cs_cbsa), "cbsa23"), with = FALSE]
setnames(cs_cbsa, "cousubfp", "cousub")
cs <- merge(cs, cs_cbsa, by = c("cousub", "county_fips"), all.x = TRUE)

# state index: add label; state_code is the key col in cs_st
cs_st <- add_base_label(cs_st, "state_code", state_names[, .(state_code, state_name)], "idx_state_base")
cs_st <- cs_st[, setdiff(names(cs_st), "state_code"), with = FALSE]
cs <- merge(cs, cs_st, by = c("cousub", "county_fips"), all.x = TRUE)

# us index: cousub + county_fips
cs <- merge(cs, cs_us, by = c("cousub", "county_fips"), all.x = TRUE)

write_layer(cs, "county_subdivision")


### ===========================================================================
### 6. PLACE
###    Key: place + state_code
###    Indexes: state, us (no county or cbsa index)
### ===========================================================================

cat("Building place...\n")

pl_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_place"))
pl_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_place_state"))
pl_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_place_us"))

# place geo id = statefp(2) + placefp(5) = state_code(2) + place(5)
pl_base[, geo_id := paste0(state_code, place)]
place_names_sub <- place_names[, .(place_id, place_name, state)]

pl <- merge(pl_base, place_names_sub, by.x = "geo_id", by.y = "place_id", all.x = TRUE)
pl[, geo_id := NULL]

pl <- pl[, c("place", "state_code", "place_name", "state",
             setdiff(names(pl), c("place", "state_code", "place_name", "state"))),
         with = FALSE]

# state index: add label; state_code is both the baseline key and the outer join key,
# so keep it in the result (add_base_label preserves the id_col)
pl_st <- add_base_label(pl_st, "state_code", state_names[, .(state_code, state_name)], "idx_state_base")
pl <- merge(pl, pl_st, by = c("place", "state_code"), all.x = TRUE)

# us index
pl <- merge(pl, pl_us, by = c("place", "state_code"), all.x = TRUE)

write_layer(pl, "place")


### ===========================================================================
### 7. URBAN AREA
###    Key: ua + state_code
###    Indexes: state, us (no county or cbsa index)
### ===========================================================================

cat("Building urban_area...\n")

ua_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_ua"))
ua_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_ua_state"))
ua_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_ua_us"))

ua <- merge(ua_base, ua_names, by.x = c("ua", "state_code"), by.y = c("ua_id", "state_code"), all.x = TRUE)
ua <- ua[, c("ua", "state_code", "ua_name",
             setdiff(names(ua), c("ua", "state_code", "ua_name"))),
         with = FALSE]

# state index: state_code is both baseline key and outer join key — keep it
ua_st <- add_base_label(ua_st, "state_code", state_names[, .(state_code, state_name)], "idx_state_base")
ua <- merge(ua, ua_st, by = c("ua", "state_code"), all.x = TRUE)

# us index
ua <- merge(ua, ua_us, by = c("ua", "state_code"), all.x = TRUE)

write_layer(ua, "urban_area")


### ===========================================================================
### 8. COUNTY
###    Key: county_fips
###    Indexes: county (self — baseline), cbsa, state, us
###    Note: county IS also an index baseline; no self-index table exists.
###          cbsa, state, us indexes exist.
### ===========================================================================

cat("Building county...\n")

co_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_county"))
co_cbsa <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_county_cbsa"))
co_st   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_county_state"))
co_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_county_us"))

# base table already has county_name, state_code, state_name — enhance with state abbrev from names
# county_names has county_id (statefp+countyfp), county_name, state (stusps abbrev)
co <- merge(co_base, county_names[, .(county_id, state)],
            by.x = "county_fips", by.y = "county_id", all.x = TRUE)

co <- co[, c("county_fips", "county_name", "state_code", "state_name", "state",
             setdiff(names(co),
                     c("county_fips", "county_name", "state_code", "state_name", "state"))),
         with = FALSE]

# cbsa index: add label
co_cbsa <- add_base_label(co_cbsa, "cbsa23", cbsa_names[, .(cbsa_id, cbsa_name)], "idx_cbsa_base")
co <- merge(co, co_cbsa, by = "county_fips", all.x = TRUE)

# state index: add label
co_st <- add_base_label(co_st, "state_fips", state_names[, .(state_code, state_name)], "idx_state_base")
co <- merge(co, co_st, by = "county_fips", all.x = TRUE)

# us index
co <- merge(co, co_us, by = "county_fips", all.x = TRUE)

write_layer(co, "county")


### ===========================================================================
### 9. CBSA
###    Key: cbsa23
###    Indexes: cbsa (self — baseline), us
###    Note: cbsa IS also an index baseline; only us index table exists.
### ===========================================================================

cat("Building cbsa...\n")

cbsa_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cbsa"))
cbsa_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_cbsa_us"))

cbsa <- merge(cbsa_base, cbsa_names, by.x = "cbsa23", by.y = "cbsa_id", all.x = TRUE)
cbsa <- cbsa[, c("cbsa23", "cbsa_name",
                 setdiff(names(cbsa), c("cbsa23", "cbsa_name"))),
             with = FALSE]

cbsa <- merge(cbsa, cbsa_us, by = "cbsa23", all.x = TRUE)

write_layer(cbsa, "cbsa")


### ===========================================================================
### 10. STATE
###     Key: state_code
###     Indexes: state (self — baseline), us
###     Note: state IS also an index baseline; only us index table exists.
### ===========================================================================

cat("Building state...\n")

st_base <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_state"))
st_us   <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_state_us"))

# base table already has state_name; bring in stusps abbreviation from geo
st <- merge(st_base, state_names, by.x = "state_code", by.y = "state_code", all.x = TRUE)
# state_name already in base; state_name.x (from base) and state_name.y (from geo names)
# drop the geo duplicate if it exists
if ("state_name.y" %in% names(st)) {
  st[, state_name.y := NULL]
  setnames(st, "state_name.x", "state_name")
}

st <- st[, c("state_code", "state_name",
             setdiff(names(st), c("state_code", "state_name"))),
         with = FALSE]

st <- merge(st, st_us, by = "state_code", all.x = TRUE)

write_layer(st, "state")


### ===========================================================================
### 11. US
###     Key: none (single row)
###     Indexes: none (US IS the top-level baseline)
### ===========================================================================

cat("Building us...\n")

us <- as.data.table(dbGetQuery(conh, "SELECT * FROM hu_us"))

write_layer(us, "us")


### VERIFY --------------------------------------------------------------------

cat("\nFinal tables in housing_distro.duckdb:\n")
print(dbListTables(cono))

cat("\nColumn counts per table:\n")
for (tbl in dbListTables(cono)) {
  info <- dbGetQuery(cono, sprintf("PRAGMA table_info('%s')", tbl))
  cat(sprintf("  %-22s %d cols\n", tbl, nrow(info)))
}

### CLOSE CONNECTIONS ---------------------------------------------------------

dbDisconnect(cono, shutdown = TRUE)
dbDisconnect(conh, shutdown = TRUE)
dbDisconnect(congeo, shutdown = TRUE)
dbDisconnect(congref, shutdown = TRUE)

cat("\nDone. Outputs:\n  ./data/housing_distro.duckdb\n  ./data/parquet/  (one .parquet per layer)\n  ./data/csv/      (one .csv per layer)\n")
