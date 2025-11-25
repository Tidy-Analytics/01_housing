# BuildingZones from TidyAnalytics
**Production System**: Housing Unit Growth Analysis Pipeline
**Location**: `/home/joel/01_housing/`
**Target Audience**: Internal Development Team, Data Engineers, Analysts
**Last Updated**: 2025-11-25
**Status**: Production

---

## Executive Summary

This directory contains the **production housing data processing pipeline** that generates housing unit growth analytics across all U.S. Census geographic levels. The scripts process Census Bureau Address Block Count Lists for multiple time periods (2020, 2024, 2025) and compute comparative growth indices, percentiles, and rollups for:

- 11M+ Census blocks → Block groups → Tracts → Counties → States → National
- 33K+ ZIP Code Tabulation Areas (ZCTAs)
- 30K+ Places (cities, towns, CDPs)
- 36K+ County Subdivisions (minor civil divisions)
- 3.5K+ Urban Areas
- 950+ Metropolitan/Micropolitan Statistical Areas (CBSAs)

**Key Outputs**:
- DuckDB database (`housing.duckdb`) with 17 tables
- RDS files for R-based analytics
- Comparative indices (vs. CBSA, county, state, US)
- National percentile rankings
- Growth trajectories (CAGR 2020-2025)

---

## Table of Contents

1. [Script Inventory](#script-inventory)
2. [Execution Order & Dependencies](#execution-order--dependencies)
3. [Script Details](#script-details)
4. [Input Requirements](#input-requirements)
5. [Output Artifacts](#output-artifacts)
6. [Documentation Gaps & Issues](#documentation-gaps--issues)
7. [Database Connections](#database-connections)
8. [Common Patterns](#common-patterns)
9. [Production Checklist](#production-checklist)

---

## Script Inventory

| Script | Purpose | Status | Execution Order | Runtime | Key Outputs |
|--------|---------|--------|-----------------|---------|-------------|
| `block-lookups.r` | **Setup**: Build geographic crosswalks | Setup | 0 (one-time) | ~20 min | `georeference.duckdb` tables |
| `prd-housing-units-update.r` | **Download** latest Census data | Active | 1 | ~15 min | Raw Census text files |
| `prd-housing-units-2025.R` | **Master pipeline**: Load, merge, aggregate | Active | 2 | ~45 min | Base HU tables + aggregations |
| `bg-housing-rollups.r` | **Block group** comparative indexes | Active | 3 | ~5 min | `hu_block_group_indexes` |
| `tract-housing-rollups.r` | **Tract** comparative indexes | Active | 4 | ~3 min | `hu_tract_indexes` |
| `county-housing-rollups.r` | **County** comparative indexes | Active | 5 | ~2 min | `hu_county_indexes` |
| `cbsa-housing-rollups.r` | **Metro area** comparative indexes | Active | 6 | ~1 min | `hu_cbsa_indexes` |
| `zcta-housing-rollups.r` | **ZCTA** comparative indexes | Active | 7 | ~2 min | `hu_zcta_indexes` |
| `cousub-housing-rollups.r` | **County subdivision** indexes | Active | 8 | ~3 min | `hu_cousub_indexes` |
| `place-housing-rollups.r` | **Place** comparative indexes | Active | 9 | ~2 min | `hu_place_indexes` |
| `zcta-to-county.r` | **Utility**: ZCTA→County crosswalk | Reference | N/A | ~5 min | `zcta_to_county.csv` |
| `zcta-to-metro.r` | **Deprecated**: Old ZCTA→CBSA logic | Archived | N/A | N/A | (Not used) |

**Total Runtime**: ~78 minutes (full pipeline from download to final indexes)
**One-Time Setup**: +20 minutes (block-lookups.r, only needed when rebuilding reference database)

---

## Execution Order & Dependencies

### Dependency Graph

```
0. block-lookups.r (ONE-TIME SETUP - Build georeference.duckdb)
   └─→ Reads: GEOCORR CSV files (block-place-*.csv, block-cosub-*.csv, block-ua-*.csv)
   └─→ Writes: georeference.duckdb tables (block_place, block_cosub, block_ua)
       └─→ Database: georeference.duckdb

1. prd-housing-units-update.r (Download Census files)
   └─→ Downloads 51 state files × 3 years = 153 files
       └─→ Stored in: /home/joel/data/geo/

2. prd-housing-units-2025.R (Master Processing)
   └─→ Reads: Downloaded Census files + Crosswalks + georeference.duckdb
   └─→ Writes: Base tables (hu_block, hu_block_group, hu_tract, etc.)
       └─→ Database: housing.duckdb

3-9. *-housing-rollups.r (Parallel - can run concurrently)
   └─→ Read: Base tables from housing.duckdb + georeference.duckdb
   └─→ Write: Index tables (*_indexes) to housing.duckdb
```

### Critical Path

1. **ONE-TIME SETUP**: `block-lookups.r` (only when rebuilding or initializing system)
2. **MUST run first**: `prd-housing-units-update.r` (if new data available)
3. **MUST run second**: `prd-housing-units-2025.R` (creates base tables)
4. **CAN run in parallel**: All `*-rollups.r` scripts (independent of each other)

### Execution Commands

```r
# STEP 0: ONE-TIME SETUP (only needed when initializing or rebuilding reference database)
# First, download GEOCORR files from University of Missouri (see Input Requirements section)
setwd("/home/joel")
source("01_housing/block-lookups.r")

# STEP 1: Download latest data (quarterly updates)
source("01_housing/prd-housing-units-update.r")

# STEP 2: Process all states and create base tables
source("01_housing/prd-housing-units-2025.R")

# STEP 3: Generate comparative indexes (can parallelize)
source("01_housing/bg-housing-rollups.r")
source("01_housing/tract-housing-rollups.r")
source("01_housing/county-housing-rollups.r")
source("01_housing/cbsa-housing-rollups.r")
source("01_housing/zcta-housing-rollups.r")
source("01_housing/cousub-housing-rollups.r")
source("01_housing/place-housing-rollups.r")
```

---

## Script Details

### 0. `block-lookups.r` (One-Time Setup)

**Purpose**: Builds the `georeference.duckdb` database by processing GEOCORR geographic correspondence files into unified lookup tables.

**Data Source**: **University of Missouri GEOCORR** (Geographic Correspondence Engine)
- **Website**: `https://mcdc.missouri.edu/applications/geocorr.html`
- **Description**: Web-based tool that generates block-level geographic correspondence files between Census geographic entities

**Key Operations**:
1. **Reads state-specific CSV files** from GEOCORR exports (pattern: `block-{geography}-{state}.csv`)
2. **Processes three geography types**:
   - Block → Place (cities, towns, CDPs)
   - Block → County Subdivision (minor civil divisions, MCDs)
   - Block → Urban Area (2020 urban/rural classification)
3. **Constructs 15-digit block GEOID** by concatenating: `county + tract (dots removed) + block`
4. **Handles UTF-8 encoding** for geographic names with special characters
5. **Creates state-specific tables** in DuckDB (e.g., `block_place_06` for California)
6. **Unions all state tables** into master tables: `block_place`, `block_cosub`, `block_ua`

**Processing Steps**:
```
For each geography type (place, cosub, ua):
  ├─ List all CSV files matching pattern (e.g., block-place-*.csv)
  ├─ For each state file:
  │  ├─ Read CSV with fread()
  │  ├─ Construct GEOID = paste0(county, gsub("\\.", "", tract), block)
  │  ├─ Convert character encoding to UTF-8
  │  ├─ Lowercase all column names
  │  ├─ Convert data types (pop20→int, lat/lon→numeric, afact→numeric)
  │  └─ Write to DuckDB as block_{geography}_{state} table
  ├─ Build UNION ALL query across all state tables
  ├─ Filter out header row (rowid > 1)
  └─ Create master table (e.g., CREATE OR REPLACE TABLE block_place AS ...)
```

**GEOCORR File Requirements**:

**File Naming Pattern**:
- `block-place-{state_abbrev}.csv` (e.g., `block-place-ca.csv`, `block-place-tx.csv`)
- `block-cosub-{state_abbrev}.csv`
- `block-ua-{state_abbrev}.csv`

**Expected Columns** (GEOCORR output):
- `county`: 5-digit county FIPS
- `tract`: Census tract (with decimal, e.g., "0001.02")
- `block`: 4-digit block number
- `place` / `cousub20` / `ua`: Target geography code
- `placename` / `mcdname` / `uaname`: Geographic name
- `pop20`: 2020 Census population (block-level)
- `intptlat`, `intptlon`: Internal point coordinates
- `afact`, `afact2`: Allocation factors (for weighted aggregations)

**How to Obtain GEOCORR Files**:

1. **Go to**: `https://mcdc.missouri.edu/applications/geocorr2022.html`
2. **Select**:
   - Source geography: **Block (2020)**
   - Target geography: **Place** OR **County Subdivision** OR **Urban Area**
   - States: Choose individual states OR **all states**
   - Output options: **CSV format**
   - Include: Block GEOID components, target geography codes/names, population, allocation factors
3. **Download** files to `/home/joel/data/geo/` with proper naming convention
4. **Run** `block-lookups.r` to process into `georeference.duckdb`

**Inputs**:
- **CSV files** (52 files per geography type = 156 total):
  - `/home/joel/data/geo/block-place-*.csv` (51 states + DC)
  - `/home/joel/data/geo/block-cosub-*.csv` (51 states + DC)
  - `/home/joel/data/geo/block-ua-*.csv` (51 states + DC)

**Outputs**:
- **DuckDB database**: `/home/joel/data/georeference.duckdb`
  - `block_place` table (~2M rows): All blocks with place assignments
  - `block_cosub` table (~1.5M rows): All blocks with county subdivision assignments
  - `block_ua` table (~3M rows): All blocks with urban area assignments

**Database Connections**:
```r
congref <- dbConnect(duckdb::duckdb(),
                    dbdir = "./data/georeference.duckdb",
                    read_only = FALSE,
                    extensions = c("spatial"))
```

**Data Validation** (built into script):
- Samples 1,000 random records per geography type for manual review
- Aggregates block counts and population by geography
- Displays summary statistics with `reactable()` for QA checks

**When to Re-Run**:
- **Initial system setup** (creating `georeference.duckdb` from scratch)
- **After Census boundary changes** (e.g., new TIGER/Line vintage)
- **When GEOCORR updates** definitions (rarely, usually after decennial census)
- **If geographic names change** (city annexations, incorporations)

**Performance Notes**:
- Runtime: ~20 minutes (processing ~6.5M block assignments)
- Memory: ~4 GB RAM required (large UNION operations)
- Disk I/O intensive during table consolidation

**Known Issues**:
- Row 1 in GEOCORR exports contains **duplicate column headers** (handled by `WHERE rowid > 1` filter)
- UTF-8 encoding required for names with accents/special characters (e.g., "Española", "Montréal")
- Some blocks have no geography assignment (valid for unpopulated areas)

**Verification Queries** (built into script):
```r
# Block counts by county subdivision
block_cosub_counts <- dbGetQuery(congref, "
  SELECT county, countyname, cousub20, mcdname,
         COUNT(*) AS block_count,
         SUM(pop20) AS total_population
  FROM block_cosub
  GROUP BY ALL
")

# Similar for places and urban areas
```

---

### 1. `prd-housing-units-update.r`

**Purpose**: Downloads latest Address Block Count List files from Census Bureau website.

**Data Source**: `https://www2.census.gov/geo/pvs/addcountlisting/2025/`

**Key Operations**:
- Loops through 51 states (50 + DC)
- Uses `wget` to download files
- File naming pattern: `{STATEFP}_{StateName}_AddressBlockCountList_072025.txt`

**Inputs**:
- State list (hardcoded in script)
- Internet connection required

**Outputs**:
- 51 text files → `/home/joel/data/geo/`

**Documentation Gaps**:
- ⚠️ **No error handling** for failed downloads
- ⚠️ **No validation** that downloaded files are complete/uncorrupted
- ⚠️ **No logging** of download success/failure
- ⚠️ **Hardcoded year** (2025) - needs manual update for future releases

**Recommendations**:
```r
# Suggested improvements:
# 1. Add error handling:
result <- system(paste("wget", "-O", shQuote(filepath), shQuote(url)), intern = TRUE)
if (result != 0) { warning(paste("Failed to download:", state_name)) }

# 2. Validate file size:
file_size <- file.info(filepath)$size
if (file_size < 1000) { warning(paste("Suspicious file size:", state_name)) }

# 3. Log downloads:
download_log <- data.table(state_name, file_size, timestamp = Sys.time())
```

---

### 2. `prd-housing-units-2025.R`

**Purpose**: Master processing pipeline that loads, merges, and aggregates housing data.

**Processing Flow**:
```
Raw Census Files (51 states × 3 years)
  ↓
state_HU_data() function - processes one state
  ├─ Load 2020, 2024, 2025 block-level data
  ├─ Merge on block_geoid
  ├─ Handle Connecticut 2022 block remapping
  ├─ Attach ZCTA, Place, County Subdivision, Urban Area lookups
  ├─ Aggregate to 8 geographic levels
  └─ Return list of 8 data.tables
  ↓
Loop through all 51 states
  ↓
Combine state results (rbindlist)
  ↓
Special aggregation for state-spanning geographies (ZCTA, Place, UA)
  ↓
calculate_hu_indices() - compute growth rates, CAGR
  ↓
Write to DuckDB (housing.duckdb) + RDS files
```

**Key Functions**:

#### `state_HU_data(state_code, state_name)`
- **Input**: State FIPS code ("06") and name ("California")
- **Output**: List with 8 data.tables (block, block_group, tract, county, zcta, cousub, place, ua)
- **Special handling**: Connecticut block remapping (line 243)

#### `calculate_hu_indices(dt)`
- **Input**: Data.table with HU_20, HU_24, HU_25 columns
- **Output**: Same data.table with added growth metrics
- **Metrics calculated**:
  - `hg_20_24`: HU_24 - HU_20 (absolute growth)
  - `hgi_20_24`: HU_24 / HU_20 (growth index)
  - `cagr_20_24`: ((HU_24/HU_20)^(1/4.33)) - 1
  - `agr_25`: (HU_25/HU_24) - 1 (annual growth rate)
  - `cagr_20_25`: ((HU_25/HU_20)^(1/5.33)) - 1

**Time Periods** (Critical for CAGR calculations):
- 2020-2024: 4.33 years (April 2020 → November 2023)
- 2024-2025: 1.0 year (simplified)
- 2020-2025: 5.33 years (full period)

**Database Connections**:
```r
conh <- dbConnect(duckdb('./data/housing.duckdb'))         # Main output
congref <- dbConnect(duckdb('./data/georeference.duckdb')) # Crosswalks
congeo <- dbConnect(duckdb('./data/spatial_storage.duckdb')) # Geometries
```

**Inputs**:
1. **Census files**: `/home/joel/data/geo/{STATEFP}_{StateName}_AddressBlockCountList_{period}.txt`
2. **Crosswalks**:
   - `./data/geo/tab20_zcta520_tabblock20_natl.txt` (Block→ZCTA)
   - `./data/ct-2022blockcrosswalk.csv` (Connecticut remapping)
   - `./data/geo/county-adjacency.txt` (County metadata)
   - `./data/county-cbsa-lookup.csv` (County→CBSA)
3. **DuckDB lookups**:
   - `georeference.duckdb::block_place` (Block→Place)
   - `georeference.duckdb::block_cosub` (Block→County Subdivision)
   - `georeference.duckdb::block_ua` (Block→Urban Area)

**Outputs**:
- **DuckDB tables** (housing.duckdb):
  - `hu_block`, `hu_block_group`, `hu_tract`, `hu_county`
  - `hu_zcta`, `hu_cousub`, `hu_place`, `hu_ua`
  - `hu_cbsa`, `hu_state`, `hu_us`
  - `metadata` (column documentation)
- **RDS files** (./data/):
  - `all_state_results.rds`, `combined_block_data.rds`, etc.

**Documentation Gaps**:
- ⚠️ **No validation** that all 51 states processed successfully
- ⚠️ **Incomplete handling** of Connecticut planning regions (line 92-101 commented out)
- ⚠️ **Hardcoded time periods** (4.33, 5.33 years) - should be calculated from actual dates
- ⚠️ **No documentation** on why some aggregations are commented out (COUSUB, PLACE, UA on lines 465-511)
- ⚠️ **SQL syntax error** on line 602: `county-cbsa-lookup.csv` (should this be in georeference.duckdb?)
- ⚠️ **UTF encoding error** noted in comment (line 622): CBSAName23 causes issues

---

### 3. `bg-housing-rollups.r`

**Purpose**: Generates comparative indices for block groups relative to parent geographies.

**Processing Logic**:
```
Load hu_block_group (base data)
  ↓
Join to parent geographies (CBSA, County, State, US)
  ↓
Calculate relative indices:
  idx_20_24_cbsa = (block_group_growth / cbsa_growth) × 100
  ↓
Calculate national percentiles (frank() function)
  ↓
Write hu_block_group_indexes to DuckDB
```

**Comparative Indices Created**:
- `idx_20_24_cbsa`: Block group growth vs. metro area
- `idx_20_24_county`: Block group growth vs. county
- `idx_20_24_state`: Block group growth vs. state
- `idx_20_24_us`: Block group growth vs. national
- `pctl_20_24_us`, `pctl_20_25_us`, `pctl_25_us`: National percentiles (1-100)

**Index Interpretation**:
- `idx = 100`: Geography growing at same rate as parent
- `idx > 100`: Geography growing faster than parent
- `idx < 100`: Geography growing slower than parent

**Inputs**:
- `housing.duckdb::hu_block_group` (base data)
- `housing.duckdb::hu_cbsa` (metro benchmarks)
- `housing.duckdb::hu_county` (county benchmarks)
- `housing.duckdb::hu_state` (state benchmarks)
- `housing.duckdb::hu_us` (national benchmark)
- `georeference.duckdb::county_to_cbsa` (crosswalk)

**Outputs**:
- `housing.duckdb::hu_block_group_indexes`

**Special Handling**:
- Connecticut block groups: Assigned `cbsa23 = "CT000"` for unmapped areas (line 93)
- Match ID trick (line 157): Creates Cartesian product for US-level merge

**Documentation Gaps**:
- ⚠️ **No handling** of NA values in index calculations (division by zero if parent has zero growth)
- ⚠️ **Unclear** why Connecticut needs special CBSA handling
- ⚠️ **No documentation** on county_to_cbsa.csv source or update frequency

---

### 4-9. Other `*-rollups.r` Scripts

All follow the **same pattern** as `bg-housing-rollups.r`:

| Script | Base Table | Parent Geographies | Special Notes |
|--------|------------|-------------------|---------------|
| `tract-housing-rollups.r` | `hu_tract` | CBSA, County, State, US | Same as BG pattern |
| `county-housing-rollups.r` | `hu_county` | CBSA, State, US | No sub-county parents |
| `cbsa-housing-rollups.r` | `hu_cbsa` | US only | Only national comparison |
| `zcta-housing-rollups.r` | `hu_zcta` | State, US | Uses zcta_to_state crosswalk |
| `cousub-housing-rollups.r` | `hu_cousub` | State, US | County subdivisions |
| `place-housing-rollups.r` | `hu_place` | State, US | Cities/towns/CDPs |

**Common Documentation Gaps**:
- ⚠️ **Copy-paste comments** - All say "ZCTA TO STATE; CORE ADAPTED FROM 'zcta-to-county.r'" even when not relevant
- ⚠️ **Inconsistent NA handling** - Some filter `!is.na()`, others don't
- ⚠️ **No error handling** - Silent failures if base tables missing
- ⚠️ **Hardcoded database paths** - Should use config file or environment variables

---

### 10. `zcta-to-county.r`

**Purpose**: Creates 1:1 ZCTA→County assignment based on maximum land area overlap.

**Algorithm**:
```
Load Census ZCTA-County relationship file
  ↓
Calculate overlap metrics:
  - area_share_pct = ZCTA share in each county
  - area_county_pct = County share from each ZCTA
  - score = area_share_pct × area_county_pct
  ↓
Rank by score (highest = best match)
  ↓
Select rank 1 for each ZCTA (unique assignment)
  ↓
Write to zcta_to_county.csv
```

**Input**:
- `./data/tab20_zcta520_county20_natl.txt` (Census relationship file)

**Output**:
- `./data/zcta_to_county.csv` (2 columns: zcta, county_fips_code)

**Documentation Gaps**:
- ⚠️ **No documentation** on Census file source URL
- ⚠️ **Unclear** if this is run manually or part of pipeline
- ⚠️ **No validation** that all ZCTAs get assigned

---

## Input Requirements

### External Data Files (Census Bureau)

#### Primary Data (Downloaded by prd-housing-units-update.r)
| File Pattern | Source | Location | Update Frequency |
|-------------|--------|----------|------------------|
| `{FIPS}_{State}_AddressBlockCountList_062022.txt` | Census 2020 | `/home/joel/data/geo/` | Static (baseline) |
| `{FIPS}_{State}_AddressBlockCountList_072024.txt` | Annual 2024 | `/home/joel/data/geo/` | Annual (July) |
| `{FIPS}_{State}_AddressBlockCountList_072025.txt` | Annual 2025 | `/home/joel/data/geo/` | Annual (July) |

**Download URL**: `https://www2.census.gov/geo/pvs/addcountlisting/{year}/{filename}`

**States Covered**: 51 (50 states + DC) - **excludes territories** (PR, VI, GU, AS, MP)

#### Crosswalk Files (Static Reference)
| File | Source | Location | Purpose |
|------|--------|----------|---------|
| `tab20_zcta520_tabblock20_natl.txt` | Census | `/home/joel/data/geo/` | Block→ZCTA lookup |
| `ct-2022blockcrosswalk.csv` | Census/State | `/home/joel/data/` | CT 2020→2022 blocks |
| `county-adjacency.txt` | Census | `/home/joel/data/geo/` | County metadata |
| `county-cbsa-lookup.csv` | OMB | `/home/joel/data/` | County→Metro area |

#### GEOCORR Files (For georeference.duckdb Setup)
| File Pattern | Source | Location | Purpose |
|-------------|--------|----------|---------|
| `block-place-*.csv` | UMissouri GEOCORR | `/home/joel/data/geo/` | Block→Place (52 files) |
| `block-cosub-*.csv` | UMissouri GEOCORR | `/home/joel/data/geo/` | Block→County Sub (52 files) |
| `block-ua-*.csv` | UMissouri GEOCORR | `/home/joel/data/geo/` | Block→Urban Area (52 files) |

**GEOCORR Website**: `https://mcdc.missouri.edu/applications/geocorr2022.html`

**Documentation Gaps**:
- ⚠️ **No source URLs** documented for Census crosswalk files
- ⚠️ **No version tracking** - Which vintage of CBSA definitions?
- ⚠️ **No update process** - How to refresh when OMB releases new metro definitions?

### Database Dependencies

#### georeference.duckdb
**Location**: `/home/joel/data/georeference.duckdb`

**Required Tables**:
- `block_place`: Block→Place assignments (~2M rows)
- `block_cosub`: Block→County Subdivision (~1.5M rows)
- `block_ua`: Block→Urban Area (~3M rows)
- `county_to_cbsa`: County→CBSA lookup (~1,900 rows)

**Creation Method**: ✅ **`block-lookups.r`** (see Script Details section)
- **Source**: University of Missouri GEOCORR website (`https://mcdc.missouri.edu/applications/geocorr2022.html`)
- **Input**: 156 CSV files (52 states × 3 geography types) exported from GEOCORR
- **Processing**: Consolidates state-specific files into master lookup tables
- **Runtime**: ~20 minutes (one-time setup)
- **Rebuild**: Only needed after Census boundary updates or GEOCORR definition changes

#### spatial_storage.duckdb
**Location**: `/home/joel/data/spatial_storage.duckdb`

**Required Tables**:
- `geo_block_group`: Block group geometries + names
- `geo_cbsa_23`: CBSA geometries + names
- `geo_cosub_23`: County subdivision geometries + names
- `geo_place_23`: Place geometries + names

**Usage**: Name lookups only (geometries not used in calculations)

**Creation Method**: ❓ **UNDOCUMENTED**

#### duckdb_metadata.duckdb
**Location**: `/home/joel/data/duckdb_metadata.duckdb`

**Tables**:
- `column_metadata`: Field descriptions
- `table_summary`: Table documentation

**Usage**: Referenced in rollup scripts but **not actually used** in calculations

---

## Output Artifacts

### DuckDB Database: housing.duckdb

**Location**: `/home/joel/data/housing.duckdb`
**Size**: ~2-5 GB
**Tables**: 17

#### Base Tables (Housing Unit Counts)
| Table | Rows | Key Field | Description |
|-------|------|-----------|-------------|
| `hu_block` | ~11M | `block_geoid` (15-digit) | Census blocks |
| `hu_block_group` | ~242K | `block_group` (12-digit) | Block groups |
| `hu_tract` | ~85K | `tract` (11-digit) | Census tracts |
| `hu_county` | ~3,200 | `co_fips` (5-digit) | Counties |
| `hu_state` | 51 | `state_code` (2-digit) | States + DC |
| `hu_us` | 1 | N/A | National total |
| `hu_zcta` | ~33K | `zcta_20` (5-digit) | ZIP Code areas |
| `hu_place` | ~30K | `place` (5-digit) | Cities/towns/CDPs |
| `hu_cousub` | ~36K | `cousub` (5-digit) | County subdivisions |
| `hu_ua` | ~3,500 | `ua` (5-digit) | Urban areas |
| `hu_cbsa` | ~950 | `cbsa23` (5-digit) | Metro/micro areas |

#### Index Tables (Comparative Metrics)
| Table | Rows | Purpose |
|-------|------|---------|
| `hu_block_group_indexes` | ~242K | BG vs. CBSA/County/State/US |
| `hu_tract_indexes` | ~85K | Tract vs. CBSA/County/State/US |
| `hu_county_indexes` | ~3,200 | County vs. CBSA/State/US |
| `hu_zcta_indexes` | ~33K | ZCTA vs. State/US |
| `hu_cousub_indexes` | ~36K | County sub vs. State/US |
| `hu_place_indexes` | ~30K | Place vs. State/US |
| `hu_cbsa_indexes` | ~950 | CBSA vs. US |

#### Metadata Table
| Table | Rows | Purpose |
|-------|------|---------|
| `metadata` | Variable | Column documentation |

### RDS Files

**Location**: `/home/joel/data/`

| File | Size | Purpose |
|------|------|---------|
| `all_state_results.rds` | Large | Full state-by-state nested list |
| `combined_block_data.rds` | Largest | All blocks (not recommended for loading) |
| `combined_block_group_data.rds` | Medium | Block groups with indices |
| `combined_tract_data.rds` | Small | Tracts with indices |
| `combined_county_data.rds` | Small | Counties with indices |
| `combined_zcta_data.rds` | Small | ZCTAs with indices |
| `combined_cousub_data.rds` | Medium | County subs with indices |
| `combined_place_data.rds` | Medium | Places with indices |
| `combined_ua_data.rds` | Small | Urban areas with indices |
| `hu_state.rds` | Tiny | State totals |
| `hu_us.rds` | Tiny | National totals |
| `hu_cbsa.rds` | Small | Metro areas |

**Note**: DuckDB tables are **preferred** for querying. RDS files are backups/legacy.

---

## Documentation Gaps & Issues

### Critical Gaps

1. **✅ RESOLVED: georeference.duckdb Documentation** (Updated 2025-11-25)
   - **Source**: University of Missouri GEOCORR (`https://mcdc.missouri.edu/applications/geocorr2022.html`)
   - **Build script**: `block-lookups.r` (see Script Details section)
   - **Input files**: 156 GEOCORR CSV exports (block-place, block-cosub, block-ua)
   - ⚠️ **Still unclear**: How to rebuild `spatial_storage.duckdb` if corrupted?
   - ⚠️ **Still unclear**: What creates `county-cbsa-lookup.csv`?

2. **⚠️ No Error Recovery**
   - What if one state fails to process?
   - How to detect incomplete/corrupted Census files?
   - No validation that all geographies have valid indices

3. **⚠️ Hardcoded Values Need Parameterization**
   - Time periods (4.33, 5.33 years) should be calculated from dates
   - Year "2025" hardcoded in multiple places
   - Database paths should be in config file

4. **⚠️ Connecticut Special Handling Unclear**
   - Why do some BGs get `cbsa23 = "CT000"`?
   - Are Connecticut planning regions properly documented?
   - Do Connecticut geographies match boundary files?

5. **⚠️ Missing Input Validation**
   - No check that Census files contain expected columns
   - No row count validation after aggregation
   - No comparison to prior year to detect anomalies

### Data Quality Issues

1. **UTF-8 Encoding Errors**
   - CBSA names cause DuckDB errors (line 622-628 in main script)
   - Workaround: CBSA names excluded from `hu_cbsa` table
   - **Impact**: Applications must join to `spatial_storage.duckdb::geo_cbsa_23` for names

2. **Commented-Out Code**
   - Large sections commented without explanation (lines 465-511)
   - Suggests uncertainty about state-spanning geography aggregation
   - **Risk**: May be silently computing wrong totals for ZCTA/Place/UA

3. **State List Inconsistencies**
   - `prd-housing-units-update.r` uses different state list format than main script
   - Potential for mismatch if one is updated without the other

### Unclear Inputs

| Input | Location | Source | Update Process | Status |
|-------|----------|--------|----------------|--------|
| `block_place` table | `georeference.duckdb` | ✅ UMissouri GEOCORR | Run `block-lookups.r` | **RESOLVED** |
| `block_cosub` table | `georeference.duckdb` | ✅ UMissouri GEOCORR | Run `block-lookups.r` | **RESOLVED** |
| `block_ua` table | `georeference.duckdb` | ✅ UMissouri GEOCORR | Run `block-lookups.r` | **RESOLVED** |
| `county-cbsa-lookup.csv` | `/home/joel/data/` | ❓ OMB/Manual? | ❓ Unknown | **UNCLEAR** |
| `ct-2022blockcrosswalk.csv` | `/home/joel/data/` | Census/CT State | ❓ Manual download? | **UNCLEAR** |

### Recommendations for Next Version

1. ✅ **COMPLETED**: Script to rebuild `georeference.duckdb` from scratch (`block-lookups.r`)
2. **Create Input Manifest**: Document all required files with source URLs and update schedules
3. **Add Validation Layer**: Row counts, null checks, range validation after each aggregation
4. **Parameterize Time Periods**: Calculate from actual Census reference dates
5. **Error Logging**: Write detailed log file with processing stats for each state
6. **Config File**: Move hardcoded paths/parameters to YAML config
7. **Test Suite**: Create test cases with known-good Texas/California data
8. **Document**: Source and update process for `county-cbsa-lookup.csv` and Connecticut crosswalk

---

## Database Connections

### Standard Connection Pattern

```r
# Main housing database (read/write)
conh <- dbConnect(duckdb('./data/housing.duckdb'))

# Geographic reference lookups (read-only)
congref <- dbConnect(duckdb('./data/georeference.duckdb'))

# Metadata (read-only, optional)
conmeta <- dbConnect(duckdb('./data/duckdb_metadata.duckdb'))

# Spatial geometries (read-only, spatial extension required)
congeo <- dbConnect(
  duckdb::duckdb(),
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,  # Why not read_only = TRUE?
  extensions = c("spatial")
)

# Always disconnect when done
dbDisconnect(conh)
dbDisconnect(congref)
dbDisconnect(conmeta)
dbDisconnect(congeo)
```

**Warning**: Scripts do **not consistently disconnect** databases. May cause lock issues if script interrupted.

---

## Common Patterns

### Geographic Hierarchy Extraction

All scripts use substring to derive parent geographies:

```r
# Block (15-digit) → Block Group (12-digit)
hu_bg[, block_group := substring(block_geoid, 1, 12)]

# Block Group (12-digit) → Tract (11-digit)
hu_tract[, tract := substring(block_group, 1, 11)]

# Tract (11-digit) → County (5-digit)
hu_county[, co_fips := substring(tract, 1, 5)]

# County (5-digit) → State (2-digit)
hu_state[, state_fips := substring(co_fips, 1, 2)]
```

### Relative Index Calculation

Pattern used in all `*-rollups.r` scripts:

```r
# Merge base geography with parent geography
hu_merged <- merge(hu_base, hu_parent, by = join_key)

# Calculate relative index (100 = same growth rate as parent)
hu_merged[, idx_20_24_parent :=
  (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100
]

# .x = base geography growth index
# .y = parent geography growth index
# Subtract 1 to convert index to growth rate
# Divide to get relative rate
# Multiply by 100 for percentile-like scale
```

### Percentile Calculation

Uses `data.table::frank()` for national percentile ranks:

```r
hu_data[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24, ties.method = "min", na.last = "keep") / .N * 100)
)]

# frank() assigns ranks (1, 2, 3, ...)
# ties.method = "min" gives tied values the lowest rank
# na.last = "keep" preserves NA positions
# Divide by .N (row count) and multiply by 100 for 1-100 scale
# ceiling() rounds up to ensure 1-100 range
```

### Cartesian Product for US-Level Merge

Trick used to attach US benchmark to all records:

```r
# Add dummy match key to both tables
hu_base[, matchid := 1]
hu_us[, matchid := 1]

# Merge creates Cartesian product (every row × US row)
hu_merged <- merge(hu_base, hu_us, by = "matchid")

# Now US metrics (.y) available for all base geography rows (.x)
```

---

## Production Checklist

### Initial System Setup (One-Time)

- [ ] **Download GEOCORR files** from University of Missouri:
  - [ ] Go to `https://mcdc.missouri.edu/applications/geocorr2022.html`
  - [ ] Export Block→Place for all states (52 files)
  - [ ] Export Block→County Subdivision for all states (52 files)
  - [ ] Export Block→Urban Area for all states (52 files)
  - [ ] Save to `/home/joel/data/geo/` with naming: `block-{geography}-{state}.csv`
- [ ] **Run `block-lookups.r`** to build `georeference.duckdb`
  - [ ] Verify 156 CSV files present
  - [ ] Execute script: `source("01_housing/block-lookups.r")`
  - [ ] Check output: `block_place`, `block_cosub`, `block_ua` tables created
  - [ ] Validate row counts: ~2M places, ~1.5M cosubs, ~3M urban areas

### Pre-Execution

- [ ] Verify working directory: `/home/joel`
- [ ] Check disk space: Need ~10 GB free for processing
- [ ] Load environment variables: `.env` file present
- [ ] Verify DuckDB extension: `spatial` extension installed
- [ ] **Verify `georeference.duckdb` exists** (if not, run Initial Setup above)
- [ ] Backup existing `housing.duckdb` (optional but recommended)

### Data Download (Step 1)

- [ ] Check Census website for new data releases
- [ ] Update year in `prd-housing-units-update.r` if needed
- [ ] Run download script: `source("01_housing/prd-housing-units-update.r")`
- [ ] Verify 51 files downloaded (one per state)
- [ ] Spot-check file sizes (should be >100 KB each)

### Master Processing (Step 2)

- [ ] Run main script: `source("01_housing/prd-housing-units-2025.R")`
- [ ] Monitor console for "Processing [State]..." messages
- [ ] Verify all 51 states processed without errors
- [ ] Check DuckDB tables created: `dbListTables(conh)`
- [ ] Validate row counts match expected (see Output Artifacts table)

### Index Generation (Step 3)

For each rollup script:
- [ ] Run script: `source("01_housing/bg-housing-rollups.r")` (etc.)
- [ ] Verify `*_indexes` table written to DuckDB
- [ ] Spot-check percentile distributions (should range 1-100)
- [ ] Check for excessive NAs in index columns

### Post-Execution Validation

- [ ] Query sample records from each table
- [ ] Verify growth rates are reasonable (-5% to +10% annually)
- [ ] Check for states with suspiciously high/low totals
- [ ] Compare to prior year totals (should be monotonically increasing)
- [ ] Disconnect all database connections

### Documentation Updates

- [ ] Update this document with any new issues discovered
- [ ] Log runtime for each script (for capacity planning)
- [ ] Document any manual interventions required
- [ ] Update `/docs/housing-layouts-and-rules.md` if schema changed

---

## Quick Reference: Key Metrics

### Housing Unit Fields

| Field | Description | Example |
|-------|-------------|---------|
| `HU_20` | Housing units, April 1, 2020 (Census baseline) | 150,234 |
| `HU_24` | Housing units, July 1, 2024 (Annual estimate) | 155,891 |
| `HU_25` | Housing units, July 1, 2025 (Latest estimate) | 157,456 |

### Growth Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| `hg_20_24` | HU_24 - HU_20 | Absolute growth (raw count) |
| `hgi_20_24` | HU_24 / HU_20 | Growth index (1.05 = 5% growth) |
| `cagr_20_24` | ((HU_24/HU_20)^(1/4.33)) - 1 | Annualized growth rate |
| `agr_25` | (HU_25/HU_24) - 1 | Annual growth rate 2024-25 |

### Comparative Indices

| Index | Formula | Example Interpretation |
|-------|---------|----------------------|
| `idx_20_24_state` | (BG_growth / State_growth) × 100 | 150 = BG growing 1.5× faster than state |
| `pctl_20_24_us` | National percentile rank (1-100) | 95 = Faster than 95% of all BGs |

---

## Support & Contact

**Documentation Issues**: Contact data engineering team
**Data Quality Questions**: Review `/docs/housing-layouts-and-rules.md`
**Census Data Updates**: Monitor `https://www.census.gov/programs-surveys/popest.html`

---

## Document Metadata

**Generated By**: Claude Code (Sonnet 4.5)
**Generation Date**: 2025-11-25
**Last Updated**: 2025-11-25 (Added GEOCORR documentation for `block-lookups.r`)
**Source Analysis**: 12 R scripts in `/home/joel/01_housing/`
**Documentation Standard**: Internal Technical Reference
**Next Review**: Upon next Census data release (July 2026)

**Major Updates**:
- 2025-11-25: Added comprehensive documentation for `block-lookups.r` script
- 2025-11-25: Documented University of Missouri GEOCORR as source for geographic crosswalks
- 2025-11-25: Resolved "Unclear Inputs" for `georeference.duckdb` tables (block_place, block_cosub, block_ua)
- 2025-11-25: Added Initial System Setup section to Production Checklist

---

## Appendix: Sample Queries

### Check processing status
```r
dbGetQuery(conh, "SELECT COUNT(*) as block_groups FROM hu_block_group")
# Expected: ~242,000
```

### Find fastest-growing counties
```r
dbGetQuery(conh, "
  SELECT county_name, state_name, cagr_20_25
  FROM hu_county
  WHERE HU_20 > 10000
  ORDER BY cagr_20_25 DESC
  LIMIT 10
")
```

### Validate index calculations
```r
# All percentiles should range 1-100
dbGetQuery(conh, "
  SELECT
    MIN(pctl_20_24_us) as min_pctl,
    MAX(pctl_20_24_us) as max_pctl,
    COUNT(*) as total_rows
  FROM hu_block_group_indexes
")
```

---

**END OF DOCUMENTATION**
