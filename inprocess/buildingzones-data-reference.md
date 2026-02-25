## Introduction

**BuildingZones** from Tidy Analytics is a housing unit data product providing count and growth measurements for U.S. residential housing units across eleven geographic levels. This reference documents the delivered data tables, their fields, calculation rules, and geographic inventories.

**Data Vintage:** April 2020 · July 2024 · July 2025 · November 2025
**Last Updated:** 2026-02-24
**Version:** 3.0

---

```{=typst}
#pagebreak()
```

## Table of Contents

1. [Source Data](#source-data)
2. [Delivered Files](#delivered-files)
3. [Geographic Levels](#geographic-levels)
4. [Base Measure Columns](#base-measure-columns)
5. [Derived Growth Metrics](#derived-growth-metrics)
6. [Index Columns](#index-columns)
7. [Percentile Columns](#percentile-columns)
8. [Table Dictionaries](#table-dictionaries)
9. [Calculation Rules](#calculation-rules)
10. [Appendix A: State Inventory](#appendix-a-state-inventory)
11. [Appendix B: CBSA Inventory (Top 50)](#appendix-b-cbsa-inventory)

---

```{=typst}
#pagebreak()
```

## Source Data

All housing unit counts originate from U.S. Census Bureau Address Block Count List files. No other primary data sources are used for housing unit counts.

::: {.tbl tbl-colwidths="[0.15, 0.15, 0.20, 0.35, 0.15]"}
| Vintage Label | Reference Date | Release | File Pattern | Approx. Block Records |
|---|---|---|---|---|
| `HU_20_apr` | April 1, 2020 | June 2022 | `{STATEFP}_{StateName}_AddressBlockCountList_062022.txt` | ~11M |
| `HU_24_jul` | July 1, 2024 | July 2024 | `{STATEFP}_{StateName}_AddressBlockCountList_072024.txt` | ~11M |
| `HU_25_jul` | July 1, 2025 | July 2025 | `{STATEFP}_{StateName}_AddressBlockCountList_072025.txt` | ~11M |
| `HU_25_nov` | November 1, 2025 | December 2025 | `{STATEFP}_{StateName}_AddressBlockCountList_122025.txt` | ~11M |
:::

Each file covers one state and contains one record per census block (or block-part). The April 2020 vintage is sourced from the 2020 Decennial Census Address Block Count List; all subsequent vintages are from the Census Bureau's ongoing Address List Count program.

Each source file provides two count fields per block:

- `total_housing_units` — the total housing unit count for the block
- `total_group_quarters` — the group quarters count for the block

Group quarters (dormitories, prisons, nursing facilities, etc.) are tracked separately at every geographic level but are **not** included in housing unit counts.

**Connecticut note:** Connecticut reorganized from counties to planning regions effective 2022. Source block GEOIDs for Connecticut are remapped from 2020 block FIPS codes to 2022 planning-region-based block FIPS codes prior to aggregation, ensuring geographic identifiers align with current Census boundary files.

---

```{=typst}
#pagebreak()
```

## Delivered Files

The delivery consists of three parallel representations of the same eleven geographic data layers, plus an integrated DuckDB database containing all layers and a metadata table.

### File Inventory

::: {.tbl tbl-colwidths="[0.35, 0.10, 0.10, 0.45]"}
| Layer | Parquet | CSV | DuckDB Table |
|---|---|---|---|
| Census Block | `block.parquet` | `block.csv` | `block` |
| Census Block Group | `block_group.parquet` | `block_group.csv` | `block_group` |
| Census Tract | `census_tract.parquet` | `census_tract.csv` | `census_tract` |
| County | `county.parquet` | `county.csv` | `county` |
| County Subdivision | `county_subdivision.parquet` | `county_subdivision.csv` | `county_subdivision` |
| ZCTA | `zcta.parquet` | `zcta.csv` | `zcta` |
| Place | `place.parquet` | `place.csv` | `place` |
| Urban Area | `urban_area.parquet` | `urban_area.csv` | `urban_area` |
| CBSA | `cbsa.parquet` | `cbsa.csv` | `cbsa` |
| State | `state.parquet` | `state.csv` | `state` |
| US | `us.parquet` | `us.csv` | `us` |
:::

Parquet files are located in the `parquet/` subdirectory; CSV files in the `csv/` subdirectory. All three formats — Parquet, CSV, and DuckDB table — contain identical rows and columns for each layer. The `metadata` table (column definitions) is present only in the DuckDB file; it is not produced as a flat file.

### Format Notes

**Parquet (`parquet/*.parquet`)** — Apache Parquet columnar format. Recommended for programmatic access and large-scale analytical workflows. Column data types (INTEGER, DOUBLE, VARCHAR) are preserved exactly as defined. Null values are represented as Parquet nulls.

**CSV (`csv/*.csv`)** — Comma-separated values, UTF-8 encoded. All columns present; no header changes relative to Parquet or DuckDB. Null values are represented as empty fields. Numeric columns are written as plain numbers without formatting. Note that the `block` layer CSV is large (~8.1M rows) and may require chunked reading in memory-constrained environments.

**DuckDB (`housing_distro.duckdb`)** — Single-file analytical database containing all eleven layers plus the `metadata` table. Suitable for multi-table joins, cross-layer analysis, and SQL-based workflows without requiring separate files per layer.

### Column Consistency

All three formats share identical column names and ordering for each layer. The schemas documented in [Table Dictionaries](#table-dictionaries) apply equally to the Parquet files, CSV files, and DuckDB tables.

---

```{=typst}
#pagebreak()
```

## Delivered Database

The DuckDB database file contains eleven data tables plus one metadata table.

**File:** `housing_distro.duckdb`
**Format:** DuckDB

::: {.tbl tbl-colwidths="[0.22, 0.12, 0.22, 0.44]"}
| Table | Row Count | Primary Key(s) | Description |
|---|---|---|---|
| `block` | 8,132,974 | `block_geoid` | Census blocks — base counts only, no growth metrics |
| `block_group` | 240,075 | `block_group` | Census block groups with full growth metrics and indexes |
| `census_tract` | 84,557 | `tract` | Census tracts with full growth metrics and indexes |
| `county` | 3,175 | `county_fips` | Counties with full growth metrics and indexes |
| `county_subdivision` | 38,136 | `county_fips` + `cousub` | Minor civil divisions (MCDs) with full growth metrics and indexes |
| `zcta` | 33,642 | `zcta_20` | ZIP Code Tabulation Areas with growth metrics and indexes |
| `place` | 31,670 | `place` + `state_code` | Incorporated places and CDPs with growth metrics and indexes |
| `urban_area` | 2,848 | `ua` + `state_code` | Urban areas with growth metrics and indexes |
| `cbsa` | 920 | `cbsa23` | Core-Based Statistical Areas (metro/micro areas) with growth metrics and indexes |
| `state` | 51 | `state_code` | States and the District of Columbia with growth metrics |
| `us` | 1 | *(none)* | National totals with growth metrics |
| `metadata` | 716 | *(none)* | Column definitions for all data tables |
:::

The `metadata` table contains one row per column across all data tables, with fields: `table_name`, `column_name`, `data_type`, `is_nullable`, `cid` (column position), and `column_description`.

---

```{=typst}
#pagebreak()
```

## Geographic Levels

Each geographic level corresponds to one table. The eleven levels range from census block (smallest) to national total (largest).

::: {.tbl tbl-colwidths="[0.20, 0.15, 0.20, 0.45]"}
| Geographic Level | Key Format | Example | Notes |
|---|---|---|---|
| **Block** | 15-digit GEOID | `481410010021000` | STATE(2) + COUNTY(3) + TRACT(6) + BLOCK(4). Base counts only; no growth metrics. |
| **Block Group** | 12-digit GEOID | `481410010021` | STATE(2) + COUNTY(3) + TRACT(6) + BG(1). |
| **Census Tract** | 11-digit GEOID | `48141001002` | STATE(2) + COUNTY(3) + TRACT(6). |
| **County** | 5-digit FIPS | `48141` | STATE(2) + COUNTY(3). |
| **County Subdivision** | `county_fips`(5) + `cousub`(5) | `48141` + `90185` | Composite key. Minor civil divisions (townships, boroughs, etc.). |
| **ZCTA** | 5-digit code | `78701` | 2020 Census ZCTA vintage. |
| **Place** | `place`(5) + `state_code`(2) | `15976` + `48` | Composite key. Incorporated cities, towns, villages, and CDPs. Places crossing state lines carry one row per state. |
| **Urban Area** | `ua`(5) + `state_code`(2) | `17860` + `48` | Composite key. 2020 Census urban areas. Urban areas crossing state lines carry one row per state. |
| **CBSA** | 5-digit code | `35620` | 2023 OMB CBSA delineations. Metro and micropolitan statistical areas. |
| **State** | 2-digit FIPS | `48` | 50 states plus the District of Columbia. |
| **US** | *(none)* | — | Single national total row. |
:::

---

```{=typst}
#pagebreak()
```

## Base Measure Columns

These columns appear in all tables (except the `block` table, which omits growth metrics; see below).

### Housing Unit and Group Quarters Counts

::: {.tbl tbl-colwidths="[0.22, 0.12, 0.15, 0.51]"}
| Column | Type | Nullable | Description |
|---|---|---|---|
| `HU_20_apr` | INTEGER | Yes | Housing units, April 1, 2020 (2020 Decennial Census reference date) |
| `gq_20_apr` | INTEGER | Yes | Group quarters, April 1, 2020 |
| `HU_24_jul` | INTEGER | Yes | Housing units, July 1, 2024 |
| `gq_24_jul` | INTEGER | Yes | Group quarters, July 1, 2024 |
| `HU_25_jul` | INTEGER | Yes | Housing units, July 1, 2025 |
| `gq_25_jul` | INTEGER | Yes | Group quarters, July 1, 2025 |
| `HU_25_nov` | INTEGER | Yes | Housing units, November 1, 2025 |
| `gq_25_nov` | INTEGER | Yes | Group quarters, November 1, 2025 |
| `block_recs` | INTEGER | Yes | Number of source census blocks aggregated into this record |
:::

**Group quarters** are tracked separately and are not included in housing unit counts. Group quarters include college/university student housing, correctional facilities, nursing facilities, military barracks, and other institutionalized or non-institutionalized group living arrangements.

### Block Table Notes

The `block` table contains **base counts only** — the eight HU/GQ vintage columns and `block_recs` (always 1 for blocks) — plus geographic crosswalk columns used internally during aggregation. No growth metrics or index columns appear at the block level.

---

```{=typst}
#pagebreak()
```

## Derived Growth Metrics

Growth metrics are present in all tables except `block`. Each metric covers a specific time interval between vintages.

### Time Intervals

::: {.tbl tbl-colwidths="[0.25, 0.18, 0.12, 0.45]"}
| Interval Label | From → To | Years | Description |
|---|---|---|---|
| `20_apr_24_jul` | Apr 2020 → Jul 2024 | 4.25 | Full period, 2020 Census to mid-2024 |
| `24_jul_25_jul` | Jul 2024 → Jul 2025 | 1.00 | One-year period, July to July |
| `24_jul_25_nov` | Jul 2024 → Nov 2025 | 1.33 | 16-month period, mid-2024 to late 2025 |
| `25_jul_25_nov` | Jul 2025 → Nov 2025 | 0.33 | 4-month period within 2025 |
| `20_apr_25_jul` | Apr 2020 → Jul 2025 | 5.25 | Full period to July 2025 |
| `20_apr_25_nov` | Apr 2020 → Nov 2025 | 5.58 | Full period to November 2025 |
:::

### Growth Metric Column Types

For each time interval above, three metric types are provided (where applicable):

::: {.tbl tbl-colwidths="[0.08, 0.30, 0.12, 0.50]"}
| Prefix | Column Pattern | Type | Description |
|---|---|---|---|
| `hg_` | `hg_{interval}` | INTEGER | **Housing Growth** — absolute change in housing units |
| `hgi_` | `hgi_{interval}` | DOUBLE | **Housing Growth Index** — ratio of ending to starting count |
| `cagr_` | `cagr_{interval}` | DOUBLE | **Compound Annual Growth Rate** — annualized growth rate (decimal) |
| `agr_` | `agr_{interval}` | DOUBLE | **Annual Growth Rate** — simple one-year rate (decimal, for 1-year intervals only) |
:::

**Note:** `agr_` (annual growth rate, simple) is used only for the exact one-year interval `24_jul_25_jul`. All other multi-year intervals use `cagr_` (compound annual growth rate).

### Complete Growth Metric Column List

::: {.tbl tbl-colwidths="[0.38, 0.12, 0.50]"}
| Column | Type | Description |
|---|---|---|
| `hg_20_apr_24_jul` | INTEGER | Housing unit change, Apr 2020 → Jul 2024 |
| `hgi_20_apr_24_jul` | DOUBLE | Housing growth index, Apr 2020 → Jul 2024 |
| `cagr_20_apr_24_jul` | DOUBLE | CAGR, Apr 2020 → Jul 2024 (4.25 years) |
| `hg_24_jul_25_jul` | INTEGER | Housing unit change, Jul 2024 → Jul 2025 |
| `hgi_24_jul_25_jul` | DOUBLE | Housing growth index, Jul 2024 → Jul 2025 |
| `agr_24_jul_25_jul` | DOUBLE | Annual growth rate, Jul 2024 → Jul 2025 (1.00 year) |
| `hg_24_jul_25_nov` | INTEGER | Housing unit change, Jul 2024 → Nov 2025 |
| `hgi_24_jul_25_nov` | DOUBLE | Housing growth index, Jul 2024 → Nov 2025 |
| `cagr_24_jul_25_nov` | DOUBLE | CAGR, Jul 2024 → Nov 2025 (1.33 years) |
| `hg_25_jul_25_nov` | INTEGER | Housing unit change, Jul 2025 → Nov 2025 |
| `hgi_25_jul_25_nov` | DOUBLE | Housing growth index, Jul 2025 → Nov 2025 |
| `cagr_25_jul_25_nov` | DOUBLE | CAGR, Jul 2025 → Nov 2025 (0.33 years) |
| `hg_20_apr_25_jul` | INTEGER | Housing unit change, Apr 2020 → Jul 2025 |
| `hgi_20_apr_25_jul` | DOUBLE | Housing growth index, Apr 2020 → Jul 2025 |
| `cagr_20_apr_25_jul` | DOUBLE | CAGR, Apr 2020 → Jul 2025 (5.25 years) |
| `hg_20_apr_25_nov` | INTEGER | Housing unit change, Apr 2020 → Nov 2025 |
| `hgi_20_apr_25_nov` | DOUBLE | Housing growth index, Apr 2020 → Nov 2025 |
| `cagr_20_apr_25_nov` | DOUBLE | CAGR, Apr 2020 → Nov 2025 (5.58 years) |
:::

---

```{=typst}
#pagebreak()
```

## Index Columns

Index columns measure a geography's growth **relative to a larger containing or reference geography**. An index of 100 means growth exactly matched the baseline; values above 100 indicate faster growth than the baseline; values below 100 indicate slower growth.

### Index Baselines by Table

Not all baseline geographies are available for every table. The available indexes vary by geographic level:

::: {.tbl tbl-colwidths="[0.22, 0.78]"}
| Table | Available Index Baselines |
|---|---|
| `block_group` | County, CBSA, State, US |
| `census_tract` | County, CBSA, State, US |
| `county` | CBSA, State, US |
| `county_subdivision` | County, CBSA, State, US |
| `zcta` | State, US |
| `place` | State, US |
| `urban_area` | State, US |
| `cbsa` | US |
| `state` | US |
| `us` | *(none — US is the top-level baseline)* |
:::

### Index Baseline Label Columns

Each index baseline includes a label column identifying the specific baseline geography used for that row:

::: {.tbl tbl-colwidths="[0.25, 0.12, 0.63]"}
| Column | Type | Description |
|---|---|---|
| `county_fips` | VARCHAR | FIPS code of the county used as the county-level baseline |
| `idx_county_base` | VARCHAR | Name of the county baseline (e.g., `"Autauga County, AL"`) |
| `cbsa23` | VARCHAR | CBSA code of the metro/micro area used as the CBSA-level baseline |
| `idx_cbsa_base` | VARCHAR | Name of the CBSA baseline (e.g., `"Dallas-Fort Worth-Arlington, TX Metro Area"`) |
| `state_fips` | VARCHAR | FIPS code of the state used as the state-level baseline |
| `idx_state_base` | VARCHAR | Name of the state baseline (e.g., `"Texas"`) |
:::

Not all baseline columns are present in every table — only the baselines applicable to that geographic level are included.

### Index Column Naming Convention

Index columns follow the pattern: `idx_{baseline}_{metric}_{interval}`

Where:
- `{baseline}` is `county`, `cbsa`, `state`, or `us`
- `{metric}` is `hgi`, `cagr`, or `agr`
- `{interval}` is the time interval label (e.g., `20_apr_24_jul`)

**Example:** `idx_county_cagr_20_apr_25_nov` — the block group's CAGR for Apr 2020 → Nov 2025, indexed against its containing county.

### Index Calculation

For `hgi_`-based indexes:

> `idx = (geography_hgi - 1) / (baseline_hgi - 1) × 100`

For `cagr_`- and `agr_`-based indexes:

> `idx = geography_rate / baseline_rate × 100`

A result of 100 means the geography grew at exactly the same rate as the baseline. Results are expressed as a dimensionless score, not a percentage point difference.

**Null values:** Index values are null when the baseline geography's denominator metric is zero or null (e.g., a county with no housing units in 2020).

---

```{=typst}
#pagebreak()
```

## Percentile Columns

Percentile columns (`pctl_`) rank each geography within the national US distribution. They appear in the following tables: `block_group`, `census_tract`, `county`, `county_subdivision`, `zcta`, `place`, `urban_area`, `cbsa`.

Percentiles are computed against the US distribution of the same geographic level. For example, `pctl_us_cagr_20_apr_25_nov` in the `block_group` table ranks each block group among all ~240,000 U.S. block groups.

**Range:** 1–100 (integer). A value of 99 means the geography's growth rate is in the top 1% nationally.

**Null values:** Percentile is null when the underlying metric is null.

### Percentile Column List

::: {.tbl tbl-colwidths="[0.42, 0.12, 0.46]"}
| Column | Type | Description |
|---|---|---|
| `pctl_us_hgi_20_apr_24_jul` | INTEGER | US percentile rank, housing growth index, Apr 2020 → Jul 2024 |
| `pctl_us_hgi_24_jul_25_jul` | INTEGER | US percentile rank, housing growth index, Jul 2024 → Jul 2025 |
| `pctl_us_hgi_24_jul_25_nov` | INTEGER | US percentile rank, housing growth index, Jul 2024 → Nov 2025 |
| `pctl_us_hgi_25_jul_25_nov` | INTEGER | US percentile rank, housing growth index, Jul 2025 → Nov 2025 |
| `pctl_us_hgi_20_apr_25_jul` | INTEGER | US percentile rank, housing growth index, Apr 2020 → Jul 2025 |
| `pctl_us_hgi_20_apr_25_nov` | INTEGER | US percentile rank, housing growth index, Apr 2020 → Nov 2025 |
| `pctl_us_cagr_20_apr_24_jul` | INTEGER | US percentile rank, CAGR, Apr 2020 → Jul 2024 |
| `pctl_us_cagr_24_jul_25_nov` | INTEGER | US percentile rank, CAGR, Jul 2024 → Nov 2025 |
| `pctl_us_cagr_25_jul_25_nov` | INTEGER | US percentile rank, CAGR, Jul 2025 → Nov 2025 |
| `pctl_us_cagr_20_apr_25_jul` | INTEGER | US percentile rank, CAGR, Apr 2020 → Jul 2025 |
| `pctl_us_cagr_20_apr_25_nov` | INTEGER | US percentile rank, CAGR, Apr 2020 → Nov 2025 |
| `pctl_us_agr_24_jul_25_jul` | INTEGER | US percentile rank, annual growth rate, Jul 2024 → Jul 2025 |
:::

---

```{=typst}
#pagebreak()
```

## Table Dictionaries

Complete column listings for each table in delivery order.

### `block`

21 columns · 8,132,974 rows

::: {.tbl tbl-colwidths="[0.30, 0.12, 0.58]"}
| Column | Type | Description |
|---|---|---|
| `block_geoid` | VARCHAR | 15-digit census block GEOID (STATE 2 + COUNTY 3 + TRACT 6 + BLOCK 4) |
| `HU_20_apr` | INTEGER | Housing units, April 1, 2020 |
| `gq_20_apr` | INTEGER | Group quarters, April 1, 2020 |
| `HU_24_jul` | INTEGER | Housing units, July 1, 2024 |
| `gq_24_jul` | INTEGER | Group quarters, July 1, 2024 |
| `HU_25_jul` | INTEGER | Housing units, July 1, 2025 |
| `gq_25_jul` | INTEGER | Group quarters, July 1, 2025 |
| `HU_25_nov` | INTEGER | Housing units, November 1, 2025 |
| `gq_25_nov` | INTEGER | Group quarters, November 1, 2025 |
| `block_part_recs` | INTEGER | Number of source block-part records summed into this block |
| `zcta_20` | VARCHAR | 2020 ZCTA code assigned to this block |
| `block_fips_2022` | VARCHAR | Remapped 2022 block FIPS (Connecticut only; null elsewhere) |
| `ua` | VARCHAR | Urban area code assigned to this block |
| `uaname` | VARCHAR | Urban area name |
| `county.x` | VARCHAR | County FIPS from place crosswalk |
| `place` | VARCHAR | 5-digit place code |
| `placename` | VARCHAR | Place name |
| `county.y` | VARCHAR | County FIPS from county subdivision crosswalk |
| `cousub` | VARCHAR | 5-digit county subdivision code |
| `mcdname` | VARCHAR | Minor civil division name |
| `county_fips` | VARCHAR | 5-digit county FIPS (STATE 2 + COUNTY 3) |
:::

### `block_group`

96 columns · 240,075 rows

::: {.tbl tbl-colwidths="[0.38, 0.12, 0.50]"}
| Column | Type | Description |
|---|---|---|
| `block_group` | VARCHAR | 12-digit block group GEOID |
| `block_group_name` | VARCHAR | Block group name (e.g., "Block Group 1") |
| `HU_20_apr` | INTEGER | Housing units, April 1, 2020 |
| `gq_20_apr` | INTEGER | Group quarters, April 1, 2020 |
| `HU_24_jul` | INTEGER | Housing units, July 1, 2024 |
| `gq_24_jul` | INTEGER | Group quarters, July 1, 2024 |
| `HU_25_jul` | INTEGER | Housing units, July 1, 2025 |
| `gq_25_jul` | INTEGER | Group quarters, July 1, 2025 |
| `HU_25_nov` | INTEGER | Housing units, November 1, 2025 |
| `gq_25_nov` | INTEGER | Group quarters, November 1, 2025 |
| `block_recs` | INTEGER | Number of source blocks aggregated |
| `state_code` | VARCHAR | 2-digit state FIPS |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `county_fips` | VARCHAR | County FIPS used as county index baseline |
| `idx_county_base` | VARCHAR | County name |
| *12 county index columns* | — | See [Index Columns](#index-columns) |
| `cbsa23` | VARCHAR | CBSA code used as CBSA index baseline |
| `idx_cbsa_base` | VARCHAR | CBSA name |
| *12 CBSA index columns* | — | See [Index Columns](#index-columns) |
| `state_fips` | VARCHAR | State FIPS used as state index baseline |
| `idx_state_base` | VARCHAR | State name |
| *12 state index columns* | — | See [Index Columns](#index-columns) |
| *12 US index columns* | — | See [Index Columns](#index-columns) |
| *12 US percentile columns* | — | See [Percentile Columns](#percentile-columns) |
:::

### `census_tract`

96 columns · 84,557 rows

Same column structure as `block_group` with these key differences:

- Primary key: `tract` (VARCHAR, 11-digit GEOID)
- Name column: `census_tract_name` (e.g., "Census Tract 1001.01")

### `county`

84 columns · 3,175 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `county_fips` | VARCHAR | 5-digit county FIPS (primary key) |
| `county_name` | VARCHAR | County name with state abbreviation (e.g., "Autauga County, AL") |
| `state_code` | VARCHAR | 2-digit state FIPS |
| `state_name` | VARCHAR | State name |
| `state` | VARCHAR | State postal abbreviation (e.g., "AL") |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `cbsa23` | VARCHAR | CBSA code (county's containing CBSA) |
| `idx_cbsa_base` | VARCHAR | CBSA name |
| *12 CBSA index columns* | — | |
| `state_fips` | VARCHAR | State FIPS used as state index baseline |
| `idx_state_base` | VARCHAR | State name |
| *12 state index columns* | — | |
| *12 US index columns* | — | |
| *12 US percentile columns* | — | |
:::

### `county_subdivision`

95 columns · 38,136 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `cousub` | VARCHAR | 5-digit county subdivision code (cousubfp) |
| `county_fips` | VARCHAR | 5-digit county FIPS (composite key with `cousub`) |
| `county_sub_name` | VARCHAR | County subdivision name |
| `county` | VARCHAR | Containing county name |
| `state` | VARCHAR | State postal abbreviation |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `idx_county_base` | VARCHAR | County name (county index baseline label) |
| *12 county index columns* | — | |
| `idx_cbsa_base` | VARCHAR | CBSA name (CBSA index baseline label) |
| *12 CBSA index columns* | — | |
| `idx_state_base` | VARCHAR | State name (state index baseline label) |
| *12 state index columns* | — | |
| *12 US index columns* | — | |
| *12 US percentile columns* | — | |
:::

**Note:** The composite key for `county_subdivision` is `county_fips` + `cousub`. County subdivision codes (cousubfp) are unique only within a county; both fields are needed to uniquely identify a record.

### `zcta`

68 columns · 33,642 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `zcta_20` | VARCHAR | 5-digit ZCTA code, 2020 vintage (primary key) |
| `zcta_name` | VARCHAR | Display label (e.g., "ZCTA 78701") |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| `n_states` | INTEGER | Number of states whose blocks contribute to this ZCTA |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `state_fips` | VARCHAR | State FIPS used as state index baseline |
| `idx_state_base` | VARCHAR | State name |
| *12 state index columns* | — | |
| *12 US index columns* | — | |
| *12 US percentile columns* | — | |
:::

### `place`

68 columns · 31,670 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `place` | VARCHAR | 5-digit place code (placefp, composite key with `state_code`) |
| `state_code` | VARCHAR | 2-digit state FIPS (composite key with `place`) |
| `place_name` | VARCHAR | Place name with type (e.g., "Houston city") |
| `state` | VARCHAR | State postal abbreviation |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `idx_state_base` | VARCHAR | State name (state index baseline label) |
| *12 state index columns* | — | |
| *12 US index columns* | — | |
| *12 US percentile columns* | — | |
:::

**Note:** Places crossing state boundaries carry one row per state, with housing units attributed only to the blocks within that state.

### `urban_area`

67 columns · 2,848 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `ua` | VARCHAR | 5-digit urban area code (composite key with `state_code`) |
| `state_code` | VARCHAR | 2-digit state FIPS (composite key with `ua`) |
| `ua_name` | VARCHAR | Urban area name |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| `idx_state_base` | VARCHAR | State name (state index baseline label) |
| *12 state index columns* | — | |
| *12 US index columns* | — | |
| *12 US percentile columns* | — | |
:::

**Note:** Urban areas crossing state boundaries carry one row per state, with housing units attributed to the blocks within that state.

### `cbsa`

53 columns · 920 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `cbsa23` | VARCHAR | 5-digit CBSA code, 2023 OMB delineations (primary key) |
| `cbsa_name` | VARCHAR | CBSA name (e.g., "Dallas-Fort Worth-Arlington, TX Metro Area") |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| *12 US index columns* | — | See [Index Columns](#index-columns) |
| *12 US percentile columns* | — | See [Percentile Columns](#percentile-columns) |
:::

### `state`

41 columns · 51 rows

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `state_code` | VARCHAR | 2-digit state FIPS (primary key) |
| `state_name` | VARCHAR | Full state name |
| `HU_20_apr`–`gq_25_nov` | INTEGER | Base counts (8 columns) |
| `block_recs` | INTEGER | Source blocks aggregated |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
| *12 US index columns* | — | See [Index Columns](#index-columns) |
:::

### `us`

27 columns · 1 row

Contains base counts and all growth metrics for the nation as a whole. No index or percentile columns (US is the top-level baseline for all other tables).

::: {.tbl tbl-colwidths="[0.28, 0.12, 0.60]"}
| Column | Type | Description |
|---|---|---|
| `HU_20_apr` | INTEGER | National housing units, April 1, 2020: **140,498,736** |
| `gq_20_apr` | INTEGER | National group quarters, April 1, 2020: **191,148** |
| `HU_24_jul` | INTEGER | National housing units, July 1, 2024: **145,191,314** |
| `gq_24_jul` | INTEGER | National group quarters, July 1, 2024: **232,022** |
| `HU_25_jul` | INTEGER | National housing units, July 1, 2025: **148,430,781** |
| `gq_25_jul` | INTEGER | National group quarters, July 1, 2025: **234,120** |
| `HU_25_nov` | INTEGER | National housing units, November 1, 2025: **149,722,862** |
| `gq_25_nov` | INTEGER | National group quarters, November 1, 2025: **235,109** |
| `block_recs` | INTEGER | Source blocks: **8,132,974** |
| *18 growth metric columns* | — | See [Derived Growth Metrics](#derived-growth-metrics) |
:::

---

```{=typst}
#pagebreak()
```

## Calculation Rules

### Block Aggregation

Source files contain one record per block-part, where a block may have multiple parts if it intersects multiple geographic units in a crosswalk file. Before any aggregation, block-parts are summed to the full census block level (15-digit GEOID). The `block_part_recs` column in the `block` table records how many source records were summed.

### Geographic Rollup

All geographic levels above block are derived by summing housing unit counts from the block level up. The aggregation hierarchy:

- Block → Block Group (first 12 characters of block GEOID)
- Block → Census Tract (first 11 characters of block GEOID)
- Block → County (first 5 characters of block GEOID)
- Block → ZCTA (via block-to-ZCTA crosswalk file)
- Block → Place (via block-to-place crosswalk)
- Block → County Subdivision (via block-to-county-subdivision crosswalk)
- Block → Urban Area (via block-to-urban-area crosswalk)
- County → CBSA (via county-to-CBSA crosswalk, 2023 OMB delineations)
- County → State (first 2 characters of county FIPS)
- County → US (national sum)

### Housing Growth (Absolute)

> `hg_{interval} = HU_{end} − HU_{start}`

Integer result. Negative values indicate housing unit loss.

### Housing Growth Index (Ratio)

> `hgi_{interval} = HU_{end} / HU_{start}`

Null when `HU_{start}` is 0 or null.

A value of 1.0 indicates no change. A value of 1.10 indicates 10% total growth.

### Compound Annual Growth Rate

> `cagr_{interval} = (HU_{end} / HU_{start})^(1 / years) − 1`

Null when `HU_{start}` is 0 or null. Expressed as a decimal (e.g., 0.0115 = 1.15% per year).

Years used per interval:

| Interval | Years |
|---|---|
| `20_apr_24_jul` | 4.25 |
| `24_jul_25_nov` | 1.33 |
| `25_jul_25_nov` | 0.33 |
| `20_apr_25_jul` | 5.25 |
| `20_apr_25_nov` | 5.58 |

### Annual Growth Rate (Simple)

> `agr_24_jul_25_jul = (HU_25_jul / HU_24_jul) − 1`

Used only for the exact one-year interval Jul 2024 → Jul 2025. Null when `HU_24_jul` is 0 or null.

### Relative Index (HGI-Based)

> `idx_{baseline}_hgi_{interval} = (geography_hgi − 1) / (baseline_hgi − 1) × 100`

Null when the baseline HGI equals 1.0 (no growth in baseline) or is null.

### Relative Index (CAGR/AGR-Based)

> `idx_{baseline}_cagr_{interval} = geography_cagr / baseline_cagr × 100`
> `idx_{baseline}_agr_{interval} = geography_agr / baseline_agr × 100`

Null when baseline rate is 0 or null.

### Percentile Rank

> `pctl_us_{metric}_{interval} = ceil(rank(value) / N × 100)`

Ranking uses minimum tie-breaking (tied values receive the lowest rank in the group). Percentiles are integers 1–100. Null metric values receive null percentile.

---

```{=typst}
#pagebreak()
```

## Appendix A: State Inventory

51 records (50 states + District of Columbia). All housing unit counts are from the `state` table.

::: {.tbl tbl-colwidths="[0.12, 0.35, 0.18, 0.18, 0.17]"}
| FIPS | State | HU Apr 2020 | HU Nov 2025 | Change |
|---|---|---|---|---|
| 01 | Alabama | 2,288,330 | 2,460,937 | +172,607 |
| 02 | Alaska | 326,200 | 336,435 | +10,235 |
| 04 | Arizona | 3,082,000 | 3,378,875 | +296,875 |
| 05 | Arkansas | 1,365,265 | 1,470,616 | +105,351 |
| 06 | California | 14,392,140 | 14,997,862 | +605,722 |
| 08 | Colorado | 2,491,404 | 2,712,566 | +221,162 |
| 09 | Connecticut | 1,530,197 | 1,575,202 | +45,005 |
| 10 | Delaware | 448,735 | 489,409 | +40,674 |
| 11 | District of Columbia | 350,364 | 391,371 | +41,007 |
| 12 | Florida | 9,865,350 | 10,852,276 | +986,926 |
| 13 | Georgia | 4,410,956 | 4,788,130 | +377,174 |
| 15 | Hawaii | 561,066 | 579,830 | +18,764 |
| 16 | Idaho | 751,859 | 857,346 | +105,487 |
| 17 | Illinois | 5,426,429 | 5,560,814 | +134,385 |
| 18 | Indiana | 2,923,175 | 3,066,204 | +143,029 |
| 19 | Iowa | 1,412,789 | 1,488,830 | +76,041 |
| 20 | Kansas | 1,275,689 | 1,331,429 | +55,740 |
| 21 | Kentucky | 1,994,323 | 2,116,266 | +121,943 |
| 22 | Louisiana | 2,073,200 | 2,178,770 | +105,570 |
| 23 | Maine | 739,072 | 776,779 | +37,707 |
| 24 | Maryland | 2,530,844 | 2,638,078 | +107,234 |
| 25 | Massachusetts | 2,998,537 | 3,107,864 | +109,327 |
| 26 | Michigan | 4,570,173 | 4,696,015 | +125,842 |
| 27 | Minnesota | 2,485,558 | 2,647,222 | +161,664 |
| 28 | Mississippi | 1,319,945 | 1,400,110 | +80,165 |
| 29 | Missouri | 2,786,621 | 2,930,388 | +143,767 |
| 30 | Montana | 514,803 | 560,292 | +45,489 |
| 31 | Nebraska | 844,278 | 902,054 | +57,776 |
| 32 | Nevada | 1,281,018 | 1,396,036 | +115,018 |
| 33 | New Hampshire | 638,795 | 664,635 | +25,840 |
| 34 | New Jersey | 3,761,229 | 3,877,406 | +116,177 |
| 35 | New Mexico | 940,859 | 986,996 | +46,137 |
| 36 | New York | 8,488,066 | 8,761,731 | +273,665 |
| 37 | North Carolina | 4,708,710 | 5,196,639 | +487,929 |
| 38 | North Dakota | 370,642 | 390,296 | +19,654 |
| 39 | Ohio | 5,242,524 | 5,425,174 | +182,650 |
| 40 | Oklahoma | 1,746,807 | 1,855,331 | +108,524 |
| 41 | Oregon | 1,813,747 | 1,933,054 | +119,307 |
| 42 | Pennsylvania | 5,742,828 | 5,935,665 | +192,837 |
| 44 | Rhode Island | 483,474 | 495,869 | +12,395 |
| 45 | South Carolina | 2,344,963 | 2,622,990 | +278,027 |
| 46 | South Dakota | 393,375 | 427,364 | +33,989 |
| 47 | Tennessee | 3,031,605 | 3,352,958 | +321,353 |
| 48 | Texas | 11,589,324 | 13,110,633 | +1,521,309 |
| 49 | Utah | 1,151,414 | 1,318,573 | +167,159 |
| 50 | Vermont | 334,318 | 349,170 | +14,852 |
| 51 | Virginia | 3,618,247 | 3,828,869 | +210,622 |
| 53 | Washington | 3,202,241 | 3,462,801 | +260,560 |
| 54 | West Virginia | 855,635 | 901,192 | +45,557 |
| 55 | Wisconsin | 2,727,726 | 2,851,611 | +123,885 |
| 56 | Wyoming | 271,887 | 285,899 | +14,012 |
| — | **United States** | **140,498,736** | **149,722,862** | **+9,224,126** |
:::

---

```{=typst}
#pagebreak()
```

## Appendix B: CBSA Inventory (Top 50)

CBSAs ranked by November 2025 housing unit count. The full `cbsa` table contains 919 CBSAs. CBSA delineations follow 2023 OMB definitions.

::: {.tbl tbl-colwidths="[0.10, 0.50, 0.20, 0.20]"}
| CBSA | Name | HU Apr 2020 | HU Nov 2025 |
|---|---|---|---|
| 35620 | New York-Newark-Jersey City, NY-NJ Metro Area | 7,942,708 | 8,219,910 |
| 31080 | Los Angeles-Long Beach-Anaheim, CA Metro Area | 4,721,766 | 4,882,423 |
| 16980 | Chicago-Naperville-Elgin, IL-IN Metro Area | 3,871,494 | 3,976,427 |
| 19100 | Dallas-Fort Worth-Arlington, TX Metro Area | 2,947,189 | 3,373,088 |
| 26420 | Houston-Pasadena-The Woodlands, TX Metro Area | 2,753,960 | 3,096,274 |
| 33100 | Miami-Fort Lauderdale-West Palm Beach, FL Metro Area | 2,641,002 | 2,776,906 |
| 37980 | Philadelphia-Camden-Wilmington, PA-NJ-DE-MD Metro Area | 2,586,947 | 2,683,327 |
| 47900 | Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area | 2,458,414 | 2,619,867 |
| 12060 | Atlanta-Sandy Springs-Roswell, GA Metro Area | 2,419,737 | 2,639,435 |
| 14460 | Boston-Cambridge-Newton, MA-NH Metro Area | 2,032,387 | 2,120,132 |
| 38060 | Phoenix-Mesa-Chandler, AZ Metro Area | 1,985,705 | 2,207,624 |
| 19820 | Detroit-Warren-Dearborn, MI Metro Area | 1,901,256 | 1,941,144 |
| 41860 | San Francisco-Oakland-Fremont, CA Metro Area | 1,847,185 | 1,911,395 |
| 42660 | Seattle-Tacoma-Bellevue, WA Metro Area | 1,650,246 | 1,784,672 |
| 40140 | Riverside-San Bernardino-Ontario, CA Metro Area | 1,580,448 | 1,664,690 |
| 33460 | Minneapolis-St. Paul-Bloomington, MN-WI Metro Area | 1,503,829 | 1,626,662 |
| 45300 | Tampa-St. Petersburg-Clearwater, FL Metro Area | 1,465,158 | 1,582,803 |
| 41180 | St. Louis, MO-IL Metro Area | 1,258,862 | 1,309,098 |
| 19740 | Denver-Aurora-Centennial, CO Metro Area | 1,242,492 | 1,360,107 |
| 36740 | Orlando-Kissimmee-Sanford, FL Metro Area | 1,175,093 | 1,310,568 |
:::

---

## Document Generation Metadata

**Generated By:** Claude Code (claude-sonnet-4-6)
**Workflow Used:** Direct analysis — code and database sources only
**Generation Date:** 2026-02-24

### Agent Configuration

- **Primary Agent:** Claude Code (claude-sonnet-4-6)
  - **Mode:** Direct documentation from source analysis
  - **Persona:** Technical documentation author

### Workflow Components Used

**Source Files Analyzed:**
- `/home/joel/01_housing/prd-housing-units-2026.R` — Primary data processing pipeline (4 vintages, block aggregation, geographic rollups)
- `/home/joel/01_housing/housing-distro-build.R` — Distribution database build (11 geographic layers, index/percentile construction)
- `/home/joel/01_housing/bg-housing-rollups.r` — Block group relative index and percentile calculations

**Database Queried:**
- `/home/joel/data/housing_distro.duckdb` — Production distribution database (ground truth)
  - 12 tables (11 data + 1 metadata), 716 metadata rows
  - All column schemas extracted from `metadata` table
  - Row counts, US totals, state totals, and CBSA inventory extracted directly

**Guidelines File:**
- `/home/joel/01_housing/doc-guidelines.md` — Documentation scope and audience requirements

### Sub-Agents

**None** — Single-session direct analysis

### Workflow Execution Summary

- **Mode:** Source-grounded documentation (no assumptions, all detail from code and database)
- **Audience:** Model builders and analysts (no platform/deployment detail)
- **Excluded per guidelines:** Implementation details, R version, directory paths, code linkage explainers, beginner explainers
- **Included per guidelines:** Table layouts and dictionaries, source data documentation and vintage, calculation rules, geographic inventory appendices
- **Output:** `/home/joel/01_housing/buildingzones-data-reference.md`
