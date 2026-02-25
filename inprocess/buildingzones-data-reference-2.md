# BuildingZones Housing Data Reference

**Product**: BuildingZones from Tidy Analytics
**Document Version**: 3.0
**Last Updated**: 2026-02-24

---

```{=typst}
#pagebreak()
```

## Table of Contents

1. [Product Overview](#product-overview)
2. [Data Sources](#data-sources)
3. [Data Deliverables](#data-deliverables)
4. [Client Data Portal](#client-data-portal)
5. [Geographic Layers](#geographic-layers)
6. [Field Definitions](#field-definitions)
7. [Calculation Rules](#calculation-rules)
8. [Data Quality Notes](#data-quality-notes)
9. [Appendices](#appendices)

---

```{=typst}
#pagebreak()
```

## Product Overview

BuildingZones provides housing unit counts and growth analytics derived from U.S. Census Bureau Address Block Count Lists, covering every census geography from individual census blocks to national totals. The product is designed to support model builders and analysts who require granular, consistent, and fully documented housing unit data across multiple geographies and time periods.

The dataset is organized around a set of **geographic layers** — one per geography type — each containing base housing counts, derived growth metrics, and comparative indices relative to parent geographies. Every layer is delivered in three formats: as tables within a single DuckDB database, as individual Parquet files, and as individual CSV files.

---

```{=typst}
#pagebreak()
```

## Data Sources

### Primary Housing Unit Data

All housing unit and group quarters counts originate from **U.S. Census Bureau Address Block Count Lists**, published by the Census Bureau's Geography Division. Data is compiled at the census block level and aggregated upward to all higher geographic levels.

| Source | Reference Date | Vintage Label Used in Fields |
|--------|----------------|------------------------------|
| Decennial Census, Address Block Count List | April 1, 2020 | `_20_apr` |
| Census Bureau Address Block Count List | July 1, 2024 | `_24_jul` |
| Census Bureau Address Block Count List | July 1, 2025 | `_25_jul` |
| Census Bureau Address Block Count List | November 1, 2025 | `_25_nov` |

The vintage label suffixes embedded in all field names encode both the year and the reference month, allowing unambiguous identification of the source period for each count.

### Geographic Crosswalk Sources

Non-hierarchical geography assignments (ZCTAs, Places, County Subdivisions, Urban Areas, CBSAs) require crosswalk files that link census blocks to those geographies. The crosswalks used are:

| Crosswalk | Source | Purpose |
|-----------|--------|---------|
| Block-to-ZCTA | Census Tab20 relationship file | Assigns ZCTAs to census blocks |
| Block-to-Place | Census relationship files | Assigns incorporated places and CDPs to blocks |
| Block-to-County Subdivision | Census relationship files | Assigns minor civil divisions to blocks |
| Block-to-Urban Area | Census relationship files | Assigns urban area designations to blocks |
| County-to-CBSA | OMB Delineation Files (2023 vintage) | Links counties to Core-Based Statistical Areas |
| Connecticut Block Remap | Census 2020-to-2022 block crosswalk | Reconciles Connecticut block GEOIDs across vintages |

### Geographic Name Sources

Geography names included in each layer are sourced from Census TIGER/Line files (2023 vintage) for counties, places, county subdivisions, CBSAs, and states. ZCTA names are constructed as `"ZCTA " + zcta_code`. Urban area names are sourced from Census block-to-urban-area relationship files.

---

```{=typst}
#pagebreak()
```

## Data Deliverables

### Primary Deliverable: `housing_distro.duckdb`

The primary deliverable is a single **DuckDB database file** named `housing_distro.duckdb`. This file contains 12 tables: one per geographic layer plus a `metadata` table.

DuckDB is an embedded analytical database that can be queried directly from R, Python, SQL clients, or the DuckDB CLI without a server. It supports standard SQL and is highly performant on analytical workloads. Clients with existing DuckDB tooling can connect directly; those without may prefer the Parquet or CSV flat files described below.

**Table inventory:**

| Table Name | Records | Primary Key | Geographic Level |
|------------|---------|-------------|-----------------|
| `block` | 8,132,974 | `block_geoid` | Census block (15-digit GEOID) |
| `block_group` | 240,075 | `block_group` | Census block group (12-digit GEOID) |
| `census_tract` | 84,557 | `tract` | Census tract (11-digit GEOID) |
| `county` | 3,175 | `county_fips` | County (5-digit FIPS) |
| `county_subdivision` | 38,136 | `cousub` + `county_fips` | County subdivision (5-digit code + 5-digit county FIPS) |
| `place` | 31,670 | `place` + `state_code` | Incorporated place / CDP |
| `urban_area` | 2,848 | `ua` + `state_code` | Urban area (5-digit code + 2-digit state FIPS) |
| `zcta` | 33,642 | `zcta_20` | ZIP Code Tabulation Area (2020 vintage) |
| `cbsa` | 920 | `cbsa23` | Core-Based Statistical Area (2023 OMB vintage) |
| `state` | 51 | `state_code` | State + DC (2-digit FIPS) |
| `us` | 1 | *(none)* | National total |
| `metadata` | 716 | *(none)* | Column definitions for all tables |

### Flat File Deliverables: Parquet and CSV

Each geographic layer is also delivered as a standalone **Parquet file** and a standalone **CSV file**, using the same table name as the filename base. These files contain identical data to the corresponding table in `housing_distro.duckdb`.

| Layer | Parquet File | CSV File |
|-------|-------------|---------|
| Census block | `block.parquet` | `block.csv` |
| Block group | `block_group.parquet` | `block_group.csv` |
| Census tract | `census_tract.parquet` | `census_tract.csv` |
| County | `county.parquet` | `county.csv` |
| County subdivision | `county_subdivision.parquet` | `county_subdivision.csv` |
| Place | `place.parquet` | `place.csv` |
| Urban area | `urban_area.parquet` | `urban_area.csv` |
| ZCTA | `zcta.parquet` | `zcta.csv` |
| CBSA | `cbsa.parquet` | `cbsa.csv` |
| State | `state.parquet` | `state.csv` |
| US | `us.parquet` | `us.csv` |

**Parquet** files are recommended for analytical use; they are columnar, compressed, and readable by virtually all modern data tooling (Arrow, Pandas, Spark, DuckDB, R's `arrow` package, etc.).

**CSV** files are provided for maximum compatibility and are suitable for use in spreadsheet applications, GIS software, or any tool that does not support Parquet or DuckDB.

---

```{=typst}
#pagebreak()
```

## Client Data Portal

### Overview

Each Tidy Analytics client is provisioned with a **dedicated, isolated data enclave** on Microsoft Azure. This enclave includes private cloud storage, a secure key vault, and a client-specific data portal — a lightweight web application that serves as the primary distribution channel for all BuildingZones data files.

The client portal is accessible only to credentialed users within the client's provisioned Azure Active Directory group. Authentication is handled via Microsoft Azure AD (OAuth2/OIDC). No data is shared between client environments.

### Data File Access via SAS URL

All BuildingZones data files (the DuckDB database, Parquet files, and CSV files) are staged in the client's dedicated Azure Blob Storage container prior to delivery notification. Access is provided via a **Shared Access Signature (SAS) URL** — a time-limited, cryptographically signed access token that grants read access to the client's specific storage container.

**Best Practice — Initial Access Flow:**

1. Upon provisioning completion, Tidy Analytics generates a SAS URL specific to the client's storage container and communicates it to the client's designated contact via a secure channel (direct email to the provisioned APP_OWNER account).
2. The client uses the SAS URL to authenticate into the data portal, which provides a file browser view of available data files.
3. From the portal, the client can browse, download, and verify the data files.
4. SAS tokens have a defined expiration period. Clients should download files promptly upon receipt of the access notification. Tidy Analytics will communicate token expiration dates and will provide renewed tokens as needed.
5. For clients who prefer direct programmatic access (e.g., downloading from R or Python scripts), the SAS URL can be used directly with any Azure Blob Storage-compatible client library.

### Portal Capabilities

The client portal application provides:

- **File browser**: List all files available in the client's storage container
- **Download**: Download individual files directly to the local machine
- **Upload**: Upload client-provided files for exchange with Tidy Analytics (e.g., for custom configuration or analysis return)

The portal is a browser-based application requiring no local software installation beyond a modern web browser. It is accessible from any network once the client's credentials are established.

### User Accounts and Access

Each client environment is provisioned with one initial user account (designated during the provisioning process). Additional users within the client organization can be added to the client's Azure AD group. All users within the group share access to the same file storage container and portal.

---

```{=typst}
#pagebreak()
```

## Geographic Layers

### Geographic Hierarchy

The standard Census geographic hierarchy flows from smallest to largest:

```
Census Block (15-digit GEOID)
 └── Block Group (first 12 digits of block GEOID)
      └── Census Tract (first 11 digits of block GEOID)
           └── County (first 5 digits of block GEOID)
                └── State (first 2 digits of block GEOID)
                     └── US (national aggregate)
```

Non-hierarchical geographies are assigned via crosswalk and do not follow this nesting:

```
Census Block
 ├── ZCTA (via block-to-ZCTA crosswalk)
 ├── Place (via block-to-place crosswalk)
 ├── County Subdivision (via block-to-county-subdivision crosswalk)
 └── Urban Area (via block-to-urban-area crosswalk)

County
 └── CBSA (via county-to-CBSA crosswalk, 2023 OMB vintage)
```

### Layer Summary

#### `block` — Census Block

The most granular layer. Contains base housing unit counts only; no derived growth metrics are included at the block level. The `block` table also carries crosswalk identifiers (`zcta_20`, `ua`, `place`, `cousub`, `county_fips`) that link each block to the non-hierarchical geographies it belongs to.

**Primary key**: `block_geoid` (15-digit Census GEOID)
**Records**: 8,132,974

**Note**: Block-level data includes only raw housing counts (`HU_*` and `gq_*` fields) and geographic identifiers. Growth metrics must be calculated by the analyst from base counts. See [Calculation Rules](#calculation-rules) for formulas.

---

#### `block_group` — Census Block Group

**Primary key**: `block_group` (12-digit GEOID)
**Records**: 240,075
**Index coverage**: County, CBSA, State, US

---

#### `census_tract` — Census Tract

**Primary key**: `tract` (11-digit GEOID)
**Records**: 84,557
**Index coverage**: County, CBSA, State, US

---

#### `county` — County

Includes embedded name fields (`county_name`, `state_name`, `state` abbreviation).

**Primary key**: `county_fips` (5-digit FIPS)
**Records**: 3,175
**Index coverage**: CBSA, State, US

---

#### `county_subdivision` — County Subdivision (Minor Civil Division)

County subdivisions are unique only within a county; both the `cousub` code and `county_fips` code are required to uniquely identify a record. Includes embedded name fields (`county_sub_name`, `county`, `state` abbreviation).

**Primary key**: `cousub` (5-digit code) + `county_fips` (5-digit FIPS)
**Records**: 38,136
**Index coverage**: County, CBSA, State, US

---

#### `place` — Incorporated Place / Census Designated Place

Place codes are unique only within a state; both `place` and `state_code` are required to uniquely identify a record. Includes embedded name fields (`place_name`, `state` abbreviation).

**Primary key**: `place` (5-digit code) + `state_code` (2-digit FIPS)
**Records**: 31,670
**Index coverage**: State, US

---

#### `urban_area` — Urban Area

Urban areas can cross state lines; records are stored one row per urban area per state. Includes `ua_name`.

**Primary key**: `ua` (5-digit code) + `state_code` (2-digit FIPS)
**Records**: 2,848
**Index coverage**: State, US

---

#### `zcta` — ZIP Code Tabulation Area

ZCTAs use 2020 vintage boundaries. ZCTAs can span state lines; the `n_states` field indicates how many states a given ZCTA covers. State assignment (for state-level index comparisons) is based on maximum land area overlap.

**Primary key**: `zcta_20` (5-digit code)
**Records**: 33,642
**Index coverage**: State, US

---

#### `cbsa` — Core-Based Statistical Area

CBSAs use 2023 OMB delineation vintage. Includes `cbsa_name`.

**Primary key**: `cbsa23` (5-digit OMB code)
**Records**: 920
**Index coverage**: US only

---

#### `state` — State / DC

**Primary key**: `state_code` (2-digit FIPS)
**Records**: 51 (50 states + DC)
**Index coverage**: US only

---

#### `us` — National Total

Single-row table providing national aggregate housing counts and growth metrics. No index comparisons (US is the top-level benchmark).

**Records**: 1

---

```{=typst}
#pagebreak()
```

## Field Definitions

### Naming Conventions

Field names in `housing_distro.duckdb` encode the metric type and time period in the field name itself, using the following pattern:

```
{metric_prefix}_{start_period}_{end_period}
```

**Time period tokens:**

| Token | Reference Date |
|-------|---------------|
| `20_apr` | April 1, 2020 (Decennial Census) |
| `24_jul` | July 1, 2024 |
| `25_jul` | July 1, 2025 |
| `25_nov` | November 1, 2025 |

**Metric prefix tokens:**

| Prefix | Metric Type |
|--------|-------------|
| `HU_` | Housing unit count (base measure) |
| `gq_` | Group quarters count (base measure) |
| `hg_` | Housing growth, absolute (net change) |
| `hgi_` | Housing growth index (ratio) |
| `cagr_` | Compound annual growth rate |
| `agr_` | Simple annual growth rate (1-year periods only) |
| `idx_{scope}_hgi_` | Growth index relative to parent geography |
| `idx_{scope}_cagr_` | CAGR relative to parent geography |
| `idx_{scope}_agr_` | AGR relative to parent geography |
| `pctl_us_{metric}_` | National percentile rank |

**Index scope tokens:**

| Scope | Parent Geography |
|-------|-----------------|
| `county` | Parent county |
| `cbsa` | Parent CBSA (metro/micro area) |
| `state` | Parent state |
| `us` | National (all geographies of same type) |

---

### Base Count Fields

These fields appear in all tables (except where noted).

| Field | Type | Description |
|-------|------|-------------|
| `HU_20_apr` | INTEGER | Housing units, April 1, 2020 |
| `gq_20_apr` | INTEGER | Group quarters persons, April 1, 2020 |
| `HU_24_jul` | INTEGER | Housing units, July 1, 2024 |
| `gq_24_jul` | INTEGER | Group quarters persons, July 1, 2024 |
| `HU_25_jul` | INTEGER | Housing units, July 1, 2025 |
| `gq_25_jul` | INTEGER | Group quarters persons, July 1, 2025 |
| `HU_25_nov` | INTEGER | Housing units, November 1, 2025 |
| `gq_25_nov` | INTEGER | Group quarters persons, November 1, 2025 |
| `block_recs` | INTEGER | Number of census blocks aggregated into this record |

**Housing unit inclusion**: Single-family homes, townhouses, duplexes, apartments, mobile homes, and vacant units. Hotels, motels, and group quarters are excluded.

**Group quarters inclusion**: Persons residing in college dormitories, nursing homes, correctional facilities, military barracks, group homes, and similar institutional arrangements. Note that `gq_*` fields represent **persons**, not housing units, and are not incorporated into housing growth calculations.

**`block_recs`**: For census blocks this is always 1. For higher geographies it reflects the number of source blocks summed. Low values may indicate sparse rural geographies.

---

### Derived Growth Metrics

These fields appear in all tables except `block` and `us`.

#### Absolute Change (hg_)

| Field | Type | Calculation |
|-------|------|-------------|
| `hg_20_apr_24_jul` | INTEGER | `HU_24_jul − HU_20_apr` |
| `hg_24_jul_25_jul` | INTEGER | `HU_25_jul − HU_24_jul` |
| `hg_24_jul_25_nov` | INTEGER | `HU_25_nov − HU_24_jul` |
| `hg_25_jul_25_nov` | INTEGER | `HU_25_nov − HU_25_jul` |
| `hg_20_apr_25_jul` | INTEGER | `HU_25_jul − HU_20_apr` |
| `hg_20_apr_25_nov` | INTEGER | `HU_25_nov − HU_20_apr` |

Positive values indicate net housing growth; negative values indicate net housing decline. Both are valid and expected.

#### Growth Index / Ratio (hgi_)

| Field | Type | Calculation |
|-------|------|-------------|
| `hgi_20_apr_24_jul` | DOUBLE | `HU_24_jul / HU_20_apr` |
| `hgi_24_jul_25_jul` | DOUBLE | `HU_25_jul / HU_24_jul` |
| `hgi_24_jul_25_nov` | DOUBLE | `HU_25_nov / HU_24_jul` |
| `hgi_25_jul_25_nov` | DOUBLE | `HU_25_nov / HU_25_jul` |
| `hgi_20_apr_25_jul` | DOUBLE | `HU_25_jul / HU_20_apr` |
| `hgi_20_apr_25_nov` | DOUBLE | `HU_25_nov / HU_20_apr` |

A value of `1.00` indicates no change; `1.10` indicates 10% growth; `0.95` indicates 5% decline. NULL is returned when the base period count is zero or NULL.

#### Compound Annual Growth Rate (cagr_)

| Field | Type | Period (years) | Calculation |
|-------|------|----------------|-------------|
| `cagr_20_apr_24_jul` | DOUBLE | 4.25 | `hgi_20_apr_24_jul ^ (1/4.25) − 1` |
| `cagr_24_jul_25_nov` | DOUBLE | 1.33 | `hgi_24_jul_25_nov ^ (1/1.333) − 1` |
| `cagr_25_jul_25_nov` | DOUBLE | 0.33 | `hgi_25_jul_25_nov ^ (1/0.333) − 1` |
| `cagr_20_apr_25_jul` | DOUBLE | 5.25 | `hgi_20_apr_25_jul ^ (1/5.25) − 1` |
| `cagr_20_apr_25_nov` | DOUBLE | 5.583 | `hgi_20_apr_25_nov ^ (1/5.583) − 1` |

CAGR values are expressed as decimal rates (e.g., `0.023` = 2.3% annual growth). They are suitable for comparing growth across geographies and time periods of different lengths. NULL is returned when the growth index is NULL or non-positive.

#### Simple Annual Growth Rate (agr_)

| Field | Type | Calculation |
|-------|------|-------------|
| `agr_24_jul_25_jul` | DOUBLE | `hgi_24_jul_25_jul − 1` |

Used for the 1-year July 2024 to July 2025 period. Equivalent to CAGR for a 1-year period; expressed as a decimal rate (e.g., `0.03` = 3%).

---

### Comparative Index Fields (idx_)

Comparative index fields are embedded directly in each geographic layer (rather than in separate index tables). They indicate how a geography's growth rate compares to a parent geography, normalized to a scale where `100` = growing at the same rate as the parent.

**General formula:**

```
idx_{scope}_{metric} = ((hgi_child − 1) / (hgi_parent − 1)) × 100
```

**Interpretation:**

| Index Value | Meaning |
|-------------|---------|
| `> 200` | Growing more than 2× faster than parent |
| `150–200` | Growing significantly faster than parent |
| `110–150` | Growing moderately faster than parent |
| `90–110` | Approximately same growth rate as parent |
| `50–90` | Growing slower than parent |
| `< 50` | Growing much slower than parent |
| `0` | No growth while parent is growing |
| `< 0` | Declining while parent is growing (or growing while parent declines) |
| `NULL` | Parent has zero growth; index is indeterminate |

Each layer carries a **label field** immediately following the parent geography's ID field, containing the parent geography's name. These label fields are named `idx_{scope}_base` (e.g., `idx_county_base`, `idx_cbsa_base`, `idx_state_base`) and contain the human-readable name of the parent geography used as the index baseline for that row.

**Index availability by layer:**

| Layer | County Index | CBSA Index | State Index | US Index |
|-------|:---:|:---:|:---:|:---:|
| `block_group` | Yes | Yes | Yes | Yes |
| `census_tract` | Yes | Yes | Yes | Yes |
| `county_subdivision` | Yes | Yes | Yes | Yes |
| `county` | — | Yes | Yes | Yes |
| `place` | — | — | Yes | Yes |
| `urban_area` | — | — | Yes | Yes |
| `zcta` | — | — | Yes | Yes |
| `cbsa` | — | — | — | Yes |
| `state` | — | — | — | Yes |

**Index fields by time period (pattern, applied to each scope):**

| Field Pattern | Metric Indexed |
|---------------|---------------|
| `idx_{scope}_hgi_20_apr_24_jul` | Growth index, Apr 2020–Jul 2024 |
| `idx_{scope}_hgi_24_jul_25_jul` | Growth index, Jul 2024–Jul 2025 |
| `idx_{scope}_hgi_24_jul_25_nov` | Growth index, Jul 2024–Nov 2025 |
| `idx_{scope}_hgi_25_jul_25_nov` | Growth index, Jul 2025–Nov 2025 |
| `idx_{scope}_hgi_20_apr_25_jul` | Growth index, Apr 2020–Jul 2025 |
| `idx_{scope}_hgi_20_apr_25_nov` | Growth index, Apr 2020–Nov 2025 |
| `idx_{scope}_cagr_20_apr_24_jul` | CAGR, Apr 2020–Jul 2024 |
| `idx_{scope}_cagr_24_jul_25_nov` | CAGR, Jul 2024–Nov 2025 |
| `idx_{scope}_cagr_25_jul_25_nov` | CAGR, Jul 2025–Nov 2025 |
| `idx_{scope}_cagr_20_apr_25_jul` | CAGR, Apr 2020–Jul 2025 |
| `idx_{scope}_cagr_20_apr_25_nov` | CAGR, Apr 2020–Nov 2025 |
| `idx_{scope}_agr_24_jul_25_jul` | AGR, Jul 2024–Jul 2025 |

---

### National Percentile Fields (pctl_)

Percentile fields rank each geography nationally among all geographies of the same type, based on growth rate. Rankings are calculated separately for each time period and metric combination.

**Formula:**

```
pctl_us_{metric} = CEILING(RANK(metric, ascending) / total_count × 100)
```

Values range from 1 (slowest-growing) to 100 (fastest-growing). Ties receive the minimum rank. Geographies with NULL growth indices receive NULL percentiles.

**Percentile fields available (pattern):**

| Field Pattern | Ranks By |
|--------------|---------|
| `pctl_us_hgi_20_apr_24_jul` | Growth index, Apr 2020–Jul 2024 |
| `pctl_us_hgi_24_jul_25_jul` | Growth index, Jul 2024–Jul 2025 |
| `pctl_us_hgi_24_jul_25_nov` | Growth index, Jul 2024–Nov 2025 |
| `pctl_us_hgi_25_jul_25_nov` | Growth index, Jul 2025–Nov 2025 |
| `pctl_us_hgi_20_apr_25_jul` | Growth index, Apr 2020–Jul 2025 |
| `pctl_us_hgi_20_apr_25_nov` | Growth index, Apr 2020–Nov 2025 |
| `pctl_us_cagr_20_apr_24_jul` | CAGR, Apr 2020–Jul 2024 |
| `pctl_us_cagr_24_jul_25_nov` | CAGR, Jul 2024–Nov 2025 |
| `pctl_us_cagr_25_jul_25_nov` | CAGR, Jul 2025–Nov 2025 |
| `pctl_us_cagr_20_apr_25_jul` | CAGR, Apr 2020–Jul 2025 |
| `pctl_us_cagr_20_apr_25_nov` | CAGR, Apr 2020–Nov 2025 |
| `pctl_us_agr_24_jul_25_jul` | AGR, Jul 2024–Jul 2025 |

**Percentile availability by layer:** Percentile fields are present in all layers except `us` (only one row, no ranking possible) and `block` (no growth metrics at block level).

**Interpretation guide:**

| Percentile | Interpretation |
|------------|---------------|
| 95–100 | Top 5% fastest-growing nationally |
| 90–94 | Top 10% fastest-growing nationally |
| 75–89 | Upper quartile (above-average growth) |
| 50–74 | Above median |
| 26–49 | Below median |
| 11–25 | Lower quartile (below-average growth) |
| 1–10 | Bottom 10% nationally |

---

### Block-Level Fields

The `block` table has a distinct schema from all other layers. It contains base housing counts and geographic linkage identifiers, but no derived growth metrics or index fields.

**Block table fields:**

| Field | Type | Description |
|-------|------|-------------|
| `block_geoid` | VARCHAR | 15-digit Census block GEOID (primary key) |
| `HU_20_apr` | INTEGER | Housing units, April 1, 2020 |
| `gq_20_apr` | INTEGER | Group quarters persons, April 1, 2020 |
| `HU_24_jul` | INTEGER | Housing units, July 1, 2024 |
| `gq_24_jul` | INTEGER | Group quarters persons, July 1, 2024 |
| `HU_25_jul` | INTEGER | Housing units, July 1, 2025 |
| `gq_25_jul` | INTEGER | Group quarters persons, July 1, 2025 |
| `HU_25_nov` | INTEGER | Housing units, November 1, 2025 |
| `gq_25_nov` | INTEGER | Group quarters persons, November 1, 2025 |
| `block_part_recs` | INTEGER | Number of block records contributing to this entry |
| `zcta_20` | VARCHAR | ZCTA assignment for this block (2020 vintage) |
| `block_fips_2022` | VARCHAR | 2022-vintage block GEOID (Connecticut remapping; see Data Quality Notes) |
| `ua` | VARCHAR | Urban area code assigned to this block |
| `uaname` | VARCHAR | Urban area name |
| `county.x` | VARCHAR | County FIPS from primary crosswalk |
| `place` | VARCHAR | Place code assigned to this block |
| `placename` | VARCHAR | Place name |
| `county.y` | VARCHAR | County FIPS from secondary crosswalk |
| `cousub` | VARCHAR | County subdivision code assigned to this block |
| `mcdname` | VARCHAR | County subdivision name |
| `county_fips` | VARCHAR | Final county FIPS assignment for this block |

---

```{=typst}
#pagebreak()
```

## Calculation Rules

### Housing Growth (Absolute Change)

**Formula:**

```
hg_{A}_{B} = HU_{B} − HU_{A}
```

**Null handling:** If either `HU_{A}` or `HU_{B}` is NULL, the result is NULL. If both are zero, the result is zero.

**Interpretation:** Positive = net housing added; negative = net housing removed. Values near zero in a high-growth area may reflect offsetting additions and demolitions rather than no activity.

---

### Housing Growth Index (Ratio)

**Formula:**

```
hgi_{A}_{B} = HU_{B} / HU_{A}
```

**Null and zero handling:**

| Condition | Result |
|-----------|--------|
| `HU_{A}` is NULL or zero | NULL (division undefined or infinite) |
| `HU_{B}` is NULL | NULL |
| `HU_{A} > 0`, `HU_{B} = 0` | `0.0` (complete depopulation) |
| Both zero | NULL |

**Interpretation reference:**

| hgi Value | Meaning |
|-----------|---------|
| 1.00 | No net change |
| 1.05 | 5% growth |
| 1.10 | 10% growth |
| 1.25 | 25% growth |
| 0.95 | 5% decline |
| 0.90 | 10% decline |

---

### Compound Annual Growth Rate (CAGR)

Annualizes a growth index over the actual number of years in the measurement period, enabling fair comparison across periods of different lengths.

**Formula:**

```
cagr_{A}_{B} = hgi_{A}_{B} ^ (1 / years_{A_to_B}) − 1
```

**Time period parameters:**

| Period | Start | End | Years |
|--------|-------|-----|-------|
| `20_apr_24_jul` | April 1, 2020 | July 1, 2024 | 4.25 |
| `24_jul_25_nov` | July 1, 2024 | November 1, 2025 | 1.333 |
| `25_jul_25_nov` | July 1, 2025 | November 1, 2025 | 0.333 |
| `20_apr_25_jul` | April 1, 2020 | July 1, 2025 | 5.25 |
| `20_apr_25_nov` | April 1, 2020 | November 1, 2025 | 5.583 |

**Null handling:** NULL if `hgi` is NULL or ≤ 0.

**Benchmark reference:**

| CAGR (annual) | Characterization |
|---------------|-----------------|
| > 5% | Exceptional growth (typically small geographies or active boom markets) |
| 3–5% | Very fast (major growth corridors, high-demand metros) |
| 2–3% | Fast (above-average Sunbelt/suburban growth) |
| 1–2% | Moderate (national average range) |
| 0.5–1% | Slow (mature or built-out areas) |
| 0–0.5% | Minimal or flat |
| −0.5–0% | Slight decline |
| < −1% | Significant decline |

---

### Simple Annual Growth Rate (AGR)

Used for the single 1-year period (July 2024 to July 2025). Equivalent to CAGR for a 1-year period.

**Formula:**

```
agr_24_jul_25_jul = hgi_24_jul_25_jul − 1
```

---

### Comparative Index vs Parent Geography

Normalizes a child geography's growth rate relative to a parent geography's growth rate, centered at 100.

**Formula:**

```
idx_{scope}_{metric} = ((hgi_child − 1) / (hgi_parent − 1)) × 100
```

**Null handling:** NULL if either `hgi` is NULL, or if `hgi_parent = 1.0` (parent has zero growth; the denominator would be zero).

**Example:**

| Geography | `hgi_20_apr_24_jul` | Result |
|-----------|---------------------|--------|
| Block group (child) | 1.15 | — |
| Travis County (parent) | 1.10 | `(0.15 / 0.10) × 100 = 150` |

Interpretation: this block group grew 50% faster than Travis County over the same period.

---

### National Percentile Ranking

Ranks all geographies of a given type nationally by growth metric, expressed as a percentile from 1 (slowest) to 100 (fastest).

**Formula:**

```
pctl_us_{metric} = CEILING(RANK(metric, ascending) / N × 100)
```

Where `N` is the total number of non-NULL geographies for that layer. Ties receive the minimum rank. NULL growth indices are excluded from ranking and receive NULL percentile values.

---

### Geographic Aggregation from Blocks

All higher-geography counts are derived by aggregating block-level housing counts:

```
HU_{period} for {geography} = SUM(HU_{period}) across all blocks in {geography}
```

Growth metrics are then computed from the aggregated counts, not summed from block-level metrics.

---

```{=typst}
#pagebreak()
```

## Data Quality Notes

### Zero-Housing-Unit Geographies

Blocks with zero housing units in all periods are retained in the `block` table. Growth indices for such blocks are NULL (division by zero is undefined). These blocks do not affect aggregated counts for higher geographies and are excluded from percentile rankings. Analysts filtering for active housing markets should apply a minimum threshold (e.g., `HU_20_apr >= 50`).

---

### Anomalous Growth Rates

Some geographies display unusually high or low growth rates. Common causes include:

- **Large multi-unit developments**: A single large apartment complex added to a previously sparse block group can produce very high growth indices.
- **Geocoding corrections**: Housing units that were geocoded to the wrong census block in one vintage and corrected in a later vintage will produce apparent "phantom" growth in the corrected block and apparent decline in the original block. This pattern is most prevalent in dense urban areas where many large multi-unit buildings are in close proximity. **Analysts should treat opposing adjacent growth/decline patterns in dense urban areas with caution.**
- **Annexations or boundary changes**: Municipal annexations can shift housing units between geographies across vintages.
- **Group quarters reclassification**: Changes in how Census classifies a facility between housing units and group quarters can affect HU counts without actual construction or demolition.

---

### Connecticut Block Remapping

The Census Bureau transitioned its Connecticut reporting geographies in 2022 from county-based to Planning Region-based definitions. This change broke GEOID continuity: 2020 block GEOIDs (county-based) do not match 2022+ block GEOIDs (planning region-based), affecting approximately 60,000 Connecticut blocks (state FIPS `09`).

**Resolution**: All Connecticut block data in this product has been re-mapped to 2022-vintage block GEOIDs. The field `block_fips_2022` in the `block` table preserves the 2022-vintage GEOID for reference. All time-series comparisons within Connecticut are valid without additional adjustment by the analyst.

---

### ZCTA Cross-State Assignment

ZCTAs can span multiple states. The `n_states` field in the `zcta` table indicates how many states a given ZCTA covers. For state-level index comparisons, each ZCTA is assigned to a single state based on maximum land area overlap. Border ZCTAs may have counterintuitive state assignments in some cases.

---

### Small-Geography Instability

Growth rates for small geographies (low base housing unit counts) are mathematically volatile. A change of five housing units in a block group with twenty housing units produces a 25% growth rate; the same five units in a 2,000-unit block group produces 0.25%. Analysts building models or maps from granular data should consider applying a minimum base count filter appropriate to their use case.

---

### Cross-State Boundary Anomalies (Counties, Tracts, Block Groups)

A small number of census counties, tracts, and block groups physically cross state boundaries due to geographic boundary edge cases. These anomalies have been resolved: secondary state rows containing zero housing units have been removed from all tables. All housing units are retained in the primary state row. The primary key for each of these geographies is therefore unique on the GEOID alone.

---

### Primary Key Structure for Complex Geographies

Places, county subdivisions, and urban areas are **not** globally unique on their FIPS/Census codes alone. Unique identification requires a composite key:

- **Place**: `place` (5-digit) + `state_code` (2-digit)
- **County Subdivision**: `cousub` (5-digit) + `county_fips` (5-digit)
- **Urban Area**: `ua` (5-digit) + `state_code` (2-digit)

Note that places sharing a name across state lines (e.g., Kansas City, MO and Kansas City, KS) carry different FIPS codes in addition to different state codes. CBSAs are unique on `cbsa23` alone.

---

```{=typst}
#pagebreak()
```

## Appendices

### Appendix A: Geographic Identifier Reference

| Geography | Field Name | Format | Length | Example | Construction |
|-----------|-----------|--------|--------|---------|--------------|
| Census Block | `block_geoid` | SSCCCTTTTTTBBBB | 15 | `481410010021000` | State(2) + County(3) + Tract(6) + Block(4) |
| Block Group | `block_group` | SSCCCTTTTTTG | 12 | `481410010021` | First 12 chars of block GEOID |
| Census Tract | `tract` | SSCCCTTTTTT | 11 | `48141001002` | First 11 chars of block GEOID |
| County | `county_fips` | SSCCC | 5 | `48141` | State(2) + County(3) |
| County Subdivision | `cousub` + `county_fips` | NNNNN + SSCCC | 5+5 | `90185` + `48141` | Subdivision(5) within County(5) |
| Place | `place` + `state_code` | PPPPP + SS | 5+2 | `15976` + `48` | Place(5) within State(2) |
| Urban Area | `ua` + `state_code` | UUUUU + SS | 5+2 | `17860` + `48` | UA(5) within State(2) |
| ZCTA | `zcta_20` | ZZZZZ | 5 | `78701` | 5-digit ZCTA code (2020 vintage) |
| CBSA | `cbsa23` | CCCCC | 5 | `12420` | OMB CBSA code (2023 vintage) |
| State | `state_code` | SS | 2 | `48` | 2-digit FIPS state code |

---

### Appendix B: State FIPS Codes

| FIPS | State | Abbrev | FIPS | State | Abbrev |
|------|-------|--------|------|-------|--------|
| 01 | Alabama | AL | 30 | Montana | MT |
| 02 | Alaska | AK | 31 | Nebraska | NE |
| 04 | Arizona | AZ | 32 | Nevada | NV |
| 05 | Arkansas | AR | 33 | New Hampshire | NH |
| 06 | California | CA | 34 | New Jersey | NJ |
| 08 | Colorado | CO | 35 | New Mexico | NM |
| 09 | Connecticut | CT | 36 | New York | NY |
| 10 | Delaware | DE | 37 | North Carolina | NC |
| 11 | District of Columbia | DC | 38 | North Dakota | ND |
| 12 | Florida | FL | 39 | Ohio | OH |
| 13 | Georgia | GA | 40 | Oklahoma | OK |
| 15 | Hawaii | HI | 41 | Oregon | OR |
| 16 | Idaho | ID | 42 | Pennsylvania | PA |
| 17 | Illinois | IL | 44 | Rhode Island | RI |
| 18 | Indiana | IN | 45 | South Carolina | SC |
| 19 | Iowa | IA | 46 | South Dakota | SD |
| 20 | Kansas | KS | 47 | Tennessee | TN |
| 21 | Kentucky | KY | 48 | Texas | TX |
| 22 | Louisiana | LA | 49 | Utah | UT |
| 23 | Maine | ME | 50 | Vermont | VT |
| 24 | Maryland | MD | 51 | Virginia | VA |
| 25 | Massachusetts | MA | 53 | Washington | WA |
| 26 | Michigan | MI | 54 | West Virginia | WV |
| 27 | Minnesota | MN | 55 | Wisconsin | WI |
| 28 | Mississippi | MS | 56 | Wyoming | WY |
| 29 | Missouri | MO | | | |

---

### Appendix C: CAGR Benchmark Reference

| Annual CAGR | Characterization | Typical Context |
|-------------|-----------------|-----------------|
| > 5.0% | Exceptional | Small geographies, active boom areas |
| 3.0–5.0% | Very fast | Major Sunbelt growth corridors (Austin, Phoenix, Nashville) |
| 2.0–3.0% | Fast | Strong suburban expansion markets |
| 1.0–2.0% | Moderate | Near national average |
| 0.5–1.0% | Slow | Mature, built-out, or stable markets |
| 0.0–0.5% | Minimal | Flat or low-turnover markets |
| −0.5–0.0% | Slight decline | Slow outmigration or aging housing stock |
| −1.0– −0.5% | Moderate decline | Rust Belt cities, depopulating rural areas |
| < −1.0% | Significant decline | Distressed markets, severe outmigration |

---

### Appendix D: Index Value Interpretation

| Index Value | Interpretation |
|-------------|---------------|
| > 300 | Growing more than 3× faster than parent geography |
| 200–300 | Growing 2–3× faster than parent geography |
| 150–200 | Growing significantly faster than parent geography |
| 110–150 | Growing moderately faster than parent geography |
| 90–110 | Growing at approximately same rate as parent geography |
| 50–90 | Growing slower than parent geography |
| 0–50 | Growing much slower than parent geography |
| 0 | No growth while parent geography is growing |
| < 0 | Declining while parent is growing, or vice versa |
| NULL | Parent geography has zero net growth (index indeterminate) |

---

```{=typst}
#pagebreak()
```

## Document Generation Metadata

**Generated By:** Claude Code (claude-sonnet-4-6)
**Workflow Used:** Direct documentation generation from ground-truth sources
**Generation Date:** 2026-02-24

### Source Files Used

**Ground-truth sources (per guidelines Golden Rule):**

- `/home/joel/data/housing_distro.duckdb` — actual delivered database; schema and record counts extracted directly
- `/home/joel/01_housing/housing-distro-build.R` — distribution build script; layer construction and naming logic
- `/home/joel/00_clientinfra/README.md` — client provisioning system overview
- `/home/joel/00_clientinfra/CLIENT_PROVISIONING.md` — client provisioning process and SAS URL workflow

**Reference sources (for context only, no technical detail extracted):**

- `/home/joel/core/docs/housing-docs/housing-layouts-and-rules.md` — prior version documentation
- `/home/joel/01_housing/doc-guidelines.md` — documentation guidelines and requirements

### Sub-Agents

**None** — all work completed in primary session

### Workflow Execution Notes

- All field names, table names, and record counts verified against live DuckDB query output
- Time period tokens (`_20_apr`, `_24_jul`, `_25_jul`, `_25_nov`) reflect actual column names in `housing_distro.duckdb`, which differ from the prior documentation (which used `_20`, `_24`, `_25`)
- Four data vintages are now present (vs. three in prior documentation); `HU_25_nov` / `gq_25_nov` are new additions
- Index and percentile fields are now embedded in layer tables directly (not in separate index tables as in prior schema)
- Block table schema differs materially from prior documentation; crosswalk identifiers are now carried directly in the block table
