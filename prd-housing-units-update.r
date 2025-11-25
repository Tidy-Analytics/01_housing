#### GET HOUSING files
### use 

library(DBI)
library(duckdb)
library(data.table)
library(arrow)

# Create a temporary database in memory and attach to the pointer database


# Set up DuckDB connection
drv <- duckdb(dbdir = '/home/joel/data/housing.duckdb')
contd <- dbConnect(drv)

states_list <- data.table(
  state_code_alpha = c(
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi",
    "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn",
    "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh",
    "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa",
    "wv", "wi", "wy"
  ),
  state_name = c(
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "DistrictofColumbia", "Florida", "Georgia", "Hawaii",
    "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", 
    "Missouri", "Montana", "Nebraska", "Nevada", "NewHampshire", "NewJersey", 
    "NewMexico", "NewYork", "NorthCarolina", "NorthDakota", "Ohio", "Oklahoma", 
    "Oregon", "Pennsylvania", "RhodeIsland", "SouthCarolina", "SouthDakota", 
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
    "WestVirginia", "Wisconsin", "Wyoming"
  ),
  state_code = c(
    "01", "02", "04", "05", "06", "08","09", "10", "11", "12", "13", "15",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27",
    "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
    "54", "55", "56"
  )
)
    
## counts <- list()
for (i in seq_along(states_list$state_code)) {
  state_code <- states_list$state_code[i]
  state_name <- states_list$state_name[i]
  url <- paste0(
    "https://www2.census.gov/geo/pvs/addcountlisting/2025/", 
    state_code, "_", state_name, "_AddressBlockCountList_072025.txt"
  )
  
  filename <- paste0(state_code, "_", state_name, "_AddressBlockCountList_072025.txt")
  filepath <- file.path("/home/joel/data/geo", filename)
  
  # Use wget to download the file
  system(paste("wget", "-O", shQuote(filepath), shQuote(url)))
}


  ## example: https://www2.census.gov/geo/pvs/addcountlisting/2025/01_Alabama_AddressBlockCountList_072025.txt
