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

drv <- duckdb('./data/housing.duckdb')
conh <- dbConnect(drv)
dbListTables(conh)

### VISUALIZE ##########################################################################
# Create the scatter plot

hu_county <- as.data.table(
  dbGetQuery(conh, "select * from hu_county")
)

plotdata <- hu_county[substring(co_fips, 1, 2) == "48", ]

thegraph <- plot_ly(
  plotdata,
  x = ~cagr_20_24,
  y = ~agr_25,
  marker = list(
    color = 'darkred',
    line = list(color = 'yellow', width = 1)
  )
) |>
  add_markers(
    size = ~HU_25,
    sizes = c(30, max(plotdata$HU_25 / 1000)),
    text = ~paste("County: ", county_name,
    "<br>HU 24: ", HU_24,
     "<br>HU 25: ", HU_25)
  ) |>
  layout(
    plot_bgcolor = "black",
    yaxis = list(
      title = "AGR in Housing Units,24-25", 
      tickformat = ".2%",
      gridcolor = 'rgba(211, 211, 211, 0.2)',
      range = c(-0.005, 0.2)
    ),
    xaxis = list(
      title = "CAGR in Housing Units, 2020-24", 
      tickformat = ".2%",
      gridcolor = 'rgba(211, 211, 211, 0.2)',
      range = c(-0.005, 0.08)
    )
  )
thegraph

# convert 'states' to a data table with two columns
# 'state_fips' and 'state_name'
# and merge with 'hu_county' to get state names
# and add to the plot
# Create a data table with state FIPS codes and names

states <- list(
  "01" = "Alabama", "02" = "Alaska", "04" = "Arizona", "05" = "Arkansas",
  "06" = "California", "08" = "Colorado", "09" = "Connecticut",
  "10" = "Delaware", "11" = "DistrictofColumbia", "12" = "Florida",
  "13" = "Georgia", "15" = "Hawaii", "16" = "Idaho", "17" = "Illinois",
  "18" = "Indiana", "19" = "Iowa", "20" = "Kansas", "21" = "Kentucky",
  "22" = "Louisiana", "23" = "Maine", "24" = "Maryland", "25" = "Massachusetts",
  "26" = "Michigan", "27" = "Minnesota", "28" = "Mississippi",
  "29" = "Missouri", "30" = "Montana", "31" = "Nebraska", "32" = "Nevada",
  "33" = "NewHampshire", "34" = "NewJersey", "35" = "NewMexico",
  "36" = "NewYork", "37" = "NorthCarolina", "38" = "NorthDakota", "39" = "Ohio",
  "40" = "Oklahoma", "41" = "Oregon", "42" = "Pennsylvania",
  "44" = "RhodeIsland", "45" = "SouthCarolina", "46" = "SouthDakota",
  "47" = "Tennessee", "48" = "Texas", "49" = "Utah", "50" = "Vermont",
  "51" = "Virginia", "53" = "Washington", "54" = "WestVirginia",
  "55" = "Wisconsin", "56" = "Wyoming"
)

states_dt <- data.table(
  state_fips = names(states),
  state_name = unlist(states)
)

hu_county[, state_fips := substring(co_fips, 1, 2)]

# Merge with hu_county to get state names
hu_county <- merge(
  hu_county,
  states_dt,
  by.x = "state_fips",
  by.y = "state_fips",
  all.x = TRUE
)

# Calculate median CAGR by state for ordering
state_medians <- hu_county[, .(median_cagr = median(cagr_20_25, na.rm = TRUE)), by = state_name.x]
state_medians <- setorder(state_medians, -median_cagr)  # Sort in descending order

# Create factor with levels ordered by median CAGR
hu_county$state_ordered <- factor(
  hu_county$state_name.x, 
  levels = state_medians$state_name.x
)

# Create the boxplot with ordered states
boxplot <- plot_ly(
  hu_county,
  #hu_county[cagr_20_25 <= 0.2],  # Filter out counties with cagr_25 > 0.2
  y = ~state_ordered,  # Using the ordered factor
  x = ~cagr_20_25,  # CAGR now on x-axis
  type = "box",
  boxpoints = "all",
  jitter = 0.5,
  pointpos = 0,
  marker = list(
    size = 3,  # Increased marker size from 3 to 5
    color = "rgba(220, 20, 20, 0.15)",  # Reduced opacity for transparency
    line = list(color = "#BCA375", width = .5, opacity = 0.1)  # Light tan outline with some transparency
  ),
  line = list(color = "rgb(0, 120, 0)", width = 2),  # Darker box lines (dark green)
  fillcolor = "rgba(220, 20, 20, 0.05)"  # Slightly color the boxes
) |>
  layout(
    plot_bgcolor = "#2b2b2b",  # Very dark charcoal grey
    paper_bgcolor = "#2b2b2b",  # Very dark charcoal grey
    xaxis = list(
      title = "Counties: CAGR in Housing Units, 2020-25", 
      tickformat = ".2%",
      gridcolor = 'rgba(255, 255, 255, 0.3)',
      tickfont = list(color = 'rgba(255, 255, 255, 0.7)'),
      titlefont = list(color = 'rgba(255, 255, 255, 0.7)')
    ),
    yaxis = list(
      title = "",  # Suppressed y-axis title
      gridcolor = 'rgba(255, 255, 255, 0.3)',
      gridwidth = 1,
      griddash = 'dot',
      tickfont = list(color = 'rgba(255, 255, 255, 0.7)', size = 8)
    ),
    boxmode = "group",
    #boxgap = 0.1,  # Control gap between boxes
    font = list(color = "rgba(255, 255, 255, 0.7)", style = "bold")  # Softer white for better contrast on dark background
  )
boxplot


