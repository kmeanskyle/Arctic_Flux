# Script Summary
#   Compile datasets from flux data collected at Northern old black spruce
#   site in Manitoba (PI: B. Amiro)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/CAMB.csv

#-- Setup ---------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    temp_dt[temp_dt == -9999] <- NA
    temp_dt <- temp_dt[, (vars[1]) := rowMeans(.SD, na.rm = TRUE)]
    temp_dt[which(is.na(temp_dt[[vars[1]]])), (vars[1]) := -9999]
    temp_dt[[vars[1]]]
  }

  new_data <- lapply(var_key, aggr)
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

#------------------------------------------------------------------------------

#-- CAMB ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

mb_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "H",
  "G",
  "SW_IN",
  "SW_OUT",
  "LW_IN",
  "LW_OUT",
  "NETRAD", 
  "TA",
  "RH",
  "WS",
  "WD",
  "P",
  "TS_1", "TS_2",
  "SWC_1", "SWC_2"
)
# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precipitation
# Soil temperature
# Soil water content

mb_fp <- "../raw_data/AmeriFlux/AMF_CA-Man_BASE-BADM_2-1/AMF_CA-Man_BASE_HH_2-1.csv"
mb <- fread(mb_fp, select = mb_vars, skip = 2)

# convert from integer timestamp
mb <- convert_ts(mb)
# add site id
mb$stid <- "camb"
# add NA for D_SNOW
mb$snowd <- NA

# aggregate variables by mean
aggr_lst <- list(
  c("tsoil", "TS_1", "TS_2"),
  c("swc", "SWC_1", "SWC_2")
)
mb <- mk_vars(mb, aggr_lst, "aggr")

# rename vars
names_lst <- list(
  c("LE", "le"),
  c("H", "h"),
  c("G", "g"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("LW_IN", "lw_in"),
  c("LW_OUT", "lw_out"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("P", "precip")
)
mb <- fix_names(mb, names_lst)

mb <- add_qc(mb)
mb <- sel_cols(mb)

fwrite(mb, "../data/Arctic_Flux/aggregate/CAMB.csv")

#------------------------------------------------------------------------------