
# Script Summary
#   Compile datasets from flux data collected at Scotty Creek sites
#   (PI: M. Helbig)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/SCCB.csv
#   ../data/Arctic_Flux/aggregate/SCCL.csv

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

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

# AmeriFlux Data
bc_vars <- c(
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
  "TS_1",
  "TS_2",
  "SWC_1",
  "SWC_2"
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
# Precip
# Soil temperature
# Soil water content

bc1_fp <- "../raw_data/AmeriFlux/AMF_US-Bn1_BASE-BADM_1-1/AMF_US-Bn1_BASE_HH_1-1.csv"
bc2_fp <- gsub("Bn1", "Bn2", bc1_fp)
bc3_fp <- gsub("Bn1", "Bn2", bc1_fp)

bc1 <- fread(bc1_fp, select = bc_vars, skip = 2)
bc2 <- fread(bc2_fp, select = bc_vars, skip = 2)
bc3 <- fread(bc3_fp, select = bc_vars, skip = 2)

# convert timestamps
bc1 <- convert_ts(bc1)
bc2 <- convert_ts(bc2)
bc3 <- convert_ts(bc3)

# site ids
bc1[, stid := "BCDA"]
bc2[, stid := "BCDB"]
bc3[, stid := "BCDC"]

# Add missing variables as NA
bc1[, snowd := NA]
bc2[, snowd := NA]
bc3[, snowd := NA]

# aggregate variables by mean
aggr_vars <- list(
  c("tsoil", "TS_1", "TS_2"),
  c("swc", "SWC_1", "SWC_2")
)
bc1 <- mk_vars(bc1, aggr_vars, "aggr")
bc2 <- mk_vars(bc2, aggr_vars, "aggr")
bc3 <- mk_vars(bc3, aggr_vars, "aggr")

bc_names <- list(
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

bc1 <- fix_names(bc1, bc_names)
bc2 <- fix_names(bc2, bc_names)
bc3 <- fix_names(bc3, bc_names)

# add QC flags as NAs and select columns
bc1 <- sel_cols(add_qc(bc1))
bc2 <- sel_cols(add_qc(bc2))
bc3 <- sel_cols(add_qc(bc3))

fwrite(bc1, "../data/Arctic_Flux/aggregate/BCDA.csv")
fwrite(bc2, "../data/Arctic_Flux/aggregate/BCDB.csv")
fwrite(bc3, "../data/Arctic_Flux/aggregate/BCDC.csv")

#------------------------------------------------------------------------------
