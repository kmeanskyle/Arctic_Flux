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
  # add missing variables with names given
  miss <- function(vars) {
    rep(NA, dim(DT)[1])
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     miss = miss))
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

scb_fp <- "../raw_data/AmeriFlux/AMF_CA-SCB_BASE-BADM_1-5/AMF_CA-SCB_BASE_HH_1-5.csv"
scc_fp <- gsub("SCB", "SCC", scb_fp)

scb_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "H",
  "G_1_1_1", "G_2_1_1",
  "SW_IN",
  "SW_OUT",
  "LW_IN",
  "LW_OUT",
  "TA_1_1_1", "TA_1_1_2", "TA_1_1_3",
  "RH_1_1_1",
  "WS",
  "P",
  "D_SNOW",
  "TS_1_1_1"
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
# Precipitation
# Soil temperature

scc_vars <- c(scb_vars, "SWC_1_1_1", "SWC_2_1_1", "WD_1_1_1", "WD_1_1_2")
scc_vars[grepl("^P$", scc_vars)] <- "P_RAIN"

scb <- fread(scb_fp, select = scb_vars, skip = 2)
scc <- fread(scc_fp, select = scc_vars, skip = 2)

scb <- convert_ts(scb)
scc <- convert_ts(scc)

scb$stid <- "SCCB"
scc$stid <- "SCCL"

# Add missing variables as NA
miss_vars <- c("wd", "swc")
scb <- mk_vars(scb, miss_vars, "miss")

# aggregate variables by mean
scb_aggr <- list(
  c("g", "G_1_1_1", "G_2_1_1"),
  c("ta", "TA_1_1_1", "TA_1_1_2", "TA_1_1_3")
)
scc_aggr <- scb_aggr
scc_aggr[[3]] <- c("swc", "SWC_1_1_1", "SWC_2_1_1")
scc_aggr[[4]] <- c("wd", "WD_1_1_1", "WD_1_1_2")

scb <- mk_vars(scb, scb_aggr, "aggr")
scc <- mk_vars(scc, scc_aggr, "aggr")

# rename vars
scb_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("LW_IN", "lw_in"),
  c("LW_OUT", "lw_out"),
  c("RH_1_1_1", "rh"),
  c("WS", "ws"),
  c("P", "precip"),
  c("D_SNOW", "snowd"),
  c("TS_1_1_1", "tsoil")
)

scc_names <- scb_names
scc_names[[9]][1] <- "P_RAIN"

scb <- fix_names(scb, scb_names)
scc <- fix_names(scc, scc_names)

# add NA qc flags
scb <- add_qc(scb)
scc <- add_qc(scc)

# calculate net radiation
scb <- calc_rnet(scb)
scc <- calc_rnet(scc)

# select columns
scb <- sel_cols(scb)
scc <- sel_cols(scc)

fwrite(scb, "../data/Arctic_Flux/aggregate/SCCB.csv")
fwrite(scc, "../data/Arctic_Flux/aggregate/SCCL.csv")

#------------------------------------------------------------------------------
