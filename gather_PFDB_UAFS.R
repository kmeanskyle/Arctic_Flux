# Script Summary
#   Compile datasets from flux data collected at UAF and Poker Flat deciduous
#   succession from burn scar site (PI: H. Iwata)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/PFDB.csv
#   ../data/Arctic_Flux/aggregate/UAFS.csv

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
  # Copy non-gapfilled data over missing (NA) values in gapfilled time series
  #   var_key is a list of pairs of column names where the first position is the
  #   name of the column to be copied and the second position is the target
  mesh <- function(vars) {
    target <- DT[[vars[3]]]
    rpl <- is.na(target)
    target[rpl] <- DT[[vars[2]]][rpl]
    target
  }
  # add qc flags for gapfilled measurements where qc flag not included
  #   and where raw data available. 0 for "close" to observed value, -9999 
  #   otherwise (unknown gapfill quality)
  fill_qc <- function(vars) {
    rpl <- DT[[vars[2]]] != -9999 & DT[[vars[3]]] != -9999 & 
      abs(DT[[vars[2]]] - DT[[vars[3]]]) < 5
    new_vec <- rep(-9999, dim(DT)[1])
    new_vec[rpl] <- 0
    new_vec
  }
  # qc flags are either 0 or -9999. Assign 0 if both/all are 0, else -9999
  aggr_qc <- function(vars) {
    rpl <- Reduce("&", lapply(vars[-1], function(var) DT[[var]] == 0))
    new_vec <- rep(-9999, dim(DT)[1])
    new_vec[rpl] <- 0
    new_vec
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     mesh = mesh,
                                     fill_qc = fill_qc,
                                     aggr_qc = aggr_qc))
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

#------------------------------------------------------------------------------

#-- PFDB ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

pf_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "LE_PI_F",
  "H",
  "H_PI_F",
  "G_1_1_1", "G_PI_F_1_1_1",
  "G_1_1_2", "G_PI_F_1_1_2",
  "SW_IN_1_1_1", "SW_IN_PI_F_1_1_1",
  "SW_IN_1_1_2", "SW_IN_PI_F_1_1_2",
  "SW_OUT_1_1_1", "SW_OUT_PI_F_1_1_1",
  "SW_OUT_1_1_2", "SW_OUT_PI_F_1_1_2",
  "LW_IN_1_1_1", "LW_IN_PI_F_1_1_1",
  "LW_OUT_1_1_1", "LW_OUT_PI_F_1_1_1",
  "NETRAD_1_1_1", "NETRAD_PI_F_1_1_1", 
  "NETRAD_1_1_2", "NETRAD_PI_F_1_1_2",
  "TA_1_2_1", "TA_PI_F_1_2_1",
  "RH_1_2_1", "RH_PI_F_1_2_1",
  "WS_1_1_1",
  "WD",
  "P_RAIN",
  "D_SNOW",
  "TS_1_1_1", "TS_PI_F_1_1_1",
  "SWC_1_1_1",
  "SWC_1_1_2"
)
# Timestamp start
# Latent heat flux (raw and filtered/gapfilled)
# Sensible heat flux (raw and filtered/gapfilled)
# Ground heat flux (raw and filtered/gapfilled)
# Incoming shortwave radiation (raw and filtered/gapfilled)
# Outgoing shortwave radiation (raw and filtered/gapfilled)
# Incoming longwave radiation (raw and filtered/gapfilled)
# Outgoing longwave radiation (raw and filtered/gapfilled)
# Net radiation (raw and filtered/gapfilled)
# Ambient air temperature (raw and filtered/gapfilled)
# Relative humidity (raw and filtered/gapfilled)
# Wind speed
# Wind direction
# Rainfall
# Snow depths
# Soil temperature (raw and filtered/gapfilled)
# Soil water content

pf_fp <- "../raw_data/AmeriFlux/AMF_US-Rpf_BASE-BADM_2-5/AMF_US-Rpf_BASE_HH_2-5.csv"
pf <- fread(pf_fp, select = pf_vars, skip = 2)

# convert from integer timestamp
pf <- convert_ts(pf)
# add site id
pf$stid <- "pfdb"

# assign qc flags for gapfilled variables not included
fill_qc_lst <- list(
  c("le_qc", "LE", "LE_PI_F"),
  c("h_qc", "H", "H_PI"),
  c("g1_qc", "G_1_1_1", "G_PI_F_1_1_1"),
  c("g2_qc", "G_1_1_2", "G_PI_F_1_1_2"),
  c("sw_in1_qc", "SW_IN_1_1_1", "SW_IN_PI_F_1_1_1"),
  c("sw_in2_qc", "SW_IN_1_1_2", "SW_IN_PI_F_1_1_2"),
  c("lw_in_qc", "LW_IN_1_1_1", "LW_IN_PI_F_1_1_1"),
  c("sw_out1_qc", "SW_OUT_1_1_1", "SW_OUT_PI_F_1_1_1"),
  c("sw_out2_qc", "SW_OUT_1_1_2", "SW_OUT_PI_F_1_1_2"),
  c("lw_out_qc", "LW_OUT_1_1_1", "LW_OUT_PI_F_1_1_1"),
  c("rnet1_qc", "NETRAD_1_1_1", "NETRAD_PI_F_1_1_1"),
  c("rnet2_qc", "NETRAD_1_1_2", "NETRAD_PI_F_1_1_2"),
  c("ta_qc", "TA_1_2_1", "TA_PI_F_1_2_1"),
  c("rh_qc", "RH_1_2_1", "RH_PI_F_1_2_1"),
  c("tsoil_qc", "TS_1_1_1", "TS_PI_F_1_1_1")
)
pf <- mk_vars(pf, fill_qc_lst, "fill_qc")

# aggr qc flags
aggr_qc_lst <- list(
  c("g_qc", "g1_qc", "g2_qc"),
  c("sw_in_qc", "sw_in1_qc", "sw_in2_qc"),
  c("sw_out_qc", "sw_out1_qc", "sw_out2_qc"),
  c("rnet_qc", "rnet1_qc", "rnet2_qc")
)
pf <- mk_vars(pf, aggr_qc_lst, "aggr_qc")

# aggregate variables by mean
aggr_lst <- list(
  c("g", "G_PI_F_1_1_1", "G_PI_F_1_1_2"),
  c("sw_in", "SW_IN_PI_F_1_1_1", "SW_IN_PI_F_1_1_2"),
  c("sw_out", "SW_OUT_PI_F_1_1_1", "SW_OUT_PI_F_1_1_2"),
  c("rnet", "NETRAD_PI_F_1_1_1", "NETRAD_PI_F_1_1_2"),
  c("swc", "SWC_1_1_1", "SWC_1_1_2")
)
pf <- mk_vars(pf, aggr_lst, "aggr")

# rename vars
names_lst <- list(
  c("LE_PI_F", "le"),
  c("H_PI_F", "h"),
  c("LW_IN_PI_F_1_1_1", "lw_in"),
  c("LW_OUT_PI_F_1_1_1", "lw_out"),
  c("TA_PI_F_1_2_1", "ta"),
  c("RH_PI_F_1_2_1", "rh"),
  c("WS_1_1_1", "ws"),
  c("WD", "wd"),
  c("P_RAIN", "precip"),
  c("D_SNOW", "snowd"),
  c("TS_PI_F_1_1_1", "tsoil")
)
pf <- fix_names(pf, names_lst)

pf <- add_qc(pf)
pf <- sel_cols(pf)

fwrite(pf, "../data/Arctic_Flux/aggregate/PFBD.csv")

#------------------------------------------------------------------------------

#-- UAFS ----------------------------------------------------------------------
uaf_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "H",
  "G_1_1_1", "G_PI_F_1_1_1",
  "G_1_1_2", "G_PI_F_1_1_2",
  "G_1_1_3", "G_PI_F_1_1_3",
  "G_1_1_4", "G_PI_F_1_1_4",
  "SW_IN", "SW_IN_PI_F",
  "LW_IN", "LW_IN_PI_F", 
  "SW_OUT", "SW_OUT_PI_F",
  "LW_OUT", "LW_OUT_PI_F",
  "NETRAD_PI_F_1_1_1",
  "NETRAD_PI_F_1_1_2",
  "TA_1_4_1", "TA_PI_F_1_4_1",
  "RH_1_4_1", "RH_PI_F_1_4_1",
  "WS_1_3_1", "WS_PI_F_1_3_1",
  "WD", "WD_PI_F",
  "P_RAIN", "P_RAIN_PI_F",
  "D_SNOW",
  "TS_PI_F_1_1_1",
  "TS_PI_F_1_1_2",
  "TS_PI_F_1_1_3",
  "SWC_PI_F_1_1_1",
  "SWC_PI_F_1_1_2"
)
# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux (raw and filtered/gapfilled)
# Incoming shortwave radiation (raw and filtered/gapfilled)
# Outgoing shortwave radiation (raw and filtered/gapfilled)
# Incoming longwave radiation (raw and filtered/gapfilled)
# Outgoing longwave radiation (raw and filtered/gapfilled)
# Net radiation (raw and filtered/gapfilled)
# Ambient air temperature (raw and filtered/gapfilled)
# Relative humidity (raw and filtered/gapfilled)
# Wind speed (raw and filtered/gapfilled)
# Wind direction (raw and filtered/gapfilled)
# Rainfall (raw and filtered/gapfilled)
# Snow depths
# Soil temperature (only filtered/gapfilled)
# Soil water content

fp <- "../raw_data/AmeriFlux/AMF_US-Uaf_BASE-BADM_2-5/AMF_US-Uaf_BASE_HH_2-5.csv"
uaf <- fread(fp, select = uaf_vars, skip = 2)

# convert from integer timestamp
uaf <- convert_ts(uaf)
# add site id
uaf$stid <- "uafs"

# assign qc flags for gapfilled variables where not included
#   (and having raw data available)
fill_qc_lst <- list(
  c("g1_qc", "G_1_1_1", "G_PI_F_1_1_1"),
  c("g2_qc", "G_1_1_2", "G_PI_F_1_1_2"),
  c("g3_qc", "G_1_1_3", "G_PI_F_1_1_3"),
  c("g4_qc", "G_1_1_4", "G_PI_F_1_1_4"),
  c("sw_in_qc", "SW_IN", "SW_IN_PI_F"),
  c("lw_in_qc", "LW_IN", "LW_IN_PI_F"),
  c("sw_out_qc", "SW_OUT", "SW_OUT_PI_F"),
  c("lw_out_qc", "LW_OUT", "LW_OUT_PI_F"),
  c("ta_qc", "TA_1_4_1", "TA_PI_F_1_4_1"),
  c("rh_qc", "RH_1_4_1", "RH_PI_F_1_4_1"),
  c("ws_qc", "WS_1_3_1", "WS_PI_F_1_3_1"),
  c("precip_qc", "P_RAIN", "P_RAIN_PI_F")
)
uaf <- mk_vars(uaf, fill_qc_lst, "fill_qc")

# aggr qc flags
aggr_qc_lst <- list(c("g_qc", "g1_qc", "g2_qc", "g3_qc", "g4_qc"))
uaf <- mk_vars(uaf, aggr_qc_lst, "aggr_qc")

# aggregate variables by mean
aggr_lst <- list(
  c("g", "G_PI_F_1_1_1", "G_PI_F_1_1_2", "G_PI_F_1_1_3", "G_PI_F_1_1_4"),
  c("rnet", "NETRAD_PI_F_1_1_1", "NETRAD_PI_F_1_1_2"),
  c("tsoil", "TS_PI_F_1_1_1", "TS_PI_F_1_1_2", "TS_PI_F_1_1_3"),
  c("swc", "SWC_PI_F_1_1_1", "SWC_PI_F_1_1_2")
)
uaf <- mk_vars(uaf, aggr_lst, "aggr")

# rename vars
names_lst <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN_PI_F", "sw_in"),
  c("LW_IN_PI_F", "lw_in"),
  c("SW_OUT_PI_F", "sw_out"),
  c("LW_OUT_PI_F", "lw_out"),
  c("TA_PI_F_1_4_1", "ta"),
  c("RH_PI_F_1_4_1", "rh"),
  c("WS_PI_F_1_3_1", "ws"),
  c("WD_PI_F", "wd"),
  c("P_RAIN_PI_F", "precip"),
  c("D_SNOW", "snowd")
)
uaf <- fix_names(uaf, names_lst)

uaf <- add_qc(uaf)
uaf <- sel_cols(uaf)

fwrite(uaf, "../data/Arctic_Flux/aggregate/UAFS.csv")

#------------------------------------------------------------------------------
