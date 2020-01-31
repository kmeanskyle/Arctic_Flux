# Script Summary
#   Compile dataset from data collected at Poker Flat Research Range 
#   (PI: H. Kobayashi)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/PFRR.csv

#-- Setup ---------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    miss_lst <- lapply(temp_dt, function(col) which(col == -9999))
    for(j in seq_along(temp_dt)){
      set(temp_dt, i=miss_lst[[j]], j=j, value=NA)
    }
    temp_dt[, (vars[1]) := rowMeans(.SD, na.rm = TRUE)]
    new_miss <- Reduce(union, miss_lst)
    new_miss <- intersect(new_miss, which(is.na(temp_dt[[vars[1]]])))
    temp_dt[new_miss, (vars[1]) := -9999]
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
  # qc flags are either 0 or -9999. Assign 0 if both are 0, else -9999
  aggr_qc <- function(vars) {
    rpl <- DT[[vars[2]]] == 0 & DT[[vars[3]]] == 0
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

#-- PFRR ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

# PFRR is a unique case in that there is unique data in each of AmeriFlux and 
#   FluxNet databases, as well as some overlap. 

# AmeriFlux Data
af_vars <- c(
  "TIMESTAMP_START",
  "LE_1_1_1",
  "LE_PI_F_1_1_1",
  "H_1_1_1",
  "H_PI_F_1_1_1",
  "G_1_1_1",
  "G_1_1_2",
  "SW_IN",
  "SW_OUT",
  "LW_IN",
  "LW_OUT",
  "TA_1_1_1",
  "RH_1_7_1",
  "WS_1_8_1",
  "WD_1_1_1",
  "P_RAIN",
  "D_SNOW_1_1_1",
  "D_SNOW_1_1_2",
  "D_SNOW_1_1_3",
  "TS_1_1_1",
  "SWC_1_1_1"
)
# Timestamp start
# Latent heat flux filtered/gapfilled
# Sensible heat flux filtered/gapfilled
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Rainfall
# Snow depths
# Soil temperature
# Soil water content

af_fp <- "../raw_data/AmeriFlux/AMF_US-Prr_BASE-BADM_3-5/AMF_US-Prr_BASE_HH_3-5.csv"
pf <- fread(af_fp, select = af_vars, skip = 2)

# FluxNet data
fn_vars <- c(
  "TIMESTAMP_START",
  "G_F_MDS",
  "G_F_MDS_QC",
  "SW_IN_F",
  "SW_IN_F_QC", 
  "LW_IN_F",
  "LW_IN_F_QC",
  "TA_F",
  "TA_F_QC",
  "P_F", 
  "P_F_QC",
  "TS_F_MDS_1",
  "TS_F_MDS_1_QC",
  "SWC_F_MDS_2",
  "SWC_F_MDS_2_QC"
)
# Timestamp start
# Ground heat flux gapfilled 
# Ground heat flux gapfill quality flag
# Incoming shortwave radiation gapfilled (consolidated from MDS and ERA)
# Incoming shortwave radiation gapfill quality flag
# Incoming longwave radiation gapfilled (consolidated from MDS and ERA)
# Incoming longwave radiation gapfill quality flag
# Ambient air temperature gapfilled
# Ambient air temperature gapfill quality flag
# Wind speed gapfilled
# Wind speed gapfil quality flag
# Precipitation gapfilled
# Precipitation gapfill quality flag
# Soil temperature gapfilled
# Soil temperature gapfill quality flag
# Soil water content gapfilled (all SWC_F_MDS_1 are -9999)
# Soil water content gapfill quality flag

fn_fp <- "../raw_data/FluxNet/FLX_US-Prr_FLUXNET2015_FULLSET_2010-2014_1-3/FLX_US-Prr_FLUXNET2015_FULLSET_HH_2010-2014_1-3.csv"
fn_pf <- fread(fn_fp, select = fn_vars)

# Join AmeriFlux and FluxNet versions
pf <- fn_pf[pf, on = "TIMESTAMP_START"]
# convert from integer timestamp
pf <- convert_ts(pf)
# add site id
pf$stid <- "pfrr"

# aggregate variables by mean
aggr_lst <- list(
  c("G_1_1_avg", "G_1_1_1", "G_1_1_2"),
  c("snowd", "D_SNOW_1_1_1", "D_SNOW_1_1_2", "D_SNOW_1_1_3")
)
#pf <- aggr_vars(pf, aggr_var_lst)
pf <- mk_vars(pf, aggr_lst, "aggr")

# mesh vars from both AF and FN databases where FN missing data
mesh_lst <- list(
  c("g", "G_1_1_avg", "G_F_MDS"),
  c("sw_in", "SW_IN", "SW_IN_F"),
  c("lw_in", "LW_IN", "LW_IN_F"),
  c("ta", "TA_1_1_1", "TA_F"),
  c("precip", "P_RAIN", "P_F"),
  c("tsoil", "TS_1_1_1", "TS_F_MDS_1"),
  c("swc", "SWC_1_1_1", "SWC_F_MDS_2")
)
pf <- mk_vars(pf, mesh_lst, "mesh")

# assign qc flags for gapfilled vars not included
fill_qc_lst <- list(
  c("le_qc", "LE_1_1_1", "LE_PI_F_1_1_1"),
  c("h_qc", "H_1_1_1", "H_PI_F_1_1_1")
)
pf <- mk_vars(pf, fill_qc_lst, "fill_qc")

# rename vars
names_lst <- list(
  c("G_F_MDS_QC", "g_qc"),
  c("SW_IN_F_QC", "sw_in_qc"),
  c("LW_IN_F_QC", "lw_in_qc"),
  c("TA_F_QC", "ta_qc"),
  c("P_F_QC", "precip_qc"),
  c("TS_F_MDS_1_QC", "tsoil_qc"),
  c("SWC_F_MDS_2_QC", "swc_qc"),
  c("LE_PI_F_1_1_1", "le"),
  c("H_PI_F_1_1_1", "h"),
  c("SW_OUT", "sw_out"),
  c("LW_OUT", "lw_out"),
  c("RH_1_7_1", "rh"),
  c("WS_1_8_1", "ws"),
  c("WD_1_1_1", "wd")
)
pf <- fix_names(pf, names_lst)

pf <- add_qc(pf)
pf <- calc_rnet(pf)
pf <- sel_cols(pf)

fwrite(pf, "../data/Arctic_Flux/aggregate/PFRR.csv")

#------------------------------------------------------------------------------
