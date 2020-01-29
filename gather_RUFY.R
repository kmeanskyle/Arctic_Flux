# Script Summary
#   Compile dataset from flux data collected at Fyodorovskoye 1 & 2
#   (PI: A. Varlagin)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/RUFA.csv
#   ../data/Arctic_Flux/aggregate/RUFB.csv

#-- Setup ---------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # add missing variables with names given
  miss <- function(vars) {
    rep(NA, dim(DT)[1])
  }
  
  new_data <- lapply(var_key, miss)
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

# fread for multiple filepaths
my_fread <- function(fps) {
  rbindlist(lapply(fps, function(fp) {
    fread(fp)
  }))
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

fa_vars <- c(
  "TIMESTAMP_START",
  "LE_F_MDS", "LE_F_MDS_QC",
  "H_F_MDS", "H_F_MDS_QC",
  "G_F_MDS", "G_F_MDS_QC",
  "SW_IN_F_MDS", "SW_IN_F_MDS_QC", 
  "LW_IN_F_MDS", "LW_IN_F_MDS_QC",
  "SW_OUT",
  "LW_OUT",
  "NETRAD",
  "TA_F_MDS", "TA_F_MDS_QC",
  "RH",
  "WS_F", "WS_F_QC",
  "WD",
  "P_F", "P_F_QC",
  "TS_F_MDS_1", "TS_F_MDS_1_QC",
  "SWC_F_MDS_1", "SWC_F_MDS_1_QC"
)
# Timestamp start
# Latent heat flux (filtered/gapfilled plus qc flag)
# Sensible heat flux (filtered/gapfilled plus qc flag)
# Ground heat flux (filtered/gapfilled plus qc flag)
# Incoming shortwave radiation (filtered/gapfilled plus qc flag)
# Incoming longwave radiation (filtered/gapfilled plus qc flag)
# Outgoing shortwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature (filtered/gapfilled plus qc flag)
# Relative humidity
# Wind speed (filtered plus qc flag)
# Wind direction
# Precip (filtered plus qc flag)
# Soil temperature (filtered/gapfilled plus qc flag)
# Soil water content (filtered/gapfilled plus qc flag)

fa_fp <- "../raw_data/FluxNet/FLX_RU-Fyo_FLUXNET2015_FULLSET_1998-2014_2-3/FLX_RU-Fyo_FLUXNET2015_FULLSET_HH_1998-2014_2-3.csv"

fb_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_RUFy2_2015_v02_30m.txt"
fb_fps <- sapply(
  c("2016_v03", "2017_v04", "2018_v04"), 
  function(yr) gsub("2015_v02", yr, fb_fp), 
  USE.NAMES = FALSE
)

fa <- fread(fa_fp, select = fa_vars)
fb <- my_fread(fb_fp)

# convert times
fa <- convert_ts(fa)
fb <- convert_ts(fb)

# site ids
fa[, stid := "RUFA"]
fb[, stid := "RUFB"]

# add missing vars as NA
fa[, snowd := NA]
fb[, snowd := NA]

fa_names <- list(
  c("LE_F_MDS", "le"),
  c("LE_F_MDS_QC", "le_qc"),
  c("H_F_MDS", "h"),
  c("H_F_MDS_QC", "h_qc"),
  c("G_F_MDS", "g"), 
  c("G_F_MDS_QC", "g_qc"), 
  c("SW_IN_F_MDS", "sw_in"),
  c("SW_IN_F_MDS_QC", "sw_in_qc"),
  c("LW_IN_F_MDS", "lw_in"),
  c("LW_IN_F_MDS_QC", "lw_in_qc"),
  c("SW_OUT", "sw_out"),
  c("LW_OUT", "lw_out"),
  c("NETRAD", "rnet"),
  c("TA_F_MDS", "ta"),
  c("TA_F_MDS_QC", "ta_qc"),
  c("RH", "rh"),
  c("WS_F", "ws"),
  c("WS_F_QC", "ws_qc"),
  c("WD", "wd"),
  c("P_F", "precip"),
  c("P_F_QC", "precip_qc"),
  c("TS_F_MDS_1", "tsoil"),
  c("TS_F_MDS_1_QC", "tsoil_qc"),
  c("SWC_F_MDS_1", "swc"),
  c("SWC_F_MDS_1_QC", "swc_qc")
)

fb_names <- lapply(fa_names, function(vars) {
  new <- gsub("_F_MDS", "", vars)
  new <- gsub("_F", "", new)
  gsub("_1", "", new)
})
fb_names <- fb_names[-grep("_QC", fb_names)]

fa <- fix_names(fa, fa_names)
fb <- fix_names(fb, fb_names)

# add NA qa flags and select cols
fa <- sel_cols(add_qc(fa))
fb <- sel_cols(add_qc(fb))

fwrite(fa, "../data/Arctic_Flux/aggregate/RUFA.csv")
fwrite(fb, "../data/Arctic_Flux/aggregate/RUFB.csv")

#------------------------------------------------------------------------------
