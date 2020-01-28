# Script Summary
#   Compile datasets from flux data collected in Denmark by researchers at 
#   the Technical University of Denmark (PI: K. Pilegaard)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/DKSR.csv
#   ../data/Arctic_Flux/aggregate/DKEN.csv

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

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

so_vars <- c(
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

en_vars <- so_vars[-grep("OUT", so_vars)]

so_fp <- "../raw_data/FluxNet/FLX_DK-Sor_FLUXNET2015_FULLSET_1996-2014_2-3/FLX_DK-Sor_FLUXNET2015_FULLSET_HH_1996-2014_2-3.csv"
en_fp <- gsub("Sor", "Eng", so_fp)
en_fp <- gsub("1996-2014_2-3", "2005-2008_1-3", en_fp)

so <- fread(so_fp, select = so_vars)
en <- fread(en_fp, select = en_vars)

# convert times
so <- convert_ts(so)
en <- convert_ts(en)

# site ids
so[, stid := "DKSR"]
en[, stid := "DKEN"]

# add missing vars as NA
so_miss <- c("snowd")
so <- mk_vars(so, so_miss, "miss")
en_miss <- c(so_miss, "sw_out", "lw_out")
en <- mk_vars(en, en_miss, "miss")

so_names <- list(
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

en_names <- so_names[-(11:12)]

so <- fix_names(so, so_names)
en <- fix_names(en, en_names)

# add NA qa flags and select cols
so <- sel_cols(add_qc(so))
en <- sel_cols(add_qc(en))

fwrite(so, "../data/Arctic_Flux/aggregate/DKSR.csv")
fwrite(en, "../data/Arctic_Flux/aggregate/DKEN.csv")

#------------------------------------------------------------------------------
