# Script Summary
#   Compile datasets from flux data collected by researchers at 
#   the Finnish Meteorological Institute (PI: A. Lohila)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/FIJO.csv
#   ../data/Arctic_Flux/aggregate/FILE.csv
#   ../data/Arctic_Flux/aggregate/FISO.csv

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

jo_vars <- c(
  "TIMESTAMP_START",
  "LE_F_MDS", "LE_F_MDS_QC",
  "H_F_MDS", "H_F_MDS_QC",
  "G_F_MDS", "G_F_MDS_QC",
  "SW_IN_F_MDS", "SW_IN_F_MDS_QC", 
  "LW_IN_F_MDS", "LW_IN_F_MDS_QC",
  "SW_OUT",
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

le_vars <- jo_vars[-grep("SWC", jo_vars)]
so_vars <- jo_vars[-grep("OUT", jo_vars)]
tk_vars <- jo_vars[-grep("G", jo_vars)]

jo_fp <- "../raw_data/FluxNet/FLX_FI-Jok_FLUXNET2015_FULLSET_2000-2003_1-3/FLX_FI-Jok_FLUXNET2015_FULLSET_HH_2000-2003_1-3.csv"
le_fp <- gsub("Jok", "Let", jo_fp)
le_fp <- gsub("2000-2003", "2009-2012", le_fp)
so_fp <- gsub("Jok", "Sod", jo_fp)
so_fp <- gsub("2000-2003", "2001-2014", so_fp)
tk_fp <- gsub("FI-Jok", "RU-Tks", jo_fp)
tk_fp <- gsub("2000-2003", "2010-2014", tk_fp)

jo <- fread(jo_fp, select = jo_vars)
le <- fread(le_fp, select = le_vars)
so <- fread(so_fp, select = so_vars)
tk <- fread(tk_fp, select = tk_vars)

# convert times
jo <- convert_ts(jo)
le <- convert_ts(le)
so <- convert_ts(so)
tk <- convert_ts(tk)

# site ids
jo[, stid := "FIJO"]
le[, stid := "FILE"]
so[, stid := "FISO"]
tk[, stid := "RUTK"]

# add missing vars as NA
jo_miss <- c("snowd", "lw_out")
le_miss <- c(jo_miss, "swc")
so_miss <- c(jo_miss, "sw_out")
tk_miss <- c(jo_miss, "g")
jo <- mk_vars(jo, jo_miss, "miss")
le <- mk_vars(le, le_miss, "miss")
so <- mk_vars(so, so_miss, "miss")
tk <- mk_vars(tk, tk_miss, "miss")

jo_names <- list(
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

le_names <- jo_names[-(23:24)]
so_names <- jo_names[-(11)]
tk_names <- jo_names[-(5:6)]

jo <- fix_names(jo, jo_names)
le <- fix_names(le, le_names)
so <- fix_names(so, so_names)
tk <- fix_names(tk, tk_names)

# add NA qa flags and select cols
jo <- sel_cols(add_qc(jo))
le <- sel_cols(add_qc(le))
so <- sel_cols(add_qc(so))
tk <- sel_cols(add_qc(tk))

fwrite(jo, "../data/Arctic_Flux/aggregate/FIJO.csv")
fwrite(le, "../data/Arctic_Flux/aggregate/FILE.csv")
fwrite(so, "../data/Arctic_Flux/aggregate/FISO.csv")
fwrite(tk, "../data/Arctic_Flux/aggregate/RUTK.csv")

#------------------------------------------------------------------------------
