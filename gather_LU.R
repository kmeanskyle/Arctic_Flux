# Script Summary
#   Compile datasets from flux data collected by researchers at Lund 
#   University (PIs: A. Lindroth)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/SEFL.csv
#   ../data/Arctic_Flux/aggregate/SESK.csv

#-- Setup ---------------------------------------------------------------------
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
  }),
  fill = TRUE)
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

fl_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_SEFla_1996_v01_30m.txt"
fl_fps <- sapply(
  c("1997_v01", "1998_v01", "2000_v01", "2001_v01", "2002_v01"), 
  function(yr) gsub("1996_v01", yr, fl_fp), 
  USE.NAMES = FALSE
)

sk_fp <- gsub("SEFla", "SESk2", fl_fp)
sk_fps <- sapply(
  c("2004_v02", "2005_v01", "2006_v01"), 
  function(yr) gsub("1996_v01", yr, sk_fp), 
  USE.NAMES = FALSE
)

fl <- my_fread(fl_fps)
sk <- my_fread(sk_fps)

fl <- convert_ts(fl)
sk <- convert_ts(sk)

fl[, stid := "SEFL"]
sk[, stid := "SESK"]

fl[, snowd := NA]
sk[, snowd := NA]

fl_miss <- c("g", "snowd", "lw_in", "lw_out")
fl <- mk_vars(fl, fl_miss, "miss")
sk_miss <- fl_miss[-(3:4)]
sk <- mk_vars(sk, sk_miss, "miss")

# fix names
fl_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("P", "precip"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("TS", "tsoil"),
  c("SWC", "swc")
)
sk_names <- fl_names
sk_names[[13]] <- c("LW_IN", "lw_in")
sk_names[[14]] <- c("LW_OUT", "lw_out")

fl <- fix_names(fl, fl_names)
sk <- fix_names(sk, sk_names)

# add missing qc and select columns
fl <- sel_cols(add_qc(fl))
sk <- sel_cols(add_qc(sk))

fwrite(fl, "../data/Arctic_Flux/aggregate/SEFL.csv")
fwrite(sk, "../data/Arctic_Flux/aggregate/SESK.csv")

#------------------------------------------------------------------------------
