# Script Summary
#   Compile datasets from flux data collected by researchers at the Centre for 
#   Ecology and Hydrology (PIs: M. Sutton, E. Nemitz)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/UKEB.csv
#   ../data/Arctic_Flux/aggregate/UKAM.csv

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

eb_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_UKEBu_2004_v03_30m.txt"
eb_fps <- sapply(
  c("2004_v03", "2005_v02", "2006_v02", "2007_v02", "2008_v01"), 
  function(yr) gsub("2004_v03", yr, eb_fp), 
  USE.NAMES = FALSE
)

am_fp <- gsub("UKEBu", "UKAMo", eb_fp)
am_fps <- sapply(
  c("2002_v01", "2003_v02", "2004_v02", "2005_v01", "2006_v02"), 
  function(yr) gsub("2004_v03", yr, am_fp), 
  USE.NAMES = FALSE
)

eb <- my_fread(eb_fps)
am <- my_fread(am_fps)

eb <- convert_ts(eb)
am <- convert_ts(am)

eb[, stid := "UKEB"]
am[, stid := "UKAM"]

eb_miss <- c("g", "snowd")
am_miss <- c(eb_miss, "sw_out", "lw_in", "lw_out")
eb <- mk_vars(eb, eb_miss, "miss")
am <- mk_vars(am, am_miss, "miss")

# fix names
eb_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("LW_IN", "lw_in"),
  c("LW_OUT", "lw_out"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("P", "precip"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("TS", "tsoil"),
  c("SWC", "swc")
)
am_names <- eb_names[-c(4:6)]

eb <- fix_names(eb, eb_names)
am <- fix_names(am, am_names)

# add missing qc and select columns
eb <- sel_cols(add_qc(eb))
am <- sel_cols(add_qc(am))

fwrite(eb, "../data/Arctic_Flux/aggregate/UKEB.csv")
fwrite(am, "../data/Arctic_Flux/aggregate/UKAM.csv")

#------------------------------------------------------------------------------
