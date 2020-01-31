# Script Summary
#   Compile datasets from remaining flux data accessed at EFDC managed by
#     University of Edinburgh (PI: J. Moncrieff)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/UKGR.csv
#   ../data/Arctic_Flux/aggregate/UKSE.csv

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

gr_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_UKGri_2000_v01_30m.txt"
gr_fps <- sapply(
  c("2000_v01", "2001_v01", "2004_v016", "2005_v021", "2006_v019", "2007_v015",
    "2008_v013"), 
  function(yr) gsub("2000_v01", yr, gr_fp), 
  USE.NAMES = FALSE
)

se_fp <- gsub("UKGri", "UKESa", gr_fp)
se_fps <- sapply(
  c("2003_v02", "2004_v02", "2005_v01", "2006_v01"), 
  function(yr) gsub("2000_v01", yr, se_fp), 
  USE.NAMES = FALSE
)

gr <- my_fread(gr_fps)
se <- my_fread(se_fps)

gr <- convert_ts(gr)
se <- convert_ts(se)

gr[, stid := "UKGR"]
se[, stid := "UKSE"]

gr[, snowd := NA]
se_miss <- c("g", "lw_in", "sw_out", "lw_out", "snowd")
se <- mk_vars(se, se_miss, "miss")

# fix names
gr_names <- list(
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
  c("P", "precip"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("TS", "tsoil"),
  c("SWC", "swc")
)
se_names <- gr_names[-c(3, 5:7)]

gr <- fix_names(gr, gr_names)
se <- fix_names(se, se_names)

# add missing qc and select columns
gr <- sel_cols(add_qc(gr))
se <- sel_cols(add_qc(se))

fwrite(gr, "../data/Arctic_Flux/aggregate/UKGR.csv")
fwrite(se, "../data/Arctic_Flux/aggregate/UKSE.csv")

#------------------------------------------------------------------------------
