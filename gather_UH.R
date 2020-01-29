# Script Summary
#   Compile datasets from flux data collected by University of Helsinki
#   (PIs: T. Vesala, P. Kolari)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/FIHY.csv
#   ../data/Arctic_Flux/aggregate/FIVR.csv

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

hy_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_FIHyy_1996_v05_30m.txt"
hy_fps <- sapply(
  c("1997_v09", "1998_v08", "1999_v07", "2000_v06", "2001_v05", "2002_v05", 
    "2003_v05", "2004_v04", "2005_v07", "2006_v08", "2007_v08", "2008_v06",
    "2009_v010", "2010_v014", "2011_v013", "2012_v014", "2013_v014", 
    "2014_v09", "2015_v04"), 
  function(yr) gsub("1996_v05", yr, hy_fp), 
  USE.NAMES = FALSE
)

vr_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_FIVar_2016_v01_30m.txt"
vr_fps <- sapply(
  c("2017_v02", "2018_v02"), 
  function(yr) gsub("1996_v05", yr, vr_fp), 
  USE.NAMES = FALSE
)

hy <- my_fread(hy_fps)
vr <- my_fread(vr_fps)

hy <- convert_ts(hy)
vr <- convert_ts(vr)

hy[, stid := "FIHY"]
vr[, stid := "FIVR"]

hy[, snowd := NA]
vr_miss <- c("lw_in", "sw_out", "lw_out", "ta", "rh", "tsoil", "swc")
vr <- mk_vars(vr, vr_miss, "miss")

# fix names
hy_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("G", "g"),
  c("SW_IN", "sw_in"),
  c("LW_IN", "lw_in"),
  c("SW_OUT", "sw_out"),
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
vr_names <- hy_names[-c(5:7, 9, 10, 14, 15)]
vr_names[[9]] <- c("D_SNOW", "snowd")

hy <- fix_names(hy, hy_names)
vr <- fix_names(vr, vr_names)

# add missing qc and select columns
hy <- sel_cols(add_qc(hy))
vr <- sel_cols(add_qc(vr))

fwrite(hy, "../data/Arctic_Flux/aggregate/FIHY.csv")
fwrite(vr, "../data/Arctic_Flux/aggregate/FIVR.csv")

#------------------------------------------------------------------------------
