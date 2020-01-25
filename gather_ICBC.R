# Script Summary
#   compile flux data sets from various downloaded sources for Imnavait Creek 
#   and Bonanza Creek
#
# Output files:
#   /data/aggregate/ICFE.csv
#   /data/aggregate/ICRI.csv
#   /data/aggregate/ICTU.csv
#   /data/aggregate/BCFE.csv
#   /data/aggregate/BCTH.csv
#   /data/aggregate/BCOB.csv
#   /data/aggregate/BCBS.csv

#-- Setup --------------------------------------------------------------------

# fread for files with units line under header (line 2 here)
# causes unnecessary coercion
my_fread <- function(path, vars) {
  name_vec <- unlist(unname(fread(path, nrows = 1, header = FALSE)))
  DT <- fread(path, skip = 2)
  names(DT) <- name_vec
  if(is.null(vars)) {
    vars <- name_vec
  } else {
    # make sure all requested vars present
    miss_vars <- setdiff(vars, name_vec)
    if(length(miss_vars) > 0) DT[, (miss_vars) := NA]
  }
  # convert logical columns (seem to result from all-NA columns) to numeric
  change_cols <- names(DT)[sapply(DT, is.logical)]
  if(length(change_cols) > 0) {
    DT[,(change_cols) := lapply(.SD, as.numeric), .SDcols = change_cols]
  }
  return(DT[, ..vars])
}

# aggregate appropriate vars
# Pass in ground heat fluxes to summarise because not all used for each site
#   (bad data for at least one)
aggr_vars <- function(DT, g_s){
  # aggregate vars by setting -9999 to NA (and revert back)
  #   first set NAs to -9998 (need to preserve NAs bc in one of datasets, 
  #   some vars were not measured (e.g. Rlong in 2018))
  # This syntax more efficient than DT[is.na(DT)] <- -9999 (negligible 
  #   difference on these data)
  for(col in names(DT)) set(DT, i = which(is.na(DT[[col]])), 
                            j = col, value = -9998)
  for(col in names(DT)) set(DT, i = which(DT[[col]] == -9999), 
                            j = col, value = NA)
  # ground fluxes
  DT <- DT[, g := rowMeans(.SD, na.rm = TRUE), 
           .SDcols = g_s]
  # soil temps
  DT <- DT[, tsoil := rowMeans(.SD, na.rm = TRUE),
           .SDcols = c("Tsoil_gf", "Tsoil_2")]
  # soil water content
  DT <- DT[, swc := rowMeans(.SD, na.rm = TRUE),
           .SDcols = c("SWC_1_f", "SWC_2_f")]
  # set NAs back to -9999 and -9998 back to NA
  for(col in names(DT)) set(DT, i = which(is.na(DT[[col]])), 
                            j = col, value = -9999)
  for(col in names(DT)) set(DT, i = which(DT[[col]] == -9998), 
                            j = col, value = NA)
  # # remove summarised variables
  # DT <- DT[, !c("G1_f", "G2_f", "G3_f", "G4_f", 
  #               "Tsoil_gf", "Tsoil_2",
  #               "SWC_1_f", "SWC_2_f")]
  DT
}

# make stid col
mk_stid <- function(DT, stid) {
  DT[, stid := stid]
  DT
}

# apply to list of paths and save
wrap_funs <- function(paths, args) {
  lapply(paths, my_fread, ic_vars) %>%
    rbindlist %>%
    mk_stid(args$stid) %>%
    aggr_vars(args$g_s) %>%
    fix_names(args$names_lst) %>%
    add_qc %>%
    sel_cols
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
library(magrittr)
library(data.table)

# downloaded data stored in external drive
raw_dir <- "../raw_data/ICBC"
# "aggregate" data dir for saving output
ag_dir <- "../data/Arctic_Flux/aggregate"

# import helper functions
source("helpers.R")

# Variable Set 1
# Imnavit Creek
ic_vars <- c(
  "Year",
  "DoY", 
  "Hour",
  "LE_cw_gf",
  "qc_LE_cw_gf",
  "H_c_gf",
  "qc_H_c_gf",
  "Rg_gf",
  "qc_Rg_gf",
  "Rg_out_f",
  "Rlong_f",
  "Rlong_out_f",
  "RNET_f",
  "G1_f",
  "G2_f", 
  "G3_f",
  "G4_f",
  "Ta_gf",
  "qc_Ta_gf",
  "RH_gf",
  "qc_RH_gf",
  "WS_1",
  "WD",
  "PRECIP",
  "SnowD_f",
  "Tsoil_gf",
  "qc_Tsoil_gf",
  "Tsoil_2",
  "SWC_1_f", 
  "SWC_2_f"
)

# Year
# Day of Year
# Hour
# Latent heat flux and qc flag
# Sensible heat flux and qc flag
# Incoming shortwave radiation and qc flag
# Outgoing shortwave radiation filtered
# Incoming longwave radiation filtered
# Outgoing longwave radiation filtered
# Net radiation filtered
# Ground heat fluxes
# Ambient air temperature
# Air temperature qc flag
# Relative humidity
# Relative humidity qc flag
# Wind speed
# Wind direction
# Precipitation
# Snow depth filtered
# Soil temperature
# Soil temperature qc flag
# Soil temperature 2
# Soil water content filtered

# fix_names list
names_lst <- list(
  c("Year", "year"),
  c("DoY", "doy"),
  c("Hour", "hour"),
  c("LE_cw_gf", "le"),
  c("qc_LE_cw_gf", "le_qc"),
  c("H_c_gf", "h"),
  c("qc_H_c_gf", "h_qc"),
  c("Rg_gf", "sw_in"),
  c("qc_Rg_gf", "sw_in_qc"),
  c("Rg_out_f", "sw_out"),
  c("Rlong_f", "lw_in"),
  c("Rlong_out_f", "lw_out"),
  c("RNET_f", "rnet"),
  c("Ta_gf", "ta"),
  c("qc_Ta_gf", "ta_qc"),
  c("RH_gf", "rh"),
  c("qc_RH_gf", "rh_qc"),
  c("WS_1", "ws"),
  c("WD", "wd"),
  c("PRECIP", "precip"),
  c("SnowD_f", "snowd")
)

# ICFE, (1523, wet sedge fen)
fns <- c(
  "IC_1523_gapfilled_2008_2013.csv",
  "2014_IC_1523_gapfilled_20141231.csv",
  "2015_IC_1523_gapfilled_20151231.csv",
  "2016_IC_1523_gapfilled_20161231.csv",
  "2017_IC_1523_gapfilled_20171231.csv",
  "2018_IC_1523_gapfilled_20181231.csv"
)
icfe_paths <- file.path(raw_dir, fns)
# aggregate data and save
g_s <- c("G1_f", "G2_f", "G3_f", "G4_f")
icfe_args <- list(stid = "icfe", g_s = g_s, names_lst = names_lst)
icfe <- wrap_funs(icfe_paths, icfe_args)
fwrite(icfe, file.path(ag_dir, "ICFE.csv"))

# ICRI (1991, ridge)
icri_paths <- file.path(raw_dir, gsub("1523", "1991", fns))
icri_args <- list(stid = "icri", g_s = g_s, names_lst = names_lst)
icri <- wrap_funs(icri_paths, icri_args)
fwrite(icri, file.path(ag_dir, "ICRI.csv"))

# ICTU (1993, tussock)
ictu_paths <- file.path(raw_dir, gsub("1523", "1993", fns))
ictu_args <- list(stid = "ictu", g_s = g_s, names_lst = names_lst)
ictu <- wrap_funs(ictu_paths, ictu_args)
fwrite(ictu, file.path(ag_dir, "ICTU.csv"))

# BCFE (Fen) 
fns <- c(
  "2013_2016_BC_FEN_gapfilled_DT.csv",
  "2017_BC_FEN_gapfilled_20171231.csv",
  "2018_BC_FEN_gapfilled_20181231.csv",
  "2019_BC_FEN_gapfilled_20190906.csv"
)
bcfe_paths <- file.path(raw_dir, fns)
bcfe_args <- list(stid = "bcfe", g_s = g_s, names_lst = names_lst)
bcfe <- wrap_funs(bcfe_paths, bcfe_args)
fwrite(bcfe, file.path(ag_dir, "BCFE.csv"))

# BCTH (Thermokarst)
bcth_paths <- file.path(raw_dir, gsub("FEN", "5166", fns))
bcth_args <- list(stid = "bcth", g_s = g_s[3:4], names_lst = names_lst)
bcth <- wrap_funs(bcth_paths, bcth_args)
fwrite(bcth, file.path(ag_dir, "BCTH.csv"))

# BCBS (Black Spruce)
fns <- gsub("BC_5166", "YF_2472", fns)
# need to include these data, but will take some work (different names, etc.)
# fns <- c(
#   "2010_AON_YF_2472.csv",
#   "2011_AON_YF_2472.csv",
#   "2012_AON_YF_2472.csv",
#   fns
# )
bcbs_paths <- file.path(raw_dir, )
bcbs_args <- list(stid = "bcbs", g_s = g_s, names_lst = names_lst)
bcbs <- wrap_funs(bcbs_paths, bcbs_args)
fwrite(bcbs, file.path(ag_dir, "BCBS.csv"))

# BCOB (Old Bog)
fns <- c(
  "2018_BC_OldBog_gapfilled_20181231.csv",
  "2019_BC_OldBog_gapfilled_20190906.csv"
)
bcob_paths <- file.path(raw_dir, fns)
bcob_args <- list(stid = "bcob", g_s = g_s, names_lst = names_lst)
bcob <- wrap_funs(bcob_paths, bcob_args)
fwrite(bcob, file.path(ag_dir, "BCOB.csv"))

#------------------------------------------------------------------------------
