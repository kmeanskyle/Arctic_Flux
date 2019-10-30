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
  changeCols <- names(DT)[sapply(DT, is.logical)]
  DT[,(changeCols) := lapply(.SD, as.numeric), .SDcols = changeCols]
  return(DT[, ..vars])
}

# aggregate appropriate vars
aggr_vars <- function(DT){
  # aggregate vars by setting -9999 to NA (and revert back)
  #   first set NAs to -9998 (need to preserve NAs bc in one of datasets, 
  #   some vars were not measured (e.g. Rlong in 2018))
  # This syntax more efficient than DT[is.na(DT)] <- -9999 (negligible difference
  #   on these data)
  for(col in names(DT)) set(DT, i = which(is.na(DT[[col]])), 
                            j = col, value = -9998)
  for(col in names(DT)) set(DT, i = which(DT[[col]] == -9999), 
                            j = col, value = NA)
  # ground fluxes
  DT <- DT[, g := rowMeans(.SD, na.rm = TRUE), 
           .SDcols = c("G1_f", "G2_f", "G3_f", "G4_f")]
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
  # remove summarised variables
  DT <- DT[, !c("G1_f", "G2_f", "G3_f", "G4_f", 
                "Tsoil_gf", "Tsoil_2",
                "SWC_1_f", "SWC_2_f")]
  return(DT)
}

# apply to list of paths and save
wrap_funs <- function(paths, stid) {
  library(magrittr)
  library(data.table)
  lapply(paths, my_fread, ic_vars) %>%
    rbindlist %>%
    aggr_vars %>%
    fix_names(stid) %>%
    add_qc %>%
    sel_cols
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------

# downloaded data stored in external drive
raw_dir <- "F:/Arctic_Flux/raw_data/ICBC"
# "aggregate" data dir for saving output
ag_dir <- "data/aggregate"

# import helper functions
source("helpers.R")

# Variable Set 1
# Imnavit Creek
ic_vars <- c("Year",
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
             "SWC_2_f")

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

# ICFE, (1523, wet sedge fen)
fns <- c("IC_1523_gapfilled_2008_2013.csv",
         "2014_IC_1523_gapfilled_20141231.csv",
         "2015_IC_1523_gapfilled_20151231.csv",
         "2016_IC_1523_gapfilled_20161231.csv",
         "2017_IC_1523_gapfilled_20171231.csv",
         "2018_IC_1523_gapfilled_20181231.csv")
icfe_paths <- file.path(raw_dir, fns)
# aggregate data and save
icfe <- wrap_funs(icfe_paths, "icfe")
fwrite(icfe, file.path(ag_dir, "ICFE.csv"))

# ICRI (1991, ridge)
icri_paths <- file.path(raw_dir, gsub("1523", "1991", fns))
# aggregate data and save
icri <- wrap_funs(icri_paths, "icri")
fwrite(icri, file.path(ag_dir, "ICRI.csv"))

# ICTU (1993, tussock)
ictu_paths <- file.path(raw_dir, gsub("1523", "1993", fns))
# aggregate data and save
ictu <- wrap_funs(ictu_paths, "ictu")
fwrite(ictu, file.path(ag_dir, "ICTU.csv"))

# BCFE (Fen) 
fns <- c("2013_2016_BC_FEN_gapfilled_DT.csv",
         "2017_BC_FEN_gapfilled_20171231.csv",
         "2018_BC_FEN_gapfilled_20181231.csv")
bcfe_paths <- file.path(raw_dir, fns)
bcfe <- wrap_funs(bcfe_paths, "bcfe")
fwrite(bcfe, file.path(ag_dir, "BCFE.csv"))

# BCTH (Thermokarst)
bcth_paths <- file.path(raw_dir, gsub("FEN", "5166", fns))
# aggregate data and save
bcth <- wrap_funs(bcth_paths, "bcth")
fwrite(bcth, file.path(ag_dir, "BCTH.csv"))

# BCOB (Old Bog)
fns <- c("2018_BC_OldBog_gapfilled_20181231.csv")
bcob_paths <- file.path(raw_dir, fns)
bcob <- wrap_funs(bcob_paths, "bcob")
fwrite(bcob, file.path(ag_dir, "BCOB.csv"))

#------------------------------------------------------------------------------
