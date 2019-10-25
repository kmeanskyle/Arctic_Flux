
# fread for files with units line under header (line 2 here)
# causes unnecessary coercion
my_fread <- function(path, vars = NULL) {
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
  return(DT[, ..vars])
}

# aggregate appropriate vars
aggr_vars <- function(DT, stid, sel_cols){
  # aggregate vars by setting -9999 to NA (and revert back)
  DT[DT == -9999] <- NA
  # ground fluxes
  DT <- DT[, g := rowMeans(.SD, na.rm = TRUE), 
           .SDcols = c("G1_f", "G2_f", "G3_f", "G4_f")]
  # soil temps
  DT <- DT[, tsoil := rowMeans(.SD, na.rm = TRUE),
           .SDcols = c("Tsoil_gf", "Tsoil_2")]
  # soil water content
  DT <- DT[, swc := rowMeans(.SD, na.rm = TRUE),
           .SDcols = c("SWC_1_f", "SWC_2_f")]
  # set NAs back to -9999
  DT[is.na(DT)] <- -9999
  # remove summarised variables
  DT <- DT[, !c("G1_f", "G2_f", "G3_f", "G4_f", 
                "Tsoil_gf", "Tsoil_2",
                "SWC_1_f", "SWC_2_f")]
  return(DT)
}

# fix variables names and save
fix_names <- function(DT, stid) {
  DT <- DT[, ..sel_cols]
}

# apply to list of paths and save
wrap_funs <- function(paths, stid) {
  lapply(icfe_paths, my_fread, ic_vars) %>%
    rbindlist %>%
    add_qc %>%
    fix_names(stid) %>%
    fwrite()
}

DT <- add_column(DT, stid = stid, 
                 .before = "year")
DT <- add_column(DT, lw_in_qc = NA, .after = "lw_in")
DT <- add_column(DT, ws_qc = NA, .after = "ws")
DT <- add_column(DT, precip_qc = NA, .after = "precip")
DT <- add_column(DT, g_qc = NA, .after = "g")
DT <- add_column(DT, swc_qc = NA, .after = "swc")
DT <- add_column(DT, rnet_qc = NA, .after = "rnet")

# re-order some vars

DT <- DT[, ..sel_cols]


# rename variables
new_names <- c("year", "doy", "hour",
               "le", "le_qc", "h", "h_qc",
               "sw_in", "sw_in_qc",
               "sw_out", "lw_in", "lw_out", "rnet",
               "ta", "ta_qc", "rh", "rh_qc", "ws", "wd", 
               "precip", "snowd", 
               "tsoil_qc", 
               "g", 
               "tsoil", 
               "swc")
names(DT) <- new_names


library(data.table)
library(lubridate)

workdir <- getwd()
datadir <- file.path(workdir, "data")
ag_dir <- file.path(datadir, "aggregate")
# downloaded data stored in external drive
source_data_dir <- "F:/Arctic_Flux/raw_data"

# select aggregate column names in order
sel_cols <- c("stid", "year", "doy", "hour",  "le", "le_qc", "h", "h_qc", "g", 
              "g_qc", "sw_in", "sw_in_qc", "sw_out", "lw_in", "lw_in_qc", 
              "lw_out", "rnet", "rnet_qc", "ta", "ta_qc", "rh", "rh_qc", "ws", 
              "ws_qc", "wd", "precip", "precip_qc", "snowd", "tsoil", 
              "tsoil_qc", "swc", "swc_qc")

# import helper functions
source(file.path(workdir, "helpers.R"))



library(tibble)
library(lubridate)

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


# 1523 (wet sedge fen)
fns <- c("IC_1523_gapfilled_2008_2013.csv",
         "2014_IC_1523_gapfilled_20141231.csv",
         "2015_IC_1523_gapfilled_20151231.csv",
         "2016_IC_1523_gapfilled_20161231.csv",
         "2017_IC_1523_gapfilled_20171231.csv",
         "2018_IC_1523_gapfilled_20181231.csv")
icfe_paths <- file.path(source_data_dir, "Eugenie_data", fns)

icfe <- rbindlist(lapply(icfe_paths, my_fread, ic_vars))
icfe <- aggr_vars(icfe)

