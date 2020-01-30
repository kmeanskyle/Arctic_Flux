# Script Summary
#   compile flux data sets from various downloaded sources for Imnavait Creek,
#   Bonanza Creek, and Pleistocene Park, Cherskiy
#
# Output files:
#   /data/aggregate/ICFE.csv
#   /data/aggregate/ICRI.csv
#   /data/aggregate/ICTU.csv
#   /data/aggregate/BCFE.csv
#   /data/aggregate/BCTH.csv
#   /data/aggregate/BCOB.csv
#   /data/aggregate/BCBS.csv

#-- Setup ---------------------------------------------------------------------
# fread for files with units line under header (line 2 here)
# causes unnecessary coercion
my_fread <- function(path, vars) {
  # erroneous extra header, units lines in 2018_YK_2472_gapfilled_20181231.csv
  if(grepl("2018_YF", path)) skip <- 4 else skip <- 2
  name_vec <- unlist(unname(fread(path, nrows = 1, header = FALSE)))
  keep <- which(name_vec %in% vars)
  DT <- fread(path, skip = skip, select = keep)
  names(DT) <- name_vec[keep]
  if(is.null(vars)) {
    vars <- name_vec
  } else {
    # make sure all requested vars present
    miss_vars <- setdiff(vars, name_vec)
    if(length(miss_vars) > 0) DT[, (miss_vars) := as.numeric(NA)]
  }
  DT
}

# aggregate appropriate vars
# Pass in ground heat fluxes to summarise because not all used for each site
#   (bad data for at least one)
aggr_vars <- function(DT, g_s){
  # aggregate, keeping memory of columns with -9999 indices
  aggr <- function(DT, vars) {
    temp_dt <- as.data.table(lapply(vars, function(var) DT[[var]]))
    miss_lst <- lapply(temp_dt, function(col) which(col == -9999))
    for(j in seq_along(temp_dt)){
      set(temp_dt, i=miss_lst[[j]], j=j, value=NA)
    }
    temp_dt[, aggr := rowMeans(.SD, na.rm = TRUE)]
    new_miss <- Reduce(union, miss_lst)
    new_miss <- intersect(new_miss, which(is.na(temp_dt[["aggr"]])))
    temp_dt[new_miss, aggr := -9999]
    temp_dt[["aggr"]]
  }
  # ground fluxes
  DT[, g := aggr(DT, g_s)]
  # soil temps
  DT[, tsoil := aggr(DT, c("Tsoil_gf", "Tsoil_2"))]
  # soil water content
  DT[, swc := aggr(DT, c("SWC_1_f", "SWC_2_f"))]
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
    rbindlist(use.names = TRUE) %>%
    mk_stid(args$stid) %>%
    aggr_vars(args$g_s) %>%
    fix_names(args$names_lst) %>%
    add_qc %>%
    sel_cols
}

# Fread AON
# files with "AON" in the filenames seem to have a different strucutre, this
#   function is for reading those files
fread_aon <- function(aon_fps, vars) {
  rbindlist(lapply(aon_fps, function(fp) {
    header <- unlist(fread(fp, nrows = 1, skip = 6, header = FALSE))
    keep <- which(header %in% vars)
    DT <- fread(fp, skip = 8, select = keep)
    names(DT) <- header[keep]
    for (j in names(DT))
      set(DT, which(is.na(DT[[j]])), j, NA)
    DT
  }))
}

# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    temp_dt[temp_dt == -9999] <- NA
    temp_dt <- temp_dt[, (vars[1]) := rowMeans(.SD, na.rm = TRUE)]
    temp_dt[which(is.na(temp_dt[[vars[1]]])), (vars[1]) := -9999]
    temp_dt[[vars[1]]]
  }
  # Copy non-gapfilled data over missing (NA) values in gapfilled time series
  #   var_key is a list of pairs of column names where the first position is the
  #   name of the column to be copied and the second position is the target
  mesh <- function(vars) {
    target <- DT[[vars[3]]]
    rpl <- is.na(target)
    target[rpl] <- DT[[vars[2]]][rpl]
    target
  }
  # add qc flags for gapfilled measurements where qc flag not included
  #   and where raw data available. 0 for "close" to observed value, -9999 
  #   otherwise (unknown gapfill quality)
  fill_qc <- function(vars) {
    rpl <- is.na(DT[[vars[2]]]) != TRUE & is.na(DT[[vars[3]]]) != TRUE & 
      abs(DT[[vars[2]]] - DT[[vars[3]]]) < 5
    new_vec <- rep(NA, dim(DT)[1])
    new_vec[rpl] <- 0
    new_vec
  }
  # qc flags are either 0 or NA. Assign 0 if both are 0, else NA
  aggr_qc <- function(vars) {
    rpl <- DT[[vars[2]]] == 0 & DT[[vars[3]]] == 0
    new_vec <- rep(NA, dim(DT)[1])
    new_vec[rpl] <- 0
    new_vec
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     mesh = mesh,
                                     fill_qc = fill_qc,
                                     aggr_qc = aggr_qc))
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

# convert timestamp that is a string (as opposed to integer)
convert_str_ts <- function(DT){
  convert_hour <- function(x){
    hr <- as.numeric(substr(x, 12, 13))
    m <- as.numeric(substr(x, 15, 16))
    hr + m/60
  }
  
  DT <- DT[, c("year", "doy", "hour") := 
             .(as.numeric(substr(TIMESTAMP, 1, 4)),
               lubridate::yday(ymd(substr(TIMESTAMP, 1, 10))),
               convert_hour(TIMESTAMP))]
  DT
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
icfe_args <- list(stid = "ICFE", g_s = g_s, names_lst = names_lst)
icfe <- wrap_funs(icfe_paths, icfe_args)
fwrite(icfe, file.path(ag_dir, "ICFE.csv"))

# ICRI (1991, ridge)
icri_paths <- file.path(raw_dir, gsub("1523", "1991", fns))
icri_args <- list(stid = "ICRI", g_s = g_s, names_lst = names_lst)
icri <- wrap_funs(icri_paths, icri_args)
fwrite(icri, file.path(ag_dir, "ICRI.csv"))

# ICTU (1993, tussock)
ictu_paths <- file.path(raw_dir, gsub("1523", "1993", fns))
ictu_args <- list(stid = "ICTU", g_s = g_s, names_lst = names_lst)
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
bcfe_args <- list(stid = "BCFE", g_s = g_s, names_lst = names_lst)
bcfe <- wrap_funs(bcfe_paths, bcfe_args)
fwrite(bcfe, file.path(ag_dir, "BCFE.csv"))

# BCTH (Thermokarst)
bcth_paths <- file.path(raw_dir, gsub("FEN", "5166", fns))
bcth_args <- list(stid = "BCTH", g_s = g_s[3:4], names_lst = names_lst)
bcth <- wrap_funs(bcth_paths, bcth_args)
fwrite(bcth, file.path(ag_dir, "BCTH.csv"))

# BCBS (Black Spruce)
# broken up into two different data formats
bs_aon_vars <- c(
  "TIMESTAMP",
  "LE", "LE_cw_gf",
  "H", "H_c_gf",
  "G1", "G1_gf",
  "G2", "G2_gf", 
  "G3", "G3_gf", 
  "G4", "G4_gf",
  "Rg", "Rg_gf",
  "Rg_out", "Rg_out_gf",
  "Rl", "Rl_gf",
  "Rl_out", "Rl_out_gf",
  "RNET", "RNET_gf",
  "Ta_1", "Ta_1_gf",
  "Rh", "Rh_gf",
  "WS", "WS_gf",
  "WD_gf",
  "PRECIP", "PRECIP_f",
  "SNOWD", "SNOWD_gf",
  "TSOIL_1", "TSOIL_1_gf",
  "TSOIL_2", "TSOIL_2_gf",
  "SWC_1", "SWC_1_gf",
  "SWC_2", "SWC_2_gf"
)
pfx1 <- 2010:2012
sfx1 <- "_AON_YF_2472.csv"
bs1_fps <- paste0("../raw_data/ICBC/", pfx1, sfx1)

# read files
bs1 <- fread_aon(bs1_fps, bs_aon_vars)
# convert ts string
bs1 <- convert_str_ts(bs1)
bs1[, stid := "BCBS"]

# assign qc flags for gapfilled vars not included
fill_qc_lst <- list(
  c("le_qc", "LE", "LE_cw_gf"),
  c("h_qc", "H", "H_c_gf"),
  c("g1_qc", "G1", "G1_gf"),
  c("g2_qc", "G2", "G2_gf"),
  c("g3_qc", "G3", "G3_gf"),
  c("g4_qc", "G4", "G4_gf"),
  c("sw_in_qc", "Rg", "Rg_gf"),
  c("sw_out_qc", "Rg_out", "Rg_out_gf"),
  c("lw_in_qc", "Rl", "Rl_gf"),
  c("lw_out_qc", "Rl_out", "Rl_out_gf"),
  c("rnet_qc", "RNET", "RNET_gf"),
  c("ta_qc", "Tc_1", "Ta_1_gf"),
  c("rh_qc", "Rh", "Rh_gf"),
  c("ws_qc", "WS", "WS_gf"),
  c("precip_qc", "PRECIP", "PRECIP_f"),
  c("snowd_qc", "SNOWD", "SNOWD_gf"),
  c("swc1_qc", "SWC_1", "SWC_1_gf"),
  c("swc2_qc", "SWC_2", "SWC_2_gf"),
  c("tsoil1_qc", "TSOIL_1", "TSOIL_1_gf"),
  c("tsoil2_qc", "TSOIL_2", "TSOIL_2_gf")
)
bs1 <- mk_vars(bs1, fill_qc_lst, "fill_qc")

# aggregate qc flags for aggregate variables
aggr_qc_lst <- list(
  c("g_qc", "g1_qc", "g2_qc", "g3_qc", "g4_qc"),
  c("swc_qc", "swc1_qc", "swc2_qc"),
  c("tsoil_qc", "tsoil1_qc", "tsoil2_qc")
)
bs1 <- mk_vars(bs1, aggr_qc_lst, "aggr_qc")

# aggregate variables by mean
aggr_lst <- list(
  c("g", "G1_gf", "G2_gf", "G3_gf", "G4_gf"),
  c("swc", "SWC_1_gf", "SWC_2_gf"),
  c("tsoil", "TSOIL_1_gf", "TSOIL_2_gf")
)
bs1 <- mk_vars(bs1, aggr_lst, "aggr")

# fix names
bs1_names <- list(
  c("LE_cw_gf", "le"),
  c("H_c_gf", "h"),
  c("Rg_gf", "sw_in"),
  c("Rl_gf", "lw_in"),
  c("Rg_out_gf", "sw_out"),
  c("Rl_out_gf", "lw_out"),
  c("RNET_gf", "rnet"),
  c("Ta_1_gf", "ta"),
  c("Rh_gf", "rh"),
  c("PRECIP_f", "precip"),
  c("SNOWD_gf", "snowd"),
  c("WS_gf", "ws"),
  c("WD_gf", "wd")
)
bs1 <- fix_names(bs1, bs1_names)

bs1 <- sel_cols(bs1)

bs2_fns <- gsub("BC_FEN", "YF_2472", fns)
bs2_fps <- file.path(raw_dir, bs2_fns)
bs_args <- list(stid = "BCBS", g_s = g_s, names_lst = names_lst)
bs2 <- wrap_funs(bs2_fps, bs_args) 

bs <- rbind(bs1, bs2)

fwrite(bs, file.path(ag_dir, "BCBS.csv"))

# BCOB (Old Bog)
fns <- c(
  "2018_BC_OldBog_gapfilled_20181231.csv",
  "2019_BC_OldBog_gapfilled_20190906.csv"
)
bcob_paths <- file.path(raw_dir, fns)
bcob_args <- list(stid = "BCOB", g_s = g_s, names_lst = names_lst)
bcob <- wrap_funs(bcob_paths, bcob_args)
fwrite(bcob, file.path(ag_dir, "BCOB.csv"))

# RUCH
# Pleistocene Park, Cherskiy
ch_aon_vars <- c(
  "TIMESTAMP",
  "LE", "LE_cw_gf",
  "H", "H_c_gf",
  "G1", "G1_gf",
  "G2", "G2_gf", 
  "G3", "G3_gf", 
  "G4", "G4_gf",
  "Rg", "Rg_gf",
  "Rg_out", "Rg_out_gf",
  "Rl", "Rl_gf",
  "Rl_out", "Rl_out_gf",
  "RNET", "RNET_gf",
  "Ta_1", "Ta_1_gf",
  "Rh", "Rh_gf",
  "WS", "WS_gf",
  "WD_gf",
  "PRECIP", "PRECIP_f",
  "SNOWD", "SNOWD_gf",
  "TSOIL_1", "TSOIL_1_gf",
  "TSOIL_2", "TSOIL_2_gf",
  "SWC_1", "SWC_1_gf",
  "SWC_2", "SWC_2_gf"
)
pfx1 <- 2008:2013
sfx1 <- "_AON_CH_2044.csv"
aon_fps <- paste0("../raw_data/AON/", pfx1, sfx1)

ch_fps <- paste0(
  "../raw_data/AON/",
  c(
    "2014_CH_2044_gapfilled_20141231.csv",
    "2015_CH_2044_gapfilled_20151120.csv",
    "2016_CH_2044_gapfilled_20160825.csv"
  )
)

# read files
ch1 <- fread_aon(aon_fps, ch_aon_vars)
# convert ts string
ch1 <- convert_str_ts(ch1)
ch1[, stid := "RUCH"]

# assign qc flags for gapfilled vars not included
fill_qc_lst <- list(
  c("le_qc", "LE", "LE_cw_gf"),
  c("h_qc", "H", "H_c_gf"),
  c("g1_qc", "G1", "G1_gf"),
  c("g2_qc", "G2", "G2_gf"),
  c("g3_qc", "G3", "G3_gf"),
  c("g4_qc", "G4", "G4_gf"),
  c("sw_in_qc", "Rg", "Rg_gf"),
  c("sw_out_qc", "Rg_out", "Rg_out_gf"),
  c("lw_in_qc", "Rl", "Rl_gf"),
  c("lw_out_qc", "Rl_out", "Rl_out_gf"),
  c("rnet_qc", "RNET", "RNET_gf"),
  c("ta_qc", "Tc_1", "Ta_1_gf"),
  c("rh_qc", "Rh", "Rh_gf"),
  c("ws_qc", "WS", "WS_gf"),
  c("precip_qc", "PRECIP", "PRECIP_f"),
  c("snowd_qc", "SNOWD", "SNOWD_gf"),
  c("swc1_qc", "SWC_1", "SWC_1_gf"),
  c("swc2_qc", "SWC_2", "SWC_2_gf"),
  c("tsoil1_qc", "TSOIL_1", "TSOIL_1_gf"),
  c("tsoil2_qc", "TSOIL_2", "TSOIL_2_gf")
)
ch1 <- mk_vars(ch1, fill_qc_lst, "fill_qc")

# aggregate qc flags for aggregate variables
aggr_qc_lst <- list(
  c("g_qc", "g1_qc", "g2_qc", "g3_qc", "g4_qc"),
  c("swc_qc", "swc1_qc", "swc2_qc"),
  c("tsoil_qc", "tsoil1_qc", "tsoil2_qc")
)
ch1 <- mk_vars(ch1, aggr_qc_lst, "aggr_qc")

# aggregate variables by mean
aggr_lst <- list(
  c("g", "G1_gf", "G2_gf", "G3_gf", "G4_gf"),
  c("swc", "SWC_1_gf", "SWC_2_gf"),
  c("tsoil", "TSOIL_1_gf", "TSOIL_2_gf")
)
ch1 <- mk_vars(ch1, aggr_lst, "aggr")

# fix names
ch1_names <- list(
  c("LE_cw_gf", "le"),
  c("H_c_gf", "h"),
  c("Rg_gf", "sw_in"),
  c("Rl_gf", "lw_in"),
  c("Rg_out_gf", "sw_out"),
  c("Rl_out_gf", "lw_out"),
  c("RNET_gf", "rnet"),
  c("Ta_1_gf", "ta"),
  c("Rh_gf", "rh"),
  c("PRECIP_f", "precip"),
  c("SNOWD_gf", "snowd"),
  c("WS_gf", "ws"),
  c("WD_gf", "wd")
)
ch1 <- fix_names(ch1, ch1_names)

ch1 <- sel_cols(ch1)

ch_args <- list(stid = "RUCH", g_s = g_s, names_lst = names_lst)
ch2 <- wrap_funs(ch_fps, ch_args)

ch <- rbind(ch1, ch2)

fwrite(ch, "../data/Arctic_Flux/aggregate/RUCH.csv")

#------------------------------------------------------------------------------
