# Script Summary
#   compile complete database from various downloaded sources
#
# Output files:
#   /data/aggregate/ICFE.csv
#   /data/aggregate/ICRI.csv
#   /data/aggregate/ICTU.csv
#   /data/aggregate/BCFE.csv
#   /data/aggregate/BCTH.csv
#   /data/aggregate/PFRR.csv
#   /data/aggregate/OKPT.csv
#   /data/aggregate/BARW.csv
#   /data/aggregate/ANSB.csv
#   /data/aggregate/ANMB.csv
#   /data/aggregate/ANUB.csv
#   /data/aggregate/ATQA.csv
#   /data/aggregate/BRWB.csv



#-- Setup --------------------------------------------------------------------

library(data.table)
library(lubridate)

workdir <- getwd()
datadir <- file.path(workdir, "data")
ag_dir <- file.path(datadir, "aggregate")
# downloaded data stored in external drive
source_data_dir <- "F:/Arctic_Flux/raw_data"

# select column names in order
sel_cols <- c("stid", "year", "doy", "hour",  "le", "le_qc", "h", "h_qc", "g", 
              "g_qc", "sw_in", "sw_in_qc", "sw_out", "lw_in", "lw_in_qc", 
              "lw_out", "rnet", "rnet_qc", "ta", "ta_qc", "rh", "rh_qc", "ws", 
              "ws_qc", "wd", "precip", "precip_qc", "snowd", "tsoil", 
              "tsoil_qc", "swc", "swc_qc")

# import helper functions
source(file.path(workdir, "helpers.R"))

#------------------------------------------------------------------------------

#-- Imnavait Creek & Bonanza Creek (UAF) --------------------------------------
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

# fread for files with units line under header (line 2 here)
# causes unnecessary coercion
my_fread <- function(fn) {
  name_vec <- unlist(unname(fread(fn, nrows = 1, header = FALSE)))
  DT <- fread(fn, skip = 2)
  names(DT) <- name_vec
  return(DT)
}

# 1523 (wet sedge fen)
icfe_path <- file.path(source_data_dir, "Eugenie_data",
                       "IC_1523_gapfilled_2008_2013.csv")
icfe14_path <- file.path(source_data_dir, "Eugenie_data",
                         "2014_IC_1523_gapfilled_20141231.csv")
# This .csv is saved with a comma at end of each row, preventing use of
#   "select" argument of fread()
icfe <- my_fread(icfe_path)
icfe14 <- my_fread(icfe14_path)

names(icfe) <- unlist(icfe[1, ])
icfe <- icfe[-c(1, 2), ..ic_vars]

# 1991 (ridge)
icri_path <- file.path(source_data_dir, "Eugenie_data",
                       "IC_1991_gapfilled_2008_2013.csv")
icri <- fread(icri_path, select = ic_vars)[-1, ]

# 1993 (tussock)
ictu_path <- file.path(source_data_dir, "Eugenie_data",
                        "IC_1993_gapfilled_2008_2013.csv")
ictu <- fread(ictu_path, select = ic_vars)[-1, ]

# Bonanza Creek
bc_vars <- ic_vars
# Fen
bcfe_path <- file.path(source_data_dir, "Eugenie_data",
                       "2013_2016_BC_FEN_gapfilled_DT.csv")
bcfe <- fread(bcfe_path, select = bc_vars)[-1, ]

# 5166 (Thermokarst)
bcth_path <- file.path(source_data_dir, "Eugenie_data",
                        "2013_2016_BC_5166_gapfilled_DT.csv")
bcth <- fread(bcth_path, select = bc_vars)[-1, ]

# function to convert col classes, aggregate
var1_df <- function(DT, stid, sel_cols){
  DT <- DT[, lapply(.SD, as.numeric)]
  
  require(tibble)
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
  # remove summarised variables
  DT <- DT[, !c("G1_f", "G2_f", "G3_f", "G4_f", 
                "Tsoil_gf", "Tsoil_2",
                "SWC_1_f", "SWC_2_f")]
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
  # set NAs back to -9999
  DT[is.na(DT)] <- -9999
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
  
  return(DT)
}

# aggregate df's
icfe <- var1_df(icfe, "icfe", sel_cols)
icri <- var1_df(icri, "icri", sel_cols)
ictu <- var1_df(ictu, "ictu", sel_cols)
bcfe <- var1_df(bcfe, "bcfe", sel_cols)
bcth <- var1_df(bcth, "bcth", sel_cols)

# save these data
fwrite(icfe, file.path(ag_dir, "ICFE.csv"))
fwrite(icri, file.path(ag_dir, "ICRI.csv"))
fwrite(ictu, file.path(ag_dir, "ICTU.csv"))
fwrite(bcfe, file.path(ag_dir, "BCFE.csv"))
fwrite(bcth, file.path(ag_dir, "BCTH.csv"))

#------------------------------------------------------------------------------

#-- Poker Flat ----------------------------------------------------------------
# AmeriFlux Data
pf_vars <- c("TIMESTAMP_START",
             "LE_1_1_1",
             "LE_PI_F_1_1_1",
             "H_1_1_1",
             "H_PI_F_1_1_1",
             "G_1_1_1",
             "SW_IN",
             "SW_OUT",
             "LW_IN",
             "LW_OUT",
             "TA_1_1_1",
             "RH_1_7_1",
             "WS_1_8_1",
             "WD_1_1_1",
             "P_RAIN",
             "D_SNOW_1_1_1",
             "D_SNOW_1_1_2",
             "D_SNOW_1_1_3",
             "TS_1_1_1",
             "SWC_1_1_1")

# Timestamp start
# Latent heat flux filtered/gapfilled
# Sensible heat flux filtered/gapfilled
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Rainfall
# Snow depths
# Soil temperature
# Soil water content

pf_path <- file.path(source_data_dir, "AmeriFlux", "AMF_US-Prr_BASE-BADM_3-5",
                     "AMF_US-Prr_BASE_HH_3-5.csv")
pf <- fread(pf_path, select = pf_vars, skip = 2)

# FluxNet data
pffn_vars <- c("TIMESTAMP_START",
               "G_F_MDS",
               "G_F_MDS_QC",
               "SW_IN_F",
               "SW_IN_F_QC", 
               "LW_IN_F",
               "LW_IN_F_QC",
               "TA_F",
               "TA_F_QC",
               "P_F", 
               "P_F_QC",
               "TS_F_MDS_1",
               "TS_F_MDS_1_QC",
               "SWC_F_MDS_2",
               "SWC_F_MDS_2_QC")

# Timestamp start
# Ground heat flux gapfilled 
# Ground heat flux gapfill quality flag
# Incoming shortwave radiation gapfilled (consolidated from MDS and ERA)
# Incoming shortwave radiation gapfill quality flag
# Incoming longwave radiation gapfilled (consolidated from MDS and ERA)
# Incoming longwave radiation gapfill quality flag
# Ambient air temperature gapfilled
# Ambient air temperature gapfill quality flag
# Wind speed gapfilled
# Wind speed gapfil quality flag
# Precipitation gapfilled
# Precipitation gapfill quality flag
# Soil temperature gapfilled
# Soil temperature gapfill quality flag
# Soil water content gapfilled (all SWC_F_MDS_1 are -9999)
# Soil water content gapfill quality flag

pffn_path <- file.path(source_data_dir, "FluxNet", 
                       "FLX_US-Prr_FLUXNET2015_FULLSET_2010-2014_1-3",
                       "FLX_US-Prr_FLUXNET2015_FULLSET_HH_2010-2014_1-3.csv")
pffn <- fread(pffn_path, select = pffn_vars)

# Join AmeriFlux and FluxNet versions
pf <- pffn[pf, on = "TIMESTAMP_START"]
# convert from integer timestamp
pf <- convert_ts(pf)
# add site id
pf$stid <- "pfrr"

# copy FN vars over AF vars where FN has data
pf$G_F_MDS[is.na(pf$G_F_MDS)] <- pf$G_1_1_1[is.na(pf$G_F_MDS)]
pf$SW_IN_F[is.na(pf$SW_IN_F)] <- pf$SW_IN[is.na(pf$SW_IN_F)]
pf$LW_IN_F[is.na(pf$LW_IN_F)] <- pf$LW_IN[is.na(pf$LW_IN_F)]
pf$TA_F[is.na(pf$TA_F)] <- pf$TA_1_1_1[is.na(pf$TA_F)]
pf$P_F[is.na(pf$P_F)] <- pf$P_RAIN[is.na(pf$P_F)]
pf$TS_F_MDS_1[is.na(pf$TS_F_MDS_1)] <- pf$TS_1_1_1[is.na(pf$TS_F_MDS_1)]
pf$SWC_F_MDS_2[is.na(pf$SWC_F_MDS_2)] <- pf$SWC_1_1_1[is.na(pf$SWC_F_MDS_2)]

# the gap-filling method here does not use any of the original data.
#   So, I am assigning the qc flag as measured (0) if the difference
#   between gap-filled and measured is less than 5.
pf$le_qc <- -9999
pf <- pf[LE_1_1_1 != -9999 & LE_PI_F_1_1_1 != -9999 & 
           abs(LE_1_1_1 - LE_PI_F_1_1_1) < 5, le_qc := 0]
pf$h_qc <- -9999
pf <- pf[H_1_1_1 != -9999 & H_PI_F_1_1_1 != -9999 & 
           abs(H_1_1_1 - H_PI_F_1_1_1) < 5, h_qc := 0]
# aggregate variables
pf[is.na(pf)] <- -8888
pf[pf == -9999] <- NA
# ground fluxes
#pf <- pf[, g := rowMeans(.SD, na.rm = TRUE), 
#         .SDcols = c("G_1_1_1", "G_1_1_2")]
# Since the FluxNet uses only the G_1_1_1, will only use that where
#   FN data ends
# snow depths
pf <- pf[, snowd := rowMeans(.SD, na.rm = TRUE),
         .SDcols = c("D_SNOW_1_1_1", "D_SNOW_1_1_2", "D_SNOW_1_1_3")]
# rnet
pf <- pf[, rnet := SW_IN_F - SW_OUT + LW_IN_F - LW_OUT]
# reset missing values
pf[is.na(pf)] <- -9999
pf[pf == -8888] <- NA

# remove vars
pf <- pf[, -c("TIMESTAMP_START", "G_1_1_1", "SW_IN", "LW_IN", 
              "LE_1_1_1", "H_1_1_1",
              "TA_1_1_1", "P_RAIN", "D_SNOW_1_1_1", 
              "D_SNOW_1_1_2", "D_SNOW_1_1_3", "TS_1_1_1", "SWC_1_1_1")]

# rename variables
new_names <- c("g", "g_qc", "sw_in", "sw_in_qc", "lw_in", "lw_in_qc", "ta", 
               "ta_qc", "precip", "precip_qc", "tsoil", "tsoil_qc", "swc", 
               "swc_qc", "le", "h", "sw_out", "lw_out", "rh", "ws", "wd", 
               "year", "doy", "hour", "stid", "le_qc", "h_qc", "snowd", "rnet")
names(pf) <- new_names

# add qc vars
pf <- pf[, rh_qc := NA]
pf <- pf[, ws_qc := NA]
# qc var for rnet depends on sw and lw in qc flags 
#   (note: no -9999 values found in either sw_in_qc or lw_in_qc)
temp_df <- pf[, .(sw_in_qc, lw_in_qc)]
qc_fun <- function(x){
  if(any(is.na(x))){
    NA
  }else{max(x)}
}
rnet_qc <- apply(temp_df, 1, qc_fun)
pf <- pf[, rnet_qc := rnet_qc] 

pf <- pf[, ..sel_cols]

fwrite(pf, file.path(ag_dir, "PFRR.csv"))

#------------------------------------------------------------------------------

#-- ARM Facilities ------------------------------------------------------------
# Point Oliktok & Barrow
okpt_vars <- c("TIMESTAMP_START",
               "LE", 
               "H",
               "G_PI_1_1_A",
               "SW_IN",
               "SW_OUT",
               "LW_IN",
               "LW_OUT",
               "NETRAD",
               "T_SONIC",
               "RH",
               "WS",
               "WD",
               "TS_PI_1_1_A",
               "SWC_PI_1_1_A")

# Timestamp start
# Latent heat flux 
# Sensible heat flux
# Ground heat flux PI-averaged
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Sonic air temperature
# Relative humidity
# Wind speed
# Wind direction
# Soil temperature
# Soil water content

okpt_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-A03_BASE-BADM_2-5", 
                       "AMF_US-A03_BASE_HH_2-5.csv")
okpt <- fread(okpt_path, select = okpt_vars, skip = 2)
barw_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-A10_BASE-BADM_2-5", 
                       "AMF_US-A10_BASE_HH_2-5.csv")
barw <- fread(barw_path, select = okpt_vars, skip = 2)

okpt <- convert_ts(okpt)
barw <- convert_ts(barw)

# add remaining variables
# stid
okpt$stid <- "okpt"
barw$stid <- "barw"
new_names <- c("t1", "le", "h", "g", "sw_in", "sw_out", "lw_in", "lw_out", 
               "rnet", "ta", "rh", "ws", "wd", "tsoil", "swc", "year", "doy",
               "hour", "stid")
names(okpt) <- new_names
names(barw) <- new_names

# add columns for snow and precip
okpt$precip <- NA
okpt$snowd <- NA
barw$precip <- NA
barw$snowd <- NA

okpt <- add_qc(okpt)[, ..sel_cols]
barw <- add_qc(barw)[, ..sel_cols]

fwrite(okpt, file.path(ag_dir, "OKPT.csv"))
fwrite(barw, file.path(ag_dir, "BARW.csv"))

#------------------------------------------------------------------------------

#-- Anaktuvuk river burn sites ------------------------------------------------
# Anaktuvuk vars
anak_vars <- c("TIMESTAMP_START",
               "LE", 
               "H",
               "G",
               "SW_IN",
               "SW_OUT",
               "LW_IN",
               "LW_OUT",
               "NETRAD",
               "TA",
               "RH",
               "WS",
               "WD",
               "P",
               "TS_1",
               "SWC_1")

# Timestamp start
# Latent heat flux 
# Sensible heat flux
# Ground heat flux PI-averaged
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Sonic air temperature
# Relative humidity
# Wind speed
# Wind direction
# Soil temperature
# Soil water content

ansb_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-An1_BASE-BADM_1-1", 
                       "AMF_US-An1_BASE_HH_1-1.csv")
ansb <- fread(ansb_path, select = anak_vars, skip = 2)
anmb_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-An2_BASE-BADM_1-1", 
                       "AMF_US-An2_BASE_HH_1-1.csv")
anmb <- fread(anmb_path, select = anak_vars, skip = 2)
anub_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-An3_BASE-BADM_1-1", 
                       "AMF_US-An3_BASE_HH_1-1.csv")
anub <- fread(anub_path, select = anak_vars, skip = 2)

# convert timestamp
ansb <- convert_ts(ansb)
anmb <- convert_ts(anmb)
anub <- convert_ts(anub)

# add remaining variables
# stid
ansb$stid <- "ansb"
anmb$stid <- "anmb"
anub$stid <- "anub"
new_names <- c("t1", "le", "h", "g", "sw_in", "sw_out", "lw_in", "lw_out", 
               "rnet", "ta", "rh", "ws", "wd", "precip", "tsoil", "swc", 
               "year", "doy", "hour", "stid")
names(ansb) <- new_names
names(anmb) <- new_names
names(anub) <- new_names

# add columns for snow and precip
ansb$snowd <- NA
anmb$snowd <- NA
anub$snowd <- NA

ansb <- add_qc(ansb)[, ..sel_cols]
anmb <- add_qc(anmb)[, ..sel_cols]
anub <- add_qc(anub)[, ..sel_cols]

fwrite(ansb, file.path(ag_dir, "ANSB.csv"))
fwrite(anmb, file.path(ag_dir, "ANMB.csv"))
fwrite(anub, file.path(ag_dir, "ANUB.csv"))

#------------------------------------------------------------------------------

#-- Atqasuk --------------------------------------------------------------------
# AmeriFlux has some older data than FluxNet, but missing more recent years
#   found in FluxNet. 

# AmeriFlux Data
atqa_vars <- c("TIMESTAMP_START",
               "LE",
               "H",
               "G",
               "SW_IN",
               "SW_OUT",
               "LW_IN",
               "LW_OUT",
               "NETRAD",
               "TA",
               "RH",
               "WS",
               "WD",
               "P",
               "TS_1",
               "TS_2",
               "SWC_1",
               "SWC_2")

# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precip
# Soil temperature
# Soil water content

atqa_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-Atq_BASE-BADM_1-1", 
                       "AMF_US-Atq_BASE_HH_1-1.csv")
atqa <- fread(atqa_path, select = atqa_vars, skip = 2)

# FluxNet data
# NOT USING FOR NOW. COMPLICATED DISCREPANCIES
aqfn_vars <- c("TIMESTAMP_START",
               "LE_F_MDS",
               "LE_F_MDS_QC",
               "H_F_MDS",
               "H_F_MDS_QC", 
               "G_F_MDS",
               "G_F_MDS_QC",
               "SW_IN_F",
               "SW_IN_F_QC",
               "LW_IN_F",
               "LW_IN_F_QC",
               "NETRAD",
               "TA_F",
               "TA_F_QC",
               "RH",
               "WS_F", 
               "WS_F_QC",
               "P_F", 
               "P_F_QC",
               "TS_F_MDS_1",
               "TS_F_MDS_1_QC")

# Timestamp start
# Ground heat flux gapfilled 
# Ground heat flux gapfill quality flag
# Incoming shortwave radiation gapfilled
# Incoming shortwave radiation gapfill quality flag
# Incoming longwave radiation gapfilled
# Incoming longwave radiation gapfill quality flag
# Ambient air temperature gapfilled
# Ambient air temperature gapfill quality flag
# Wind speed gapfilled
# Wind speed gapfil quality flag
# Precipitation gapfilled
# Precipitation gapfill quality flag
# Soil temperature gapfilled
# Soil temperature gapfill quality flag

#aqfn_path <- file.path(source_data_dir, "FluxNet", 
#                       "FLX_US-Atq_FLUXNET2015_FULLSET_2003-2008_1-3",
#                       "FLX_US-Atq_FLUXNET2015_FULLSET_HH_2003-2008_1-3.csv")
#aqfn <- fread(aqfn_path, select = aqfn_vars)

#aq <- aqfn[atqa, on = "TIMESTAMP_START"]
#aq_clean <- na.omit(aq)

atqa <- convert_ts(atqa)

# which obs should tsoil = TS_2
atqa$tsoil <- -9999
atqa <- atqa[TS_1 != TS_2 & TS_1 == -9999, tsoil := TS_2]
atqa <- atqa[TS_1 != TS_2 & TS_2 == -9999, tsoil := TS_1]
atqa <- atqa[TS_1 != -9999 & TS_2 != -9999, tsoil := (TS_1 + TS_2)/2]

# stid
atqa$stid <- "atqa"
new_names <- c("t1", "le", "h", "g", "sw_in", "sw_out", "lw_in", "lw_out", 
               "rnet", "ta", "rh", "ws", "wd", "precip", "ts1", "ts2", "swc",
               "swc2", "year", "doy", "hour", "tsoil", "stid")
names(atqa) <- new_names

# add columns for snow and precip
atqa$snowd <- NA

# add QC flags as NAs
atqa <- add_qc(atqa)

atqa <- add_qc(atqa)[, ..sel_cols]

fwrite(atqa, file.path(ag_dir, "ATQA.csv"))

#------------------------------------------------------------------------------

#-- Bonanza Creek Delta Junction ----------------------------------------------
# AmeriFlux Data
bcd_vars <- c("TIMESTAMP_START",
              "LE",
              "H",
              "G",
              "SW_IN",
              "SW_OUT",
              "LW_IN",
              "LW_OUT",
              "NETRAD",
              "TA",
              "RH",
              "WS",
              "WD",
              "P",
              "TS_1",
              "TS_2",
              "SWC_1",
              "SWC_2")

# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precip
# Soil temperature
# Soil water content

bcda_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-Bn1_BASE-BADM_1-1", 
                       "AMF_US-Bn1_BASE_HH_1-1.csv")
bcdb_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-Bn2_BASE-BADM_1-1", 
                       "AMF_US-Bn2_BASE_HH_1-1.csv")
bcdc_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-Bn3_BASE-BADM_1-1", 
                       "AMF_US-Bn3_BASE_HH_1-1.csv")

bcda <- fread(bcda_path, select = bcd_vars, skip = 2)
bcdb <- fread(bcdb_path, select = bcd_vars, skip = 2)
bcdc <- fread(bcdc_path, select = bcd_vars, skip = 2)

# convert timestamps
bcda <- convert_ts(bcda)
bcdb <- convert_ts(bcdb)
bcdc <- convert_ts(bcdc)

# aggregate soil temp measurements
bcda[, tsoil := agg_vars(TS_1, TS_2)]
bcdb[, tsoil := agg_vars(TS_1, TS_2)]
bcdc[, tsoil := agg_vars(TS_1, TS_2)]
# aggregate soil water content measurements
bcda[, swc := agg_vars(SWC_1, SWC_2)]
bcdb[, swc := agg_vars(SWC_1, SWC_2)]
bcdc[, swc := agg_vars(SWC_1, SWC_2)]

# stid
bcda[, stid := "bcda"]
bcdb[, stid := "bcdb"]
bcdc[, stid := "bcdc"]

# rename
new_names <- c("t1", "le", "h", "g", "sw_in", "sw_out", "lw_in", "lw_out", 
               "rnet", "ta", "rh", "ws", "wd", "precip", "ts1", "ts2", "swc1",
               "swc2", "year", "doy", "hour", "tsoil", "swc", "stid")
names(bcda) <- new_names
names(bcdb) <- new_names
names(bcdc) <- new_names

# add columns for snow and precip
bcda$snowd <- NA
bcdb$snowd <- NA
bcdc$snowd <- NA

# add QC flags as NAs and select columns
bcda <- add_qc(bcda)[, ..sel_cols]
bcdb <- add_qc(bcdb)[, ..sel_cols]
bcdc <- add_qc(bcdc)[, ..sel_cols]

fwrite(bcda, file.path(ag_dir, "BCDA.csv"))
fwrite(bcdb, file.path(ag_dir, "BCDB.csv"))
fwrite(bcdc, file.path(ag_dir, "BCDC.csv"))

#------------------------------------------------------------------------------

#-- Barrow (Non-ARM) ----------------------------------------------------------
# AmeriFlux Data
brw_vars <- c("TIMESTAMP_START",
              "LE",
              "H",
              "G",
              "NETRAD",
              "TA",
              "RH",
              "WS",
              "WD",
              "P",
              "TS_1",
              "TS_2")

# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precip
# Soil temperature
# Soil water content

brwb_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-Brw_BASE-BADM_2-1", 
                       "AMF_US-Brw_BASE_HH_2-1.csv")

brwb <- fread(brwb_path, select = brw_vars, skip = 2)

# convert timestamps
brwb <- convert_ts(brwb)

# aggregate soil temp measurements
brwb[, tsoil := agg_vars(TS_1, TS_2)]

# stid
brwb[, stid := "brwb"]

# rename
new_names <- c("t1", "le", "h", "g", "rnet", "ta", "rh", "ws", "wd", "precip", 
               "ts1", "ts2", "year", "doy", "hour", "tsoil", "stid")
names(brwb) <- new_names

# add columns for snow and precip
brwb$sw_in <- NA
brwb$sw_out <- NA
brwb$lw_in <- NA
brwb$lw_out <- NA
brwb$snowd <- NA
brwb$swc <- NA

# add QC flags as NAs and select columns
brwb <- add_qc(brwb)[, ..sel_cols]

fwrite(brwb, file.path(ag_dir, "BRWB.csv"))

#------------------------------------------------------------------------------

#-- Eight Mile Lake -----------------------------------------------------------
# AmeriFlux Data
eml_vars <- c("TIMESTAMP_START",
              "LE",
              "H",
              "SW_IN",
              "SW_IN_PI_F",
              "SW_OUT",
              "LW_IN",
              "LW_OUT",
              "NETRAD",
              "NETRAD_PI_F",
              "TA",
              "TA_PI_F",
              "RH",
              "RH_PI_F",
              "WS",
              "WD",
              "D_SNOW",
              "TS",
              "TS_1_1_1",
              "TS_2_1_1",
              "SWC", 
              "SWC_1_1_1", 
              "SWC_2_1_1")

# Timestamp start
# Latent heat flux
# Sensible heat flux
# Ground heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Incoming longwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precip
# Soil temperature
# Soil water content

emlh_path <- file.path(source_data_dir, "AmeriFlux", 
                       "AMF_US-EML_BASE-BADM_3-5", 
                       "AMF_US-EML_BASE_HH_3-5.csv")

emlh <- fread(emlh_path, select = eml_vars, skip = 2)

apply(emlh, 2, function(x) all(x == -9999))

# convert timestamps
emlh <- convert_ts(emlh)

# aggregate soil temp measurements
emlh[, tsoil := agg_vars(TS_1_1_1, TS_2_1_1)]
emlh[TS != -9999, tsoil := TS]
# aggregate soil water content measurements
emlh[, swc := agg_vars(SWC_1_1_1, SWC_2_1_1)]
emlh[SWC != -9999, swc := SWC]

# need to combine gapfilled vars with observed
# Reltaive humidity
emlh[, ':=' (rh = -9999,
             rh_qc = as.numeric(NA))]
emlh[RH != -9999, ':=' (rh = RH, 
                        rh_qc = 0)]
emlh[RH_PI_F != -9999 & RH == -9999, ':=' (rh = RH_PI_F,
                                           rh_qc = 1)]
# temperature
emlh[, ':=' (ta = -9999,
             ta_qc = as.numeric(NA))]
emlh[TA != -9999, ':=' (ta = RH, 
                        ta_qc = 0)]
emlh[TA_PI_F != -9999 & TA == -9999, ':=' (ta = TA_PI_F,
                                           ta_qc = 1)]
# Incoming shortwave radiation
emlh[, ':=' (sw_in = -9999,
             sw_in_qc = as.numeric(NA))]
emlh[SW_IN != -9999, ':=' (sw_in = SW_IN, 
                           sw_in_qc = 0)]
emlh[SW_IN_PI_F != -9999 & SW_IN == -9999, ':=' (sw_in = SW_IN_PI_F,
                                                 sw_in_qc = 1)]
# Net radiation
emlh[, ':=' (rnet = -9999,
             rnet_qc = as.numeric(NA))]
emlh[NETRAD != -9999, ':=' (rnet = RH, 
                            rnet_qc = 0)]
emlh[NETRAD_PI_F != -9999 & NETRAD == -9999, ':=' (rnet = NETRAD_PI_F,
                                                   rnet_qc = 1)]

# stid
emlh[, stid := "emlh"]

# rename
new_names <- c("t1", "le", "h", "t2", "t3", "sw_out", "lw_in", "lw_out", "t4",
               "t5", "t6", "t7", "t8", "t9", "ws", "wd", "snowd", 
               paste0("t", 10:15), names(emlh)[24:37])
names(emlh) <- new_names

# add columns for snow and precip
emlh[, g := NA]
emlh[, precip := NA]

# add QC flags as NAs and select columns
emlh <- add_qc(emlh)[, ..sel_cols]

fwrite(emlh, file.path(ag_dir, "EMLH.csv"))

#------------------------------------------------------------------------------
