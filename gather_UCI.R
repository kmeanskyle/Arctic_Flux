# Script Summary
#   Compile datasets from flux data collected at Northern old black spruce UCI
#   old burn sites (PI: M. Goulden)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/UCIA.csv
#   ../data/Arctic_Flux/aggregate/UCIB.csv
#   ../data/Arctic_Flux/aggregate/UCIC.csv
#   ../data/Arctic_Flux/aggregate/UCID.csv
#   ../data/Arctic_Flux/aggregate/UCIE.csv
#   ../data/Arctic_Flux/aggregate/UCIF.csv
#   ../data/Arctic_Flux/aggregate/UCIG.csv
#   ../data/Arctic_Flux/aggregate/UCIH.csv

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

# NS1, 5, 6 same
# NS2, 3, 4, 7 same
# NS8 unique

ns1_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "H",
  "SW_IN",
  "SW_OUT",
  "NETRAD", 
  "TA_1_1_1",
  "RH",
  "WS",
  "WD",
  "P",
  "TS_1_1_1",
  "SWC"
)
# Timestamp start
# Latent heat flux
# Sensible heat flux
# Incoming shortwave radiation
# Outgoing shortwave radiation
# Net radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Precipitation
# Soil temperature
# Soil water content

# varnames for NS2, 3, 4
ns2_vars <- gsub("SWC", "SWC_1_1_1", ns1_vars)
# varnames for NS8
ns8_vars <- gsub("SWC", "SWC_1", ns1_vars)
ns8_vars <- gsub("TA_1_1_1", "TA", ns8_vars)
ns8_vars <- gsub("TS_1_1_1", "TS_1", ns8_vars)
ns8_vars <- c(ns8_vars, c("G", "LW_IN", "LW_OUT"))

ns1_fp <- "../raw_data/AmeriFlux/AMF_CA-NS1_BASE-BADM_3-5/AMF_CA-NS1_BASE_HH_3-5.csv"
ns1 <- fread(ns1_fp, select = ns1_vars, skip = 2)
ns2 <- fread(gsub("NS1", "NS2", ns1_fp), select = ns2_vars, skip = 2)
ns3 <- fread(gsub("NS1", "NS3", ns1_fp), select = ns2_vars, skip = 2)
ns4 <- fread(gsub("NS1", "NS4", ns1_fp), select = ns2_vars, skip = 2)
ns5 <- fread(gsub("NS1", "NS5", ns1_fp), select = ns1_vars, skip = 2)
ns6 <- fread(gsub("NS1", "NS6", ns1_fp), select = ns1_vars, skip = 2)
ns7 <- fread(gsub("NS1", "NS7", ns1_fp), select = ns2_vars, skip = 2)

ns8_fp <- gsub("NS1", "NS8", ns1_fp)
ns8_fp <- gsub("3-5", "1-1", ns8_fp)
ns8 <- fread(ns8_fp, select = ns8_vars, skip = 2)

# convert from integer timestamp
ns1 <- convert_ts(ns1)
ns2 <- convert_ts(ns2)
ns3 <- convert_ts(ns3)
ns4 <- convert_ts(ns4)
ns5 <- convert_ts(ns5)
ns6 <- convert_ts(ns6)
ns7 <- convert_ts(ns7)
ns8 <- convert_ts(ns8)

# add site id
ns1$stid <- "UCIA"
ns2$stid <- "UCIB"
ns3$stid <- "UCIC"
ns4$stid <- "UCID"
ns5$stid <- "UCIE"
ns6$stid <- "UCIF"
ns7$stid <- "UCIG"
ns8$stid <- "UCIH"

# Add missing variables as NA
miss_vars <- c("g", "lw_in", "lw_out", "snowd")
ns1 <- mk_vars(ns1, miss_vars, "miss")
ns2 <- mk_vars(ns2, miss_vars, "miss")
ns3 <- mk_vars(ns3, miss_vars, "miss")
ns4 <- mk_vars(ns4, miss_vars, "miss")
ns5 <- mk_vars(ns5, miss_vars, "miss")
ns6 <- mk_vars(ns6, miss_vars, "miss")
ns7 <- mk_vars(ns7, miss_vars, "miss")

# only var missing for ns8 is snowd
ns8$snowd <- NA

# rename vars
ns1_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("NETRAD", "rnet"),
  c("TA_1_1_1", "ta"),
  c("RH", "rh"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("P", "precip"),
  c("TS_1_1_1", "tsoil"),
  c("SWC", "swc")
)

ns2_names <- ns1_names
ns2_names[[12]][1] <- "SWC_1_1_1"

ns8_names <- ns1_names
ns8_names[[6]][1] <- "TA"
ns8_names[[11]][1] <- "TS_1"
ns8_names[[12]][1] <- "SWC_1"
ns8_names[[13]] <- c("LW_IN", "lw_in")
ns8_names[[14]] <- c("LW_OUT", "lw_out")
ns8_names[[15]] <- c("G", "g")


ns1 <- fix_names(ns1, ns1_names)
ns2 <- fix_names(ns2, ns2_names)
ns3 <- fix_names(ns3, ns2_names)
ns4 <- fix_names(ns4, ns2_names)
ns5 <- fix_names(ns5, ns1_names)
ns6 <- fix_names(ns6, ns1_names)
ns7 <- fix_names(ns7, ns2_names)
ns8 <- fix_names(ns8, ns8_names)

ns1 <- add_qc(ns1)
ns2 <- add_qc(ns2)
ns3 <- add_qc(ns3)
ns4 <- add_qc(ns4)
ns5 <- add_qc(ns5)
ns6 <- add_qc(ns6)
ns7 <- add_qc(ns7)
ns8 <- add_qc(ns8)

ns1 <- sel_cols(ns1)
ns2 <- sel_cols(ns2)
ns3 <- sel_cols(ns3)
ns4 <- sel_cols(ns4)
ns5 <- sel_cols(ns5)
ns6 <- sel_cols(ns6)
ns7 <- sel_cols(ns7)
ns8 <- sel_cols(ns8)


fwrite(ns1, "../data/Arctic_Flux/aggregate/UCIA.csv")
fwrite(ns2, "../data/Arctic_Flux/aggregate/UCIB.csv")
fwrite(ns3, "../data/Arctic_Flux/aggregate/UCIC.csv")
fwrite(ns4, "../data/Arctic_Flux/aggregate/UCID.csv")
fwrite(ns5, "../data/Arctic_Flux/aggregate/UCIE.csv")
fwrite(ns6, "../data/Arctic_Flux/aggregate/UCIF.csv")
fwrite(ns7, "../data/Arctic_Flux/aggregate/UCIG.csv")
fwrite(ns8, "../data/Arctic_Flux/aggregate/UCIH.csv")

#------------------------------------------------------------------------------
