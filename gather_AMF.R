# Script Summary
#   compile datasets from AmeriFlux data for standalone sites
#   ABWP  -  Alberta Western Peatland, AB, CA (PI: L. Flanagan, University of Lethbridge)
#   EMLH  -  Eight Mile Lake, Healy, AK, US (PI: T. Schuur, University of Northern Arizona)
#   NGBR  -  NGEE, Barrow, AK, US (PI: M. Torn)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/ABWP.csv
#   ../data/Arctic_Flux/aggregate/EMLH.csv
#   ../data/Arctic_Flux/aggregate/NGBR.csv

#-- Setup --------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    miss_lst <- lapply(temp_dt, function(col) which(col == -9999))
    for(j in seq_along(temp_dt)){
      set(temp_dt, i=miss_lst[[j]], j=j, value=NA)
    }
    temp_dt[, (vars[1]) := rowMeans(.SD, na.rm = TRUE)]
    new_miss <- Reduce(union, miss_lst)
    new_miss <- intersect(new_miss, which(is.na(temp_dt[[vars[1]]])))
    temp_dt[new_miss, (vars[1]) := -9999]
    temp_dt[[vars[1]]]
  }
  # add qc flags for gapfilled measurements where qc flag not included
  #   and where raw data available. 0 for "close" to observed value, -9999 
  #   otherwise (unknown gapfill quality)
  fill_qc <- function(vars) {
    rpl <- DT[[vars[2]]] != -9999 & DT[[vars[3]]] != -9999 & 
      abs(DT[[vars[2]]] - DT[[vars[3]]]) < 5
    new_vec <- rep(-9999, dim(DT)[1])
    new_vec[rpl] <- 0
    new_vec
  }
  # add missing variables with names given
  miss <- function(vars) {
    rep(NA, dim(DT)[1])
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     fill_qc = fill_qc,
                                     miss = miss))
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages({
  library(data.table)
  library(magrittr)
})

source("helpers.R")

# ABWP
wp_vars <- c(
  "TIMESTAMP_START",
  "LE",
  "H",
  "SW_IN",
  "NETRAD",
  "TA",
  "RH",
  "WS",
  "WD",
  "P_RAIN",
  "D_SNOW",
  "TS_1_1_1",
  "TS_2_1_1"
)
# Timestamp start
# Latent heat flux filtered/gapfilled
# Sensible heat flux filtered/gapfilled
# Incoming shortwave radiation
# Ambient air temperature
# Relative humidity
# Wind speed
# Wind direction
# Rainfall
# Snow depth
# Soil temperature

# EMLH
em_vars <- c(
  wp_vars[-grep("P_RAIN", wp_vars)],
  "SW_IN_PI_F",
  "SW_OUT",
  "LW_IN", 
  "LW_OUT", 
  "NETRAD_PI_F",
  "TA_PI_F", 
  "RH_PI_F",
  "SWC", 
  "SWC_1_1_1", 
  "SWC_2_1_1"
)

# NGBR
ng_vars <- c(
  wp_vars[1:9],
  "LW_IN",
  "SW_OUT",
  "LW_OUT",
  "G_1_1_1",
  "G_2_1_1",
  "G_3_1_1",
  "G_4_1_1"
)

wp_fp <- "../raw_data/AmeriFlux/AMF_CA-WP2_BASE-BADM_1-5/AMF_CA-WP2_BASE_HH_1-5.csv"
em_fp <- gsub("CA-WP2", "US-EML", wp_fp) %>%
  gsub(pattern = "1-5", replacement = "3-5")
ng_fp <- gsub("CA-WP2", "US-NGB", wp_fp)

wp <- fread(wp_fp, select = wp_vars, skip = 2)
em <- fread(em_fp, select = em_vars, skip = 2)
ng <- fread(ng_fp, select = ng_vars, skip = 2)

wp <- convert_ts(wp)
em <- convert_ts(em)
ng <- convert_ts(ng)

wp[, stid := "ABWP"]
em[, stid := "EMLH"]
ng[, stid := "NGBR"]

wp_miss <- c("g", "sw_out", "lw_in", "lw_out", "swc")
em_miss <- c("g", "precip")
ng_miss <- c("precip", "tsoil", "swc", "snowd")

wp <- mk_vars(wp, wp_miss, "miss")
em <- mk_vars(em, em_miss, "miss")
ng <- mk_vars(ng, ng_miss, "miss")

wp_aggr <- list(
  c("tsoil", "TS_1_1_1", "TS_2_1_1") 
)
em_aggr <- wp_aggr
em_aggr[[2]] <- c("swc", "SWC_1_1_1", "SWC_2_1_1", "SWC")
ng_aggr <- list(
  c("g", "G_1_1_1", "G_2_1_1", "G_3_1_1" ,"G_4_1_1")
)

wp <- mk_vars(wp, wp_aggr, "aggr")
em <- mk_vars(em, em_aggr, "aggr")
ng <- mk_vars(ng, ng_aggr, "aggr")

em_fill <- list(
  c("sw_in_qc", "SW_IN", "SW_IN_PI_F"),
  c("rnet_qc", "NETRAD", "NETRAD_PI_F"),
  c("ta_qc", "TA", "TA_PI_F"),
  c("rh_qc", "RH", "RH_PI_F")
)
em <- mk_vars(em, em_fill, "fill_qc")

wp_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("P_RAIN", "precip"),
  c("D_SNOW", "snowd")
)
em_names <- append(
  wp_names[-c(3:6, 9)],
  list(
    c("SW_IN_PI_F", "sw_in"),
    c("LW_IN", "lw_in"),
    c("SW_OUT", "sw_out"),
    c("LW_OUT", "lw_out"),
    c("NETRAD_PI_F", "rnet"),
    c("TA_PI_F", "ta"),
    c("RH_PI_F", "rh")
  )
)
ng_names <- append(
  wp_names[-c(9, 10)],
  list(
    c("LW_IN", "lw_in"),
    c("SW_OUT", "sw_out"),
    c("LW_OUT", "lw_out")
  )
)

wp <- fix_names(wp, wp_names)
em <- fix_names(em, em_names)
ng <- fix_names(ng, ng_names)

wp <- sel_cols(add_qc(wp))
em <- sel_cols(add_qc(em))
ng <- sel_cols(add_qc(ng))

fwrite(wp, "../data/Arctic_Flux/aggregate/ABWP.csv")
fwrite(em, "../data/Arctic_Flux/aggregate/EMLH.csv")
fwrite(ng, "../data/Arctic_Flux/aggregate/NGBR.csv")

#------------------------------------------------------------------------------
