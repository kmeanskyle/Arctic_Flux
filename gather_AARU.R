# Script Summary
#   compile datasets from AmeriFlux data for sites managed by 
#   M. A. J.-Korczynski of AARHUS University
#   GLZF  -  Greenland
#   GLZH  -  Greenland
#
# Output files:
#   ../data/Arctic_Flux/aggregate/GLZF.csv
#   ../data/Arctic_Flux/aggregate/GLZH.csv

#-- Setup --------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    # don't need memory of missing vars, no NAs from joining
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    for(j in seq_along(temp_dt)){
      set(temp_dt, which(temp_dt[[j]] == -9999), j, value = NA)
    }
    temp_dt[, (vars[1]) := rowMeans(.SD, na.rm = TRUE)]
    temp_dt[is.na(temp_dt[[vars[1]]]), (vars[1]) := -9999]
    temp_dt[[vars[1]]]
  }
  # aggregate QC flags
  aggr_qc <- function(vars) {
    # no NAs in theses datasets, no memory of missing vals needed
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    for(j in seq_along(temp_dt)){
      set(temp_dt, which(temp_dt[[j]] == -9999), j, value = NA)
    }
    temp_dt[, (vars[1]) := matrixStats::rowMaxs(as.matrix(.SD), na.rm = TRUE)]
    temp_dt[is.na(temp_dt[[vars[1]]]), (vars[1]) := -9999]
    temp_dt[is.infinite(temp_dt[[vars[1]]]), (vars[1]) := -9999]
    temp_dt[[vars[1]]]
  }
  # add missing variables with names given
  miss <- function(vars) {
    rep(NA, dim(DT)[1])
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     aggr_qc = aggr_qc, 
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

# FULLSET vars for all sites (some could be missing for each site)

zf_vars <- c(
  "TIMESTAMP_START",
  "LE_F_MDS", "LE_F_MDS_QC",
  "H_F_MDS", "H_F_MDS_QC",
  "G_F_MDS", "G_F_MDS_QC",
  "SW_IN_F_MDS", "SW_IN_F_MDS_QC", 
  "LW_IN_F_MDS", "LW_IN_F_MDS_QC",
  "SW_OUT",
  "NETRAD",
  "TA_F_MDS", "TA_F_MDS_QC",
  "RH",
  "WS_F", "WS_F_QC",
  "WD",
  "P_F", "P_F_QC",
  "TS_F_MDS_1", "TS_F_MDS_1_QC",
  "TS_F_MDS_2", "TS_F_MDS_2_QC",
  "TS_F_MDS_3", "TS_F_MDS_3_QC"
)
zh_vars <- c(zf_vars, "SWC_F_MDS_1", "SWC_F_MDS_1_QC")

zf_fp <- "../raw_data/FluxNet/FLX_DK-ZaF_FLUXNET2015_FULLSET_2008-2011_2-3/FLX_DK-ZaF_FLUXNET2015_FULLSET_HH_2008-2011_2-3.csv"
zh_fp <- gsub("DK-ZaF", "DK-ZaH", zf_fp) %>%
  gsub(pattern = "2008-2011", replacement = "2000-2014")

# suppress missing variable warnings
zf <- fread(zf_fp, select = zf_vars)
zh <- fread(zh_fp, select = zh_vars)

zf <- convert_ts(zf)
zh <- convert_ts(zh)

zf[, stid := "GLZF"]
zh[, stid := "GLZH"]

zf_miss <- c("lw_out", "snowd", "swc")
zf <- mk_vars(zf, zf_miss, "miss")
zh <- mk_vars(zh, zf_miss[-3], "miss")

aggr_lst <- list(
  c("tsoil", "TS_F_MDS_1", "TS_F_MDS_2", "TS_F_MDS_3") 
)
zf <- mk_vars(zf, aggr_lst, "aggr")
zh <- mk_vars(zh, aggr_lst, "aggr")

aggr_qc_lst <- list(
  c("tsoil_qc", "TS_F_MDS_1_QC", "TS_F_MDS_2_QC", "TS_F_MDS_3_QC") 
)
zf <- mk_vars(zf, aggr_qc_lst, "aggr_qc")
zh <- mk_vars(zh, aggr_qc_lst, "aggr_qc")

zf_names <- list(
  c("LE_F_MDS", "le"),
  c("LE_F_MDS_QC", "le_qc"),
  c("H_F_MDS", "h"),
  c("H_F_MDS_QC", "h_qc"), 
  c("G_F_MDS", "g"), 
  c("G_F_MDS_QC", "g_qc"),
  c("SW_IN_F_MDS", "sw_in"),
  c("SW_IN_F_MDS_QC", "sw_in_qc"),
  c("LW_IN_F_MDS", "lw_in"),
  c("LW_IN_F_MDS_QC", "lw_in_qc"),
  c("SW_OUT", "sw_out"),
  c("NETRAD", "rnet"),
  c("TA_F_MDS", "ta"),
  c("TA_F_MDS_QC", "ta_qc"),
  c("RH", "rh"),
  c("WS_F", "ws"),
  c("WS_F_QC", "ws_qc"),
  c("WD", "wd"),
  c("P_F", "precip"),
  c("P_F_QC", "precip_qc")
)
zh_names <- append(
  zf_names,
  list(
    c("SWC_F_MDS_1", "swc"),
    c("SWC_F_MDS_1_QC", "swc_qc")
  )
)

zf <- fix_names(zf, zf_names)
zh <- fix_names(zh, zh_names)

zf <- sel_cols(add_qc(zf))
zh <- sel_cols(add_qc(zh))

fwrite(zf, "../data/Arctic_Flux/aggregate/GLZF.csv")
fwrite(zh, "../data/Arctic_Flux/aggregate/GLZH.csv")

#------------------------------------------------------------------------------
