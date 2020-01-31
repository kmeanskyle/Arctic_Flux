# Script Summary
#   compile datasets from AmeriFlux data for sites managed by D. Zona of 
#   San Diego State University
#   BRWB  -  Barrow, AK, US
#   IVTK  -  Ivotuk, AK, US
#   ATQA  -  Atqasuk, AK, US
#
# Output files:
#   ../data/Arctic_Flux/aggregate/BRWB.csv
#   ../data/Arctic_Flux/aggregate/IVTK.csv
#   ../data/Arctic_Flux/aggregate/ATQA.csv

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
  # add missing variables with names given
  miss <- function(vars) {
    rep(NA, dim(DT)[1])
  }
  
  new_data <- lapply(var_key, switch(mk_opt, 
                                     aggr = aggr,
                                     fill_qc = fill_qc,
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

# BRWB
br_vars <- c(
  "TIMESTAMP_START",
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
  "TS_2"
)
# IVTK
iv_vars <- c(
  br_vars[-grep("TS", br_vars)],
  "SW_IN",
  "SW_OUT",
  "LW_IN", 
  "LW_OUT", 
  "TS_1_1_1", 
  "TS_2_1_1", 
  "TS_3_1_1", 
  "TS_4_1_1",
  "SWC_2_1_1", 
  "SWC_3_1_1"
)
# ATQA
at_vars <- c(
  br_vars,
  "SW_IN",
  "SW_OUT",
  "LW_IN", 
  "LW_OUT",
  "SWC_1", 
  "SWC_2"
)

br_fp <- "../raw_data/AmeriFlux/AMF_US-Brw_BASE-BADM_2-1/AMF_US-Brw_BASE_HH_2-1.csv"
iv_fp <- gsub("US-Brw", "US-Ivo", br_fp) %>%
  gsub(pattern = "2-1", replacement = "4-5")
at_fp <- gsub("US-Brw", "US-Atq", br_fp) %>%
  gsub(pattern = "2-1", replacement = "1-1")

br <- fread(br_fp, select = br_vars, skip = 2)
iv <- fread(iv_fp, select = iv_vars, skip = 2)
at <- fread(at_fp, select = at_vars, skip = 2)

br <- convert_ts(br)
iv <- convert_ts(iv)
at <- convert_ts(at)

br[, stid := "BRWB"]
iv[, stid := "IVTK"]
at[, stid := "ATQA"]

br_miss <- c("sw_in", "lw_in", "sw_out", "lw_out", "snowd", "swc")
br <- mk_vars(br, br_miss, "miss")
iv[, snowd := NA]
at[, snowd := NA]

br_aggr <- list(
  c("tsoil", "TS_1", "TS_2") 
)
iv_aggr <- list(
  c("tsoil", "TS_1_1_1", "TS_2_1_1", "TS_3_1_1", "TS_4_1_1"),
  c("swc", "SWC_2_1_1", "SWC_3_1_1")
)
at_aggr <- append(
  br_aggr,
  list(
    c("swc", "SWC_1", "SWC_2")
  )
)

br <- mk_vars(br, br_aggr, "aggr")
iv <- mk_vars(iv, iv_aggr, "aggr")
at <- mk_vars(at, at_aggr, "aggr")

br_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("G", "g"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("P", "precip")
)
iv_names <- append(
  br_names,
  list(
    c("SW_IN", "sw_in"),
    c("LW_IN", "lw_in"),
    c("SW_OUT", "sw_out"),
    c("LW_OUT", "lw_out")
  )
)

br <- fix_names(br, br_names)
iv <- fix_names(iv, iv_names)
at <- fix_names(at, iv_names)

br <- sel_cols(add_qc(br))
iv <- sel_cols(add_qc(iv))
at <- sel_cols(add_qc(at))

fwrite(br, "../data/Arctic_Flux/aggregate/BRWB.csv")
fwrite(iv, "../data/Arctic_Flux/aggregate/IVTK.csv")
fwrite(at, "../data/Arctic_Flux/aggregate/ATQA.csv")

#------------------------------------------------------------------------------
