# Script Summary
#   compile datasets from AmeriFlux data for Anaktuvuk River Burn sites 
#   managed by A. Rocha, G. Shaver, J. Hobbie. 
#   ANSB  -  AK, US
#   ANMB  -  AK, US
#   ANUB  -  AK, US
#
# Output files:
#   ../data/Arctic_Flux/aggregate/ANSB.csv
#   ../data/Arctic_Flux/aggregate/ANMB.csv
#   ../data/Arctic_Flux/aggregate/ANUB.csv

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

# can wrap functions because these sites use same variables
wrap_funs <- function(fp, vars, stid, names_key) {
  # add stid and snowd
  mk_vars <- function(DT) {
    DT[, stid := stid]
    DT[, snowd := NA]
    DT
  }
  fread(fp, select = vars, skip = 2) %>%
    convert_ts %>%
    mk_vars %>%
    fix_names(names_key) %>%
    add_qc %>%
    sel_cols
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages({
  library(data.table)
  library(magrittr)
})

source("helpers.R")

# TS_2 and SWC_2 all == -9999
an_vars <- c(
  "TIMESTAMP_START",
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
  "SWC_1"
)

names_lst <- list(
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
  c("WS", "ws"),
  c("WD", "wd"),
  c("P", "precip"),
  c("TS_1", "tsoil"),
  c("SWC_1", "swc")
)

an1_fp <- "../raw_data/AmeriFlux/AMF_US-An1_BASE-BADM_1-1/AMF_US-An1_BASE_HH_1-1.csv"
an2_fp <- gsub("An1", "An2", an1_fp)
an3_fp <- gsub("An1", "An3", an1_fp)

an1 <- wrap_funs(an1_fp, an_vars, "ANSB", names_lst)
an2 <- wrap_funs(an2_fp, an_vars, "ANSB", names_lst)
an3 <- wrap_funs(an3_fp, an_vars, "ANSB", names_lst)

fwrite(an1, "../data/Arctic_Flux/aggregate/ANSB.csv")
fwrite(an2, "../data/Arctic_Flux/aggregate/ANMB.csv")
fwrite(an3, "../data/Arctic_Flux/aggregate/ANUB.csv")

#------------------------------------------------------------------------------
