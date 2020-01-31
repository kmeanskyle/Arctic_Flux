# Script Summary
#   Compile datasets from remaining flux data pulled from FluxNet that were 
#   collected at various sites unique PIs:
#     GLNU  -  Nuuk Fen, Greenland (PI: B. Hansen)
#     SVAD  -  Adventdalen, Svalbard (PI: T. Christensen)
#     CHET  -  Cherski, RU (PI: L. Merbold)
#     RUCO  -  Chokurdakh (PI: H. Dolman)
#     RUSA  -  Samoylov (Pi: L. Kutzbach)
#     STGR  -  Stordalen Grassland (PI: T. Friborg)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/GLNU.csv
#   ../data/Arctic_Flux/aggregate/SVAD.csv
#   ../data/Arctic_Flux/aggregate/CHET.csv
#   ../data/Arctic_Flux/aggregate/RUCO.csv
#   ../data/Arctic_Flux/aggregate/RUSA.csv
#   ../data/Arctic_Flux/aggregate/STGR.csv

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

fn_vars <- c(
  "TIMESTAMP_START",
  "LE_F_MDS", "LE_F_MDS_QC",
  "H_F_MDS", "H_F_MDS_QC",
  "G_F_MDS", "G_F_MDS_QC",
  "SW_IN_F_MDS", "SW_IN_F_MDS_QC", 
  "LW_IN_F_MDS", "LW_IN_F_MDS_QC",
  "SW_OUT",
  "LW_OUT",
  "NETRAD",
  "TA_F_MDS", "TA_F_MDS_QC",
  "RH",
  "WS_F", "WS_F_QC",
  "WD",
  "P_F", "P_F_QC",
  "TS_F_MDS_1", "TS_F_MDS_1_QC",
  "TS_F_MDS_2", "TS_F_MDS_2_QC",
  "TS_F_MDS_3", "TS_F_MDS_3_QC",
  "TS_F_MDS_4", "TS_F_MDS_4_QC"
)

nu_fp <- "../raw_data/FluxNet/FLX_DK-NuF_FLUXNET2015_FULLSET_2008-2014_1-3/FLX_DK-NuF_FLUXNET2015_FULLSET_HH_2008-2014_1-3.csv"
sv_fp <- gsub("DK-NuF", "NO-Adv", nu_fp) %>%
  gsub(pattern = "2008-2014", replacement = "2011-2014")
ch_fp <- gsub("DK-NuF", "RU-Che", nu_fp) %>%
  gsub(pattern = "2008-2014", replacement = "2002-2005") 
co_fp <- gsub("DK-NuF", "RU-Cok", nu_fp) %>%
  gsub(pattern = "2008-2014_1-3", replacement = "2003-2014_2-3") 
sm_fp <- gsub("DK-NuF", "RU-Sam", nu_fp) %>%
  gsub(pattern = "2008", replacement = "2002") 
st_fp <- gsub("DK-NuF", "SE-St1", nu_fp) %>%
  gsub(pattern = "2008", replacement = "2012") 

# suppress missing variable warnings
suppressWarnings({
  nu <- fread(nu_fp, select = fn_vars)
  sv <- fread(sv_fp, select = fn_vars)
  ch <- fread(ch_fp, select = fn_vars)
  co <- fread(co_fp, select = fn_vars)
  sm <- fread(sm_fp, select = fn_vars)
  st <- fread(st_fp, select = fn_vars)
})

nu <- convert_ts(nu)
sv <- convert_ts(sv)
ch <- convert_ts(ch)
co <- convert_ts(co)
sm <- convert_ts(sm)
st <- convert_ts(st)

nu[, stid := "GLNU"]
sv[, stid := "SVAD"]
ch[, stid := "CHET"]
co[, stid := "RUCO"]
sm[, stid := "RUSA"]
st[, stid := "STGR"]

nu_miss <- c("lw_out", "snowd", "swc")
nu <- mk_vars(nu, nu_miss, "miss")
sv <- mk_vars(sv, nu_miss[-1], "miss")
ch <- mk_vars(ch, nu_miss, "miss")
co <- mk_vars(co, nu_miss, "miss")
sm <- mk_vars(sm, nu_miss, "miss")
st <- mk_vars(st, nu_miss[-1], "miss")

aggr_lst <- list(
  c("tsoil", "TS_F_MDS_1", "TS_F_MDS_2", "TS_F_MDS_3", "TS_F_MDS_4") 
)
nu_aggr <- aggr_lst
nu_aggr[[1]] <- nu_aggr[[1]][-5]

nu <- mk_vars(nu, nu_aggr, "aggr")
sv <- mk_vars(sv, aggr_lst, "aggr")
ch <- mk_vars(ch, nu_aggr, "aggr")
co <- mk_vars(co, nu_aggr, "aggr")
st <- mk_vars(st, aggr_lst, "aggr")

aggr_qc_lst <- list(
  c("tsoil_qc", "TS_F_MDS_1_QC", "TS_F_MDS_2_QC", "TS_F_MDS_3_QC", "TS_F_MDS_4_QC") 
)
nu_aggr_qc <- aggr_qc_lst
nu_aggr_qc[[1]] <- nu_aggr_qc[[1]][-5]

nu <- mk_vars(nu, nu_aggr_qc, "aggr_qc")
sv <- mk_vars(sv, aggr_qc_lst, "aggr_qc")
ch <- mk_vars(ch, nu_aggr_qc, "aggr_qc")
co <- mk_vars(co, nu_aggr_qc, "aggr_qc")
st <- mk_vars(st, aggr_qc_lst, "aggr_qc")

names_lst <- list(
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
  c("LW_OUT", "lw_out"),
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
nu_names <- names_lst[-grep("LW_OUT", names_lst)]
ch_names <- names_lst[-grep("RH", names_lst)]
sm_names <- append(
  nu_names,
  list(
    c("TS_F_MDS_1", "tsoil"),
    c("TS_F_MDS_1_QC", "tsoil_qc")
  )
)

nu <- fix_names(nu, nu_names)
sv <- fix_names(sv, names_lst)
ch <- fix_names(ch, names_lst)
co <- fix_names(co, names_lst)
sm <- fix_names(sm, sm_names)
st <- fix_names(st, names_lst)

nu <- sel_cols(add_qc(nu))
sv <- sel_cols(add_qc(sv))
ch <- sel_cols(add_qc(ch))
co <- sel_cols(add_qc(co))
sm <- sel_cols(add_qc(sm))
st <- sel_cols(add_qc(st))

fwrite(nu, "../data/Arctic_Flux/aggregate/GLNU.csv")
fwrite(sv, "../data/Arctic_Flux/aggregate/SVAD.csv")
fwrite(ch, "../data/Arctic_Flux/aggregate/CHET.csv")
fwrite(co, "../data/Arctic_Flux/aggregate/RUCO.csv")
fwrite(sm, "../data/Arctic_Flux/aggregate/RUSA.csv")
fwrite(st, "../data/Arctic_Flux/aggregate/STGR.csv")

#------------------------------------------------------------------------------
