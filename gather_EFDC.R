# Script Summary
#   Compile datasets from remaining flux data stored in EFDC that were 
#   collected at various sites having unique PIs:
#     Gunnarsholt (PI: NA)
#     Skyttorp (PI: A. Grelle)
#     Griffin (PI: J. Moncrieff)
#     Zotino (PI: C. Rebmann)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/ISGU.csv
#   ../data/Arctic_Flux/aggregate/SEKY.csv
#   ../data/Arctic_Flux/aggregate/UKGR.csv
#   ../data/Arctic_Flux/aggregate/RUZO.csv

#-- Setup ---------------------------------------------------------------------
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

# fread for multiple filepaths
my_fread <- function(fps) {
  rbindlist(lapply(fps, function(fp) {
    fread(fp)
  }),
  fill = TRUE)
}

#------------------------------------------------------------------------------

#-- Main ----------------------------------------------------------------------
suppressMessages(library(data.table))

source("helpers.R")

gu_fp <- "../raw_data/EFDC/1911409666/EFDC_L2_Flx_ISGun_1996_v01_30m.txt"
gu_fps <- sapply(
  c("1997_v01", "1998_v01"), 
  function(yr) gsub("1996_v01", yr, gu_fp), 
  USE.NAMES = FALSE
)

sk_fp <- gsub("ISGun", "SESk1", gu_fp)
sk_fps <- sapply(
  c("2004_v02", "2005_v01", "2006_v01", "2007_v01", "2008_v01"), 
  function(yr) gsub("1996_v01", yr, sk_fp), 
  USE.NAMES = FALSE
)

gr_fp <- gsub("ISGun", "UKGri", gu_fp)
gr_fps <- sapply(
  c("2000_v01", "2001_v01", "2004_v016", "2005_v021", "2006_v019", "2007_v015",
    "2008_v013"), 
  function(yr) gsub("1996_v01", yr, gr_fp), 
  USE.NAMES = FALSE
)

zo_fp <- gsub("ISGun", "RUZot", gu_fp)
zo_fps <- sapply(
  c("2002_v01", "2003_v01", "2004_v01"), 
  function(yr) gsub("1996_v01", yr, zo_fp), 
  USE.NAMES = FALSE
)

gu <- my_fread(gu_fps)
sk <- my_fread(sk_fps)
gr <- my_fread(gr_fps)
zo <- my_fread(zo_fps)

gu <- convert_ts(gu)
sk <- convert_ts(sk)
gr <- convert_ts(gr)
zo <- convert_ts(zo)

gu[, stid := "ISGU"]
sk[, stid := "SEKY"]
gr[, stid := "UKGR"]
zo[, stid := "RUZO"]

gu_miss <- c("g", "snowd", "lw_in", "lw_out")
sk_miss <- c("g", "snowd")
gr_miss <- c("snowd")
zo_miss <- c("g", "snowd", "swc")

gu <- mk_vars(gu, gu_miss, "miss")
sk <- mk_vars(sk, sk_miss, "miss")
gr[, snowd := NA]
zo <- mk_vars(zo, zo_miss, "miss")

# fix names
gu_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("SW_IN", "sw_in"),
  c("SW_OUT", "sw_out"),
  c("NETRAD", "rnet"),
  c("TA", "ta"),
  c("RH", "rh"),
  c("P", "precip"),
  c("WS", "ws"),
  c("WD", "wd"),
  c("TS", "tsoil"),
  c("SWC", "swc")
)
sk_names <- gu_names
sk_names[[13]] <- c("LW_IN", "lw_in")
sk_names[[14]] <- c("LW_OUT", "lw_out")
gr_names <- sk_names
gr_names[[15]] <- c("G", "g")
zo_names <- sk_names[-grep("SWC", gu_names)]

gu <- fix_names(gu, gu_names)
sk <- fix_names(sk, sk_names)
gr <- fix_names(gr, gr_names)
zo <- fix_names(zo, zo_names)

# add missing qc and select columns
gu <- sel_cols(add_qc(gu))
sk <- sel_cols(add_qc(sk))
gr <- sel_cols(add_qc(gr))
zo <- sel_cols(add_qc(zo))

fwrite(gu, "../data/Arctic_Flux/aggregate/ISGU.csv")
fwrite(sk, "../data/Arctic_Flux/aggregate/SEKY.csv")
fwrite(gr, "../data/Arctic_Flux/aggregate/UKGR.csv")
fwrite(zo, "../data/Arctic_Flux/aggregate/RUZO.csv")

#------------------------------------------------------------------------------
