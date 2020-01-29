# Script Summary
#   Compile dataset from flux data collected at sites managed by IBPC Russia
#   (PI: T. Maximov)
#
# Output files:
#   ../data/Arctic_Flux/aggregate/RUSP.csv
#   ../data/Arctic_Flux/aggregate/RUYP.csv

#-- Setup ---------------------------------------------------------------------
# master function for creating new columns
mk_vars <- function(DT, var_key, mk_opt) {
  # aggregate multiple measurements via mean()
  aggr <- function(vars) {
    temp_dt <- as.data.table(lapply(vars[-1], function(var) DT[[var]]))
    miss_lst <- lapply(temp_dt, function(col) which(col == -9999))
    for(j in seq_along(temp_dt)){
      set(temp_dt, i=miss_lst[[j]], j=j, value=NA)
    }
    #miss_vec <- which(temp_dt == -9999)
    #temp_dt <- NA
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
                                     miss = miss))
  
  new_cols <- unlist(lapply(var_key, function(vars) vars[1]))
  DT[, (new_cols) := new_data]
  DT
}

my_fread <- function(fps) {
  convert_ts <- function(dat) {
    # get h:m:s
    hms_vec <- sprintf("%04.0f", dat$TIME)
    hms_vec <- paste0(substr(hms_vec, 1, 2), ":", substr(hms_vec, 3, 4), ":00")
    # get mdy, make datetime
    mdy_lst <- chron::month.day.year(dat$DOY, c(1, 0, dat$Year[1]))
    mdy_lst[[1]] <- paste0(mdy_lst[[1]], "-")
    mdy_lst[[2]] <- paste0(mdy_lst[[2]], "-")
    ts <- mdy_hms(paste(Reduce(paste0, mdy_lst), hms_vec))
    # separate
    dat[, c("year", "doy", "hour") :=
          .(year = year(ts),
            doy = yday(ts),
            hr = hour(ts) + minute(ts)/60)]
    dat
  }
  rbindlist(lapply(fps, function(fp) {
    hdr <- fread(fp, nrows = 1, header = FALSE)
    dat <- fread(fp, skip = 2, header = FALSE)
    names(dat) <- as.character(hdr[1])
    # change -99999 to -9999
    dat[dat == -99999] <- -9999
    # convert timestamps before binding dt's
    convert_ts(dat)
  }), 
  fill = TRUE)
}

#------------------------------------------------------------------------------

#-- RUSP ----------------------------------------------------------------------
suppressMessages({
  library(data.table)
  library(lubridate)
})

source("helpers.R")

sp_vars <- c(
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
  "TS_F_MDS_1", "TS_F_MDS_1_QC"
)
# Timestamp start
# Latent heat flux (filtered/gapfilled plus qc flag)
# Sensible heat flux (filtered/gapfilled plus qc flag)
# Ground heat flux (filtered/gapfilled plus qc flag)
# Incoming shortwave radiation (filtered/gapfilled plus qc flag)
# Incoming longwave radiation (filtered/gapfilled plus qc flag)
# Outgoing shortwave radiation
# Outgoing longwave radiation
# Net radiation
# Ambient air temperature (filtered/gapfilled plus qc flag)
# Relative humidity
# Wind speed (filtered plus qc flag)
# Wind direction
# Precip (filtered plus qc flag)
# Soil temperature (filtered/gapfilled plus qc flag)
# Soil water content (filtered/gapfilled plus qc flag)

sp_fp <- "../raw_data/FluxNet/FLX_RU-SkP_FLUXNET2015_FULLSET_2012-2014_1-3/FLX_RU-SkP_FLUXNET2015_FULLSET_HH_2012-2014_1-3.csv"
sp <- fread(sp_fp, select = sp_vars)

# convert times
sp <- convert_ts(sp)

# site ids
sp[, stid := "RUSP"]

# add missing vars as NA
miss_vars <- c("swc", "snowd")
sp <- mk_vars(sp, miss_vars, "miss")

# fix names
sp_names <- list(
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
  c("P_F_QC", "precip_qc"),
  c("TS_F_MDS_1", "tsoil"),
  c("TS_F_MDS_1_QC", "tsoil_qc")
)

sp <- fix_names(sp, sp_names)

# add NA qa flags and select cols
sp <- sel_cols(add_qc(sp))

fwrite(sp, "../data/Arctic_Flux/aggregate/RUSP.csv")

#------------------------------------------------------------------------------

#-- RUYP ----------------------------------------------------------------------
yp_fp <- "../raw_data/AsiaFlux/FxMt_YPF_2004_30m_01/FxMt_YPF_2004_30m_01.csv"
yp_fps <- sapply(
  c("2004", "2005", "2006", "2007"), 
  function(yr) gsub("2004", yr, yp_fp), 
  USE.NAMES = FALSE
)

yp <- my_fread(yp_fps)

yp[, stid := "RUYP"]

# aggregate variables
aggr_lst <- list(
  c("ta", "Ta_17.2", "Ta", "Ta _18"),
  c("rh", "Rh_17.2", "Rh", "Rh _18"),
  c("ws", "WS", "WS_18"),
  c("wd", "WD", "WD_18"),
  c("sw_in", "Rg", "Rg_18"),
  c("sw_out", "Rg_out", "Rg_out_18"),
  c("lw_in", "Rgl", "Rgl_18"),
  c("lw_out", "Rgl_out", "Rgl_out_18")
)
#pf <- aggr_vars(pf, aggr_var_lst)
yp <- mk_vars(yp, aggr_lst, "aggr")

# add missing vars as NA
miss_vars <- c("swc", "snowd", "precip")
yp <- mk_vars(yp, miss_vars, "miss")

# fix names
yp_names <- list(
  c("LE", "le"),
  c("H", "h"),
  c("G", "g"),
  c("Rn_18", "rnet"),
  c("Ts_0", "tsoil")
)
yp <- fix_names(yp, yp_names)

# add missing qc flags
yp <- add_qc(yp)

# select columns
yp <- sel_cols(yp)

fwrite(yp, "../data/Arctic_Flux/aggregate/RUYP.csv")

#------------------------------------------------------------------------------
