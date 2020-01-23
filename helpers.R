suppressMessages(library(lubridate))

# vector of variable names for master database
sel_colnames <- c("stid", "year", "doy", "hour",  "le", "le_qc", "h", "h_qc", "g", 
                  "g_qc", "sw_in", "sw_in_qc", "sw_out", "lw_in", "lw_in_qc", 
                  "lw_out", "rnet", "rnet_qc", "ta", "ta_qc", "rh", "rh_qc", "ws", 
                  "ws_qc", "wd", "precip", "precip_qc", "snowd", "tsoil", 
                  "tsoil_qc", "swc", "swc_qc")

# convert hour string to decimal value for integer-valued TIMESTAMP
convert_ts <- function(DT){
  convert_hour <- function(x){
    x <- hms(paste0(substr(x, 9, 10), ":",
                    substr(x, 11, 12), ":00"))
    hour(x) + minute(x)/60
  }
  
  DT <- DT[, c("year", "doy", "hour") := 
             .(as.numeric(substr(TIMESTAMP_START, 1, 4)),
               yday(ymd(substr(TIMESTAMP_START, 1, 8))),
               convert_hour(TIMESTAMP_START))]
  return(DT)
}

# write a function that adds quality flags with values of NA where not present
add_qc <- function(DT){
  # vars where there are no quality flags in any of the flux datasets seen (yet)
  no_qc_vars <- c("year", "doy", "hour", "sw_out", "lw_out", "wd", "snowd", 
                  "stid")
  vars <- names(DT)[-which(names(DT) %in% no_qc_vars)]
  qci <- grep("qc", vars)
  qc_vars <- setdiff(vars, c(gsub("_qc", "", vars[qci]), vars[qci]))
  qc_vars <- intersect(qc_vars, sel_colnames)
  qc_vars <- paste0(qc_vars, "_qc")
  DT <- DT[, (qc_vars) := NA]
  return(DT)
}

# function to average over values (or take non-missing value)
agg_vars <- function(v1, v2) {
  temp <- -9999
  temp <- ifelse(v1 != v2 & v1 == -9999, v2, temp)
  temp <- ifelse(v1 != v2 & v2 == -9999, v1, temp)
  temp <- ifelse(v1 != v2 & v1 != -9999 & v2 != -9999, (v1 + v2)/2, temp)
}

# fix the names for a DT based on list of current name/new name 
#   pairs (names_key)
fix_names <- function(DT, names_key) {
  rpl <- unlist(lapply(names_key, function(vn) which(names(DT) == vn[1])))
  new_names <- unlist(lapply(names_key, function(vn) vn[2]))
  names(DT)[rpl] <- new_names
  DT
}

# calculate net radiation from sw_in, sw_out, lw_in, lw_out
calc_rnet <- function(DT) {
  DT[, rnet := -9999]
  DT <- DT[sw_in != -9999 & sw_out != -9999 &
             lw_in != -9999 & lw_out != -9999, 
           rnet := sw_in - sw_out + lw_in - lw_out]
  
  temp_dt <- DT[, .(sw_in_qc, lw_in_qc)]
  qc_fun <- function(qc_val){
    if(any(is.na(qc_val))) {
      NA
    } else if (any(qc_val == -9999)) {
      -9999
    } else {max(qc_val)}
  }
  # rnet
  DT[, rnet_qc := apply(temp_dt, 1, qc_fun)]
  # make rnet_qc -9999 if rnet can't be calculated
  DT[rnet == -9999, rnet_qc := -9999]
  DT
}

# Universal variable names
# arrange column names in universal order
sel_cols <- function(DT) {
  DT[, ..sel_colnames]
}
