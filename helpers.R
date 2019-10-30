library(lubridate)

# convert hour string to decimal value for integer-valued TIMESTAMP
convert_ts <- function(DT){
  convert_hour <- function(x){
    x <- hms(paste0(substr(x, 9, 10), ":",
                    substr(x, 11, 12), ":00"))
    hour(x) + minute(x)/60
  }
  
  DT <- DT[, c("year", "doy", "hour") := 
             .(substr(TIMESTAMP_START, 1, 4),
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

# fix the names for a given DT which has all of the appropriate vars 
#   but wrong names
fix_names <- function(DT, stid) {
  # read master var name compatability file
  # contains the var names besing used as the header and other potential
  #   names to be changed in each column
  names_df <- read.csv("data/var_names.csv")
  rul_names <- names(names_df)
  dt_names <- names(DT)
  # get universal var name given unique var name
  get_name <- function(varname) {
    outname <- varname
    if(!(varname %in% rul_names)) {
      lookup <- rul_names[which(apply(names_df, 2, function(x) varname %in% x))]
      if(length(lookup) != 0) outname <- lookup
    }
    return(outname)
  }
  new_names <- unlist(lapply(dt_names, get_name))
  keep_names <- c(intersect(new_names, rul_names), "stid")
  names(DT) <- new_names
  # add stid
  DT[, stid := stid]
  
  return(DT[, ..keep_names])
}

# Universal variable names
# arrange column names in universal order
sel_cols <- function(DT) {
  sel_cols <- c("stid", "year", "doy", "hour",  "le", "le_qc", "h", "h_qc", "g", 
                "g_qc", "sw_in", "sw_in_qc", "sw_out", "lw_in", "lw_in_qc", 
                "lw_out", "rnet", "rnet_qc", "ta", "ta_qc", "rh", "rh_qc", "ws", 
                "ws_qc", "wd", "precip", "precip_qc", "snowd", "tsoil", 
                "tsoil_qc", "swc", "swc_qc")
  DT[, ..sel_cols]
}
