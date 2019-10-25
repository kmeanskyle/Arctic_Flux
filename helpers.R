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
  vars <- names(DT)[-which(names(DT) %in% c("year", "doy", "hour", "snowd",
                                            "stid"))]
  if(any(grep("qc", vars))){vars <- vars[-grep("qc", vars)]}
  qc_vars <- paste0(vars, "_qc")
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
fix_names <- function(DT) {
  # read master var name compatability file
  # contains the var names besing used as the header and other potential
  #   names to be changed in each column
  names_df <- read.csv("data/var_names.csv")
  rul_names <- names(names_df)
  dt_names <- names(DT)
  get_name <- function(varname) {
    outname <- varname
    if(varname %in% rul_names) {
      outname <- varname
    } else {
      outname <- rul_names[which(apply(names_df, 2, function(x) varname %in% x))]
    }
  }
  names(DT) <- unlist(lapply(dt_names, get_name))
  return(DT[, ..sel_cols])
}

