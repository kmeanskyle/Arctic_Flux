# Script for scratch work and for investigating issues


#-- BCTH ground heat flux issue -----------------------------------------------
# Extremely large values found for ground heat flux values, need to check
#   whether raw data contains these errors

library(data.table)
library(lubridate)
library(hms)
library(ggplot2)

# read raw data
fn1 <- "F:/raw_data/Arctic_Flux/ICBC/2013_2016_BC_5166_gapfilled_DT.csv"
fn2 <- "F:/raw_data/Arctic_Flux/ICBC/2017_BC_5166_gapfilled_20171231.csv"
vars <- c("Year", "DoY", "Hour", "G1_f", "G2_f", "G3_f", "G4_f")
bcth1 <- fread(fn1, select = vars)
bcth2 <- fread(fn2, select = vars)
# remove units row
bcth1 <- bcth1[Year != "--", ]
bcth2 <- bcth2[Year != "--", ]
# make columns numeric
bcth1 <- bcth1[, lapply(.SD, as.numeric)]
bcth2 <- bcth2[, lapply(.SD, as.numeric)]
# filter to 2016, 2017
bcth1 <- bcth1[Year == 2016, ]
# create single var timestamp
bcth1[, ts := ymd_hms(paste0(as.Date(DoY - 1, origin = paste0(Year, "-01-01")),
                             " ", hms(hours = Hour)))]
bcth2[, ts := ymd_hms(paste0(as.Date(DoY - 1, origin = paste0(Year, "-01-01")),
                             " ", hms(hours = Hour)))]
# reshape data 
bcth <- rbind(bcth1, bcth2)
bcth <- bcth[, -(1:3)]
bcth <- melt(bcth, measure.vars = c("G1_f", "G2_f", "G3_f", "G4_f"), 
             variable.name = "g")
bcth[value == -9999, value := NA]

# plot time series
p <- ggplot(bcth, aes(ts, value, col = g)) + 
  geom_line() + 
  theme_bw()
p

table(bcth[value > 1000]$g)
# looks like G1_f is wonky
# plot time series with G1_f removed
p <- ggplot(bcth[g != "G1_f"], aes(ts, value, col = g)) + 
  geom_line() + 
  theme_bw()
p
# looks like G2_f also poor quality, many 0 values
p <- ggplot(bcth[g != "G1_f" & g != "G2_f"], aes(ts, value, col = g)) + 
  geom_line() + 
  theme_bw()
p

#------------------------------------------------------------------------------

#-- old scrap code ------------------------------------------------------------
#-- Create New Columns Conditionally ------------------------------------------
# Need to create new column of average of four columns conditionally, only 
#   using values ont equal to -9999
library(data.table)
library(dplyr)

set.seed(313)
x <- c(seq(20, 30, length.out = 21), rep(-9999, 20))
df <- data.frame(matrix(sample(x, 40, replace = TRUE), ncol = 4))

df[df == -9999] <- NA
df %>%
  rowwise() %>%
  mutate(X5 = mean(c(X1, X2, X3, X4), na.rm = TRUE))



#------------------------------------------------------------------------------

#-- Imnavit Creek Explore -----------------------------------------------------
# suggested that we use the combined, "_gapfilled" dataset.

# vars of interest
ic_vars <- c("Year",
             "DoY", 
             "Hour",
             "LE_cw_gf",
             "qc_LE_cw_gf",
             "H_c_gf",
             "qc_H_c_gf",
             "Rg_gf",
             "qc_Rg_gf",
             "Rg_out_f",
             "Rlong_f",
             "Rlong_out_f",
             "RNET_f",
             "G1_f",
             "G2_f", 
             "G3_f",
             "G4_f",
             "Ta_gf",
             "qc_Ta_gf",
             "VPD_gf",
             "qc_VPD_gf",
             "WS_1",
             "WD",
             "PRECIP",
             "SnowD_f",
             "Tsoil_gf",
             "qc_Tsoil_gf",
             "Tsoil_2",
             "SWC_1_f", 
             "SWC_2_f")


# Year
# Day of Year
# Hour
# Latent heat flux and qc flag
# Sensible heat flux and qc flag
# Incoming shortwave radiation and qc flag
# Outgoing shortwave radiation filtered
# Incoming longwave radiation filtered
# Outgoing longwave radiation filtered
# Net radiation filtered
# Ground heat fluxes
# Ambient air temperature
# Air temperature qc flag
# Vapor pressure deficit
# Vapor pressure deficit qc flag
# Wind speed
# Wind direction
# Precipitation
# Snow depth filtered
# Soil temperature
# Soil temperature qc flag
# Soil temperature 2
# Soil water content filtered

ic_91_span_path <- file.path(source_data_dir, "Eugenie_data",
                             "IC_1991_gapfilled_2008_2013.csv")
ic_91_span <- fread(ic_91_span_path, select = ic_vars)[-1, ]

length(which(ic_23_span$SWC_1_f != "-9999"))
length(which(ic_23_span$SWC_2_f != "-9999"))
x1 <- as.numeric(ic_23_span$SWC_1_f)
x2 <- as.numeric(ic_23_span$SWC_2_f)
bad <- unique(c(which(x1 == -9999), which(x2 == -9999)))
x1 <- x1[-bad]
x2 <- x2[-bad]
cor(x1, x2)
# correlation of 0.72

# comparing variables between AON file and gapfilled (will only use gap-filled)
ic_aon_vars <- c("  \"\"TIMESTAMP\"\"",
                 "Ta_1_gf",
                 "VPD_gf",
                 "Rg_gf",
                 "Rg_out_gf",
                 "Rl_gf",
                 "Rl_out_gf",
                 "RNET_gf",
                 "PRECIP_f",
                 "SNOWD_gf",
                 "G1_gf",
                 "G2_gf",
                 "G3_gf",
                 "G4_gf",
                 "SWC_1_gf",
                 "SWC_2_gf",
                 "TSOIL_1_gf",
                 "TSOIL_2_gf",
                 "TSURF_gf",
                 "WS_1_f",
                 "WD_1_gf",
                 "WS_gf",
                 "WD_gf",
                 "Ta_gf",
                 "H_c_gf",
                 "LE_cw_gf")

ic_91_08_AON_path <- file.path(source_data_dir, "Eugenie_data", 
                               "2008_AON_IC_1991.csv") 
ic_91_08_AON <- fread(ic_91_08_AON_path, skip = 6, select = ic_aon_vars)[-1, ]

ic_91_08_gf <- ic_91_span %>% filter(Year == "2008")
# air temperature
ic_91_08_AON$Ta_1_gf[1:5]
ic_91_08_AON$Ta_gf[1:5]
ic_91_08_gf$Ta_gf[2:6]

# Time of AON file 0.5 hour ahead. 
#   Will use this to assume GF DATA TIME IS TIME BEGINNING SAMPLING PERIOD

# shortwave radiation
ic_91_08_AON$Rg_gf[1:5]
ic_91_08_gf$Rg_gf[2:6]
# outgoing shortwave radiation
ic_91_08_AON$Rg_out_gf[1:5]
ic_91_08_gf$Rg_out_f[2:6]

# longwave radiation
any(ic_91_08_AON$Rl_gf != "NaN")
any(ic_91_08_gf$Rlong_f != -9999)
# outgoing longwave radiation
any(ic_91_08_AON$Rl_out_gf != "NaN")
any(ic_91_08_gf$Rlong_out_f != -9999)

# correlation
x1 <- as.numeric(ic_91_08_AON$Rg_gf)
x2 <- as.numeric(ic_91_08_gf$Rg_gf[-c(1, 2)])
bad <- unique(c(which(is.na(x1)), which(x2 == -9999)))
x1 <- x1[-bad]
x2 <- x2[-bad]
cor(x1, x2)

# Latent heat
ic_91_08_AON$LE_cw_gf[1:5]
ic_91_08_gf$LE_cw_gf[3:7]

# sensible heat
ic_91_08_AON$H_c_gf[1:5]
ic_91_08_gf$H_c_gf[3:7]

# vapor pressure deficit
all(ic_91_08_AON$VPD_gf == "NaN")
ic_91_08_gf$VPD_gf[2:6]

# soil temp1
ic_91_08_AON$TSOIL_1_gf[1:5]
ic_91_08_gf$Tsoil_gf[2:6]

# soil temp 2
ic_91_08_AON$TSOIL_2_gf[1:5]
ic_91_08_gf$Tsoil_2[2:6]

# Net radiation
ic_91_08_AON$RNET_gf[1:5]
ic_91_08_gf$RNET_f[2:6]

# Precipitation
ic_91_08_AON$PRECIP_f[11630:11634]
ic_91_08_gf$PRECIP[11631:11635]
which(as.numeric(ic_91_08_AON$PRECIP_f) != as.numeric(ic_91_08_gf$PRECIP[-c(1, nrow(ic_91_08_gf))]))

# Snow depth
as.numeric(ic_23_08_AON$SNOWD_gf[1:1000])
as.numeric(ic_23_08_gf$SnowD_f[2:1001])

# soil water content
all(ic_23_08_AON$SWC_1_gf[1:5] == "NaN")
all(ic_23_08_gf$SWC_1_f[2:6] == "-9999")
all(ic_23_08_AON$SWC_2_gf[1:5] == "NaN")
all(ic_23_08_gf$SWC_2_f[2:6] == "-9999")

# wind speed
ic_23_08_AON$WS_gf[1:5]
ic_23_08_AON$WS_1_f[1:5]
ic_23_08_gf$WS_1[2:6]

# wind direction
ic_23_08_AON$WD_gf[1:5]
all(ic_23_08_AON$WD_1_gf[1:5] == "NaN")
ic_23_08_gf$WD[2:6]
x1 <- round(as.numeric(ic_23_08_AON$WD_gf), 2)
x2 <- round(as.numeric(ic_23_08_gf$WD[-c(1, nrow(ic_23_08_gf))]), 2)
bad <- unique(c(which(is.na(x1)), which(x2 == -9999)))
x1 <- x1[-bad]
x2 <- x2[-bad]
x1[which(x1 != x2)]
x2[which(x1 != x2)]

# Imnavit Creek
ic_vars <- c("  \"\"TIMESTAMP\"\"",
             "Ta_1_gf",
             "VPD_gf",
             "Rg_gf",
             "Rg_out_gf",
             "Rl_gf",
             "Rl_out_gf",
             "RNET_gf",
             "PRECIP_f",
             "SNOWD_gf",
             "G1_gf",
             "G2_gf",
             "G3_gf",
             "G4_gf",
             "SWC_1_gf",
             "SWC_2_gf",
             "TSOIL_1_gf",
             "TSOIL_2_gf",
             "TSURF_gf",
             "WS_1_f",
             "WD_1_gf",
             "WS_gf",
             "WD_gf",
             "Ta_gf",
             "H_c_gf",
             "LE_cw_gf")

# Timestamp
# Ambient air temp (gap-filled)(C)
# Vapor pressure deficit (gf)(kPa)
# Incoming short wave radiation (gf)(W m-2)
# Outgoing short wave radiation (gf)(W m-2)
# Incoming long wave radiation (gf)(W m-2)
# Outgoing long wave radiation (gf)(W m-2)
# Net radiation (gf)(W m-2)
# Precipitation (filtered)(mm)
# Snow depth (gf)(m)
# Ground heat flux (gf)(W m-2)
# Ground heat flux (gf)(W m-2)
# Ground heat flux (gf)(W m-2)
# Ground heat flux (gf)(W m-2)
# Soil water content (gf)(%?)
# Soil water content (gf)(%?)
# Soil temperature (gf)(C)
# Soil temperature (gf)(C)
# Surface temperature (gf)(C)
# Wind speed (f)(m s-1)
# Wind direction (gf)(decimal degrees)
# Wind speed (gf)(m s-1)
# Wind direction (gf)(decimal degrees)
# Ambient air temperature (gf)(C)
# Sensible heat flux (gf)(W m-2)
# Latent heat flux (gf)(W m-2)

ic_91_07_path <- file.path(source_data_dir, "Eugenie_data", 
                           "2007_AON_IC_1991.csv") 
ic_91_07 <- fread(ic_91_07_path, skip = 6, select = ic_vars)

ic_93_vars <- ic_vars
ic_93_vars[1] <- "TIMESTAMP"
ic_93_07_path <- file.path(source_data_dir, "Eugenie_data", 
                           "2007_AON_IC_1993.csv") 
ic_93_07 <- fread(ic_93_07_path, skip = 6, select = ic_93_vars)

#------------------------------------------------------------------------------
