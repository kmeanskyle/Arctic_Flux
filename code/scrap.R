# Script Summary
#   scrap code for development

#-- Setup --------------------------------------------------------------------

library(data.table)

workdir <- getwd()
# downloaded data stored in external drive
source_data_dir <- "F:/Arctic_Flux"

#------------------------------------------------------------------------------

#-- Plan ----------------------------------------------------------------------
# Develop a plan for organizing the data 
#   Start with reading in data by station and getting questions about it
#   answered

#------------------------------------------------------------------------------

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
# Eugenie suggested we use the combined, "_gapfilled" dataset.

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







# FluxNet FULLSET data

# For some variables, there may be related variables we might want access to.
# E.g., with temperature, we might only care about TA_F, the consolidated air
#   temp, and the quality flag for it, TA_F_QC. But there are other variables 
#   as well, like the downscaled only, the gapfilled only, quality tags for 
#   each, etc. 

# Variables from FLUXNET and ICOS data that I want
fn_vars <- c("TIMESTAMP_START", "TIMESTAMP_END", "TA_F", "TA_F_QC",
             "SW_IN_F", "SW_IN_F_QC", "LW_IN_F", "LW_IN_F_QC", "VPD_F", 
             "VPD_F_QC", "P_F", "P_F_QC", "WS_F", "WS_F_QC", "WD", "RH",
             "NETRAD", "SW_OUT", "LW_OUT", "TS_F_MDS_#", "TS_F_MDS_#_QC",
             "SWC_F_MDS_#", "SWC_F_MDS_#_QC", "G_F_MDS", "G_F_MDS_QC",
             "LE_F_MDS", "LE_F_MDS_QC", "H_F_MDS", "H_F_MDS_QC")
# chosen variables in order (NOTE: F = consolidated from gapfilled and ERA):
# Air temperature (deg C)(F)
# Shortwave incoming radiation (W m-2)(F)
# Longwave incoming radiation (W m-2)(F)
# Vapor pressure deficit (hPa)(F)
# Precipitation (mm)(F)
# Wind speed (m s-1)
# Wind direction (decimal degrees)
# Relative humidity (%)
# Net radiation (W m-2)
# Shortwave outgoing radiation (W m-2)
# Longwave outgoing radiation (W m-2)
# Soil temperature (deg C)
# Soil water content (%)
# Ground heat flux (W m-2)
# Latent heat flux (W m-2)(variable incomplete - use patch file for QC var)
# Sensible heat flux (W m-2)(variable incomplete - use patch file for QC var)

# AmeriFlux Data
af_vars <- c("TIMESTAMP_START", "TIMESTAMP_END", "G", "H", "LE", "RH", "TA",
             "VPD_PI", "D_SNOW", "P", "P_RAIN", "P_SNOW", "LW_IN", "LW_OUT", 
             "SW_IN", "SW_OUT", "SW_BC_IN", "SW_BC_OUT", "TS_1","TS_2",
             "WD", "WS", "SWC_1", "SWC_2")





