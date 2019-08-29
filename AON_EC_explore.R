library(data.table)

workdir <- file.path("C:/Users/Keal/Desktop/IARC/P_ET")
datadir <- file.path(workdir, "data")

data_path <- file.path(datadir, "2018_IC_1991_gapfilled_20181126.csv")
# vector of variables to read
# qc_LE_cw_gf = Latent hear flux frequency and WPL corrected filtered,
#   gap-filled + quality control
# RNET_f = Net radiation
# qc_VPD_gf = Vapor pressure deficit filtered and gap-filled + quality control
# 
IC_eddy_df <- read.csv(data_path, )
