# Script Summary
#   Transform site coordinates taken from web pages, metadata etc



#-- Transform ---------------------------------------------------------------------
# only need one section for now, just manually entering codes and transforming
#   without output
library(sf)

# Imnavait Creek
# http://aon.iab.uaf.edu/imnavait (20190821)
# Fen
ic_23_sf <- st_point(c(405938.720, 7612178.324))
# EPSG code for NAD83 / UTM zone 6n = 26906
ic_23_sf <- st_sfc(ic_23_sf, crs = 26906)
st_transform(ic_23_sf, 4326)
# Ridge
ic_91_sf <- st_point(c(406561.569, 7612263.771))
ic_91_sf <- st_sfc(ic_91_sf, crs = 26906)
st_transform(ic_91_sf, 4326)
# Tussock
ic_93_sf <- st_point(c(406223.006, 7612223.454))
ic_93_sf <- st_sfc(ic_93_sf, crs = 26906)
st_transform(ic_93_sf, 4326)


#------------------------------------------------------------------------------
