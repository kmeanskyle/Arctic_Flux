# Arctic Flux

Curation of a flux measurement database and related meterological variables, for arctic and subarctic sites. raw data has been downloaded from sources such as FluxNet, AmeriFlux, and EDFC. The current cutoff for inclusion of a site is a latitutde of 55N.

#### root
"gather_" scripts are the code for generating standard datasets from the various raw data
**gather_ICBC.R**: curate Imnavait Creek and Bonanza Creek
**helpers.R**: helper functions for various aspects of the data curation in R
**scratch.R**: scratch work and investigating issues
**spatial.R**: spatial operations (e.g. convert listed coordinates to WGS84)

#### data
**aggregate/**: curated data files (completed)

#### documents
misc documents
**data_notes**: log for documenting progress through the data
**README.docx**: about the project
