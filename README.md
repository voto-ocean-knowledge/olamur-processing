# olamur-processing

Processing scripts avaiable in the github repo for this project https://github.com/voto-ocean-knowledge/olamur-processing

Data directories `data_from_cruise` and `data_cleaned` must be downloaded from the olamur FTP server and placed in this directory before running the notebooks

## data

### data_from_cruise

Files as downloaded from the instruments on the cruise:
- **ctd** raw text files from the Sun and Sea CTD
- **quinsy** waypoints, sites and manual fixes from the vessel navigation system
- **yuco** csv and gpx fies from the yuco AUV

### data_cleaned

Data that have been combined, standardised and written to an open format. These are created from the files in data_from_cruise using the python notebooks in the github repo

- **ctd**
    - **csv** per-cast csv files
    - **nc** one combined timeseries file and one gridded file at 1 m vertical resolution
- **yuco.csv** all combined data from the YUCO while it is in dive mode
- **quinsy.csv** locations of the CTD casts
