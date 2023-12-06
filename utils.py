import datetime
import numpy as np
import xarray as xr
import yaml
import ioos_qc
from ioos_qc.config import Config
from ioos_qc.qartod import aggregate
from ioos_qc.streams import XarrayStream
from ioos_qc.results import collect_results, CollectedResult
import datetime
from pathlib import Path
import logging
_log = logging.getLogger(__name__)

def get_configs():
    configs = {
        "TEMP": {
            "TEMP": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [0, 30], "fail_span": [-2.5, 40]},
                    "spike_test": {"suspect_threshold": 2.0, "fail_threshold": 6.0},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                }
            }
        },
        "PSAL": {
            "CNDC": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [6, 42], "fail_span": [3, 45]}
                }
            },
            "PSAL": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [5, 38], "fail_span": [2, 41]},
                    "spike_test": {"suspect_threshold": 0.3, "fail_threshold": 0.9},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                }
            }
        },
        "CNDC": {
            "CNDC": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [6, 42], "fail_span": [3, 45]},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                }
            },
        },
        "DOXY": {
            "DOXY": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [0, 350], "fail_span": [0, 500]},
                    "spike_test": {"suspect_threshold": 10, "fail_threshold": 50},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                }
            }
        },

        "CPWC": {
            "CPWC": {
                "qartod": {
                    "gross_range_test": {"suspect_span": [0, 15], "fail_span": [-1, 20]},
                    "spike_test": {"suspect_threshold": 1, "fail_threshold": 5},
                    "location_test": {"bbox": [10, 50, 25, 70]},
                }
            }
        }
    }
    return configs

def apply_ioos_flags(ds, config):
    if not set(config.keys()).issubset(set(list(ds))):
        _log.warning(f"{ds.keys} not found in dataset. Skipping")
        return None, None
    c = Config(config)
    qc = XarrayStream(ds, lon="longitude", lat="latitude")
    runner = list(qc.run(c))
    results = collect_results(runner, how='list')
    agg = CollectedResult(
        stream_id='',
        package='qartod',
        test='qc_rollup',
        function=aggregate,
        results=aggregate(results),
        tinp=qc.time(),
        data=ds
    )
    flag_vals = agg.results
    call = c.calls
    proc_record = str(call)
    return flag_vals, proc_record


def flag_ioos(ds):
    configs = get_configs()

    for config_name, config in configs.items():
        if config_name not in list(ds):
            _log.warning(f"{config_name} not found in dataset")
            continue
        # extract ioos flags for these variables
        flags, comment = apply_ioos_flags(ds, config)
        flagged_prop = 100 * sum(np.logical_and(flags > 1, flags < 9)) / len(flags)
        _log.info(f"Flagged {flagged_prop.round(3)} % of {config_name} as bad")
        # Apply flags and add comment
        ioos_comment = f"Quality control flags from IOOS QC QARTOD https://github.com/ioos/ioos_qc Version: " \
                       f"{ioos_qc.__version__}. Using config: {comment}."
        if "temperature" in config.keys() or "salinity" in config.keys() or "conductivity" in config.keys():
            ioos_comment = f"{ioos_comment}  Threshold values from EuroGOOS DATA-MEQ Working Group (2010)" \
                           f" Recommendations for in-situ data Near Real Time Quality Control [Version 1.2]. EuroGOOS" \
                           f", 23pp. DOI http://dx.doi.org/10.25607/OBP-214."
        
        flag = ds[config_name].copy()
        flag.values = flags
        parent_attrs = flag.attrs
        flag.attrs = {
            'ioos_qc_module': 'qartod',
            "quality_control_conventions": "IOOS QARTOD standard flags",
            "quality_control_set": 1,
            "valid_min": 1,
            "valid_max": 9,
            "flag_values": [1, 2, 3, 4, 9],
            'flag_meanings': 'GOOD, UNKNOWN, SUSPECT, FAIL, MISSING',
            "long_name": f"quality control flags for {parent_attrs['long_name']}",
            "standard_name": f"{parent_attrs['standard_name']}_flag",
            "comment": ioos_comment}
        ds[f"{config_name}_qc"] = flag
    ds.attrs["processing_level"] = f"L1. Quality control flags from IOOS QC QARTOD https://github.com/ioos/ioos_qc " \
                               f"Version: {ioos_qc.__version__} "
    ds.attrs["disclaimer"] = "Data, products and services from VOTO are provided 'as is' without any warranty as" \
                         " to fitness for a particular purpose."
    return ds








def encode_times(ds):
    if 'units' in ds.time.attrs.keys():
        ds.time.attrs.pop('units')
    if 'calendar' in ds.time.attrs.keys():
        ds.time.attrs.pop('calendar')
    ds["time"].encoding["units"] = 'seconds since 1970-01-01T00:00:00Z'
    for var_name in list(ds):
        if "time" in var_name.lower() and not var_name == "time":
            for drop_attr in ['units', 'calendar', 'dtype']:
                if drop_attr in ds[var_name].attrs.keys():
                    ds[var_name].attrs.pop(drop_attr)
            ds[var_name].encoding = ds["time"].encoding
    return ds


clean_names = {"Press [dbar]": "pressure",
               "Temp [°C]": "temperature",
               "Temp. [deg C]": "temperature",
               "Salinity": "salinity",
               "SALIN [PSU]": "salinity",
               "Cond [mS/cm]": "conductivity",
               'Density [kg/m3]': "density",
               'Density [kg/m^3]': "density",
               'DO [μmol/L]': 'oxygen_concentration',
               'Chl_A [µg/l]': 'chlorophyll',
               'Chl-a [ug/l]': 'chlorophyll',
               'sonde_name': 'sonde_name',
               'sonde_number': 'sonde_number',
               'calibration_date': 'calibration_date',
               'filename': 'filename',
               'latitude': 'latitude',
               'cast_number': 'cast_number',
               'longitude': 'longitude',
               "Legato3 Pressure (bar)": "pressure",
               "Legato3 Temperature (�C)": "temperature",
               "Legato3 Salinity (PSU)": "salinity",
               "Legato3 Conductivity (mS/cm)": "conductivity",
               'Legato3 Oxygen Concentration (umol/L)': 'oxygen_concentration',
               'Legato3 Chlorophyll (?)': 'chlorophyll',
               'INX Latitude (�)': 'latitude',
               'INX Longitude (�)': 'longitude',
               }



attrs_dict = {"sonde_name": {"comment": "model name of CTD"},
              "sonde_number": {"comment": "serial number of CTD"},
              "cast_number": {"comment": "cast number"},
              "calibration_date": {"comment": "date of last calibration"},
              "filename":
                  {"comment": "source filename"},
              "latitude": {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                           'long_name': 'latitude',
                           'observation_type': 'measured',
                           'platform': 'platform',
                           'reference': 'WGS84',
                           'standard_name': 'latitude',
                           'units': 'degrees_north',
                           'valid_max': '90.0',
                           'valid_min': '-90.0',
                          },
              "longitude":
                  {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                   'long_name': 'longitude',
                   'observation_type': 'measured',
                   'platform': 'platform',
                   'reference': 'WGS84',
                   'standard_name': 'longitude',
                   'units': 'degrees_east',
                   'valid_max': '180.0',
                   'valid_min': '-180.0',
                  },

              "oxygen_concentration":
                  {
                      'long_name': 'oxygen concentration',
                      'observation_type': 'calculated',
                      'standard_name': 'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water',
                      'units': 'mmol m-3',
                      'valid_max': '425.',
                      'valid_min': '0.',
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/DOXY/',
                  },
              "chlorophyll":
                  {
                      'long_name': 'chlorophyll',
                      'observation_type': 'calculated',
                      'standard_name': 'concentration_of_chlorophyll_in_sea_water',
                      'units': 'mg m-3',
                      'valid_max': '50.',
                      'valid_min': '0.',
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/CPWC/',
                  },
              "density": {'long_name': 'Density',
                          'standard_name': 'sea_water_density',
                          'units': 'kg m-3',
                          'comment': 'raw, uncorrected salinity',
                          'observation_type': 'calculated',
                          'sources': 'salinity temperature pressure',
                          'valid_min': '1000.0',
                          'valid_max': '1040.0',
                          'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/SIGT/'
                          },

              "conductivity": {
                  'instrument': 'instrument_ctd',
                  'long_name': 'water conductivity',
                  'observation_type': 'measured',
                  'standard_name': 'sea_water_electrical_conductivity',
                  'units': 'mS cm-1',
                  'valid_max': '85.',
                  'valid_min': '0.',
                  'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/CNDC/',
              },
              "pressure": {
                  'comment': 'ctd pressure sensor',
                  'instrument': 'instrument_ctd',
                  'long_name': 'water pressure',
                  'observation_type': 'measured',
                  'positive': 'down',
                  'reference_datum': 'sea-surface',
                  'standard_name': 'sea_water_pressure',
                  'units': 'dbar',
                  'valid_max': '1000',
                  'valid_min': '0',
              },
              "salinity": {'long_name': 'water salinity',
                           'standard_name': 'sea_water_practical_salinity',
                           'units': '1e-3',
                           'comment': 'raw, uncorrected salinity',
                           'sources': 'conductivity temperature pressure',
                           'observation_type': 'calulated',
                           'instrument': 'instrument_ctd',
                           'valid_max': '40.0',
                           'valid_min': '0.0',
                           'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/PSAL/'
                           },
              "temperature":
                  {
                      'long_name': 'water temperature',
                      'observation_type': 'measured',
                      'standard_name': 'sea_water_temperature',
                      'units': 'Celsius',
                      'valid_max': '42',
                      'valid_min': '-5',
                      'URI':'https://vocab.nerc.ac.uk/collection/P02/current/TEMP/',
                      
                  },
              "datetime":
                  {
                      'long_name': 'datetime',
                      'observation_type': 'measured',
                      'standard_name': 'datetime',
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/AYMD/',
                  },
              }
def get_attrs(ds):
    date_created = str(datetime.datetime.now())
    attrs = {
        'acknowledgement': 'This study used data collected and made freely available by Voice of the Ocean Foundation ('
                           'https://voiceoftheocean.org)',
        'area': 'Baltic Sea',
        'cdm_data_type': 'TrajectoryProfile',
        'conventions': 'CF-1.10',
        'country': 'SWE',
        'creator_email': 'callum.rollo@voiceoftheocean.org',
        'creator_name': 'Callum Rollo',
        'creator_type': 'Person',
        'creator_url': 'https://observations.voiceoftheocean.org',
        'date_created': date_created,
        'date_issued': date_created,
        'geospatial_lat_max': np.nanmax(ds.latitude),
        'geospatial_lat_min': np.nanmin(ds.latitude),
        'geospatial_lat_units': 'degrees_north',
        'geospatial_lon_max': np.nanmax(ds.longitude),
        'geospatial_lon_min': np.nanmin(ds.longitude),
        'geospatial_lon_units': 'degrees_east',
        'infoUrl': 'https://observations.voiceoftheocean.org',
        'inspire': "ISO 19115",
        'institution': 'Voice of the Ocean Foundation',
        'institution_edmo_code': '5579',
        'keywords': 'CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, '
                    'Conductivity, Density, Salinity',
        'keywords_vocabulary': 'GCMD Science Keywords',
        'license': 'Creative Commons Attribution 4.0 (https://creativecommons.org/licenses/by/4.0/) This study used data collected and made freely available by Voice of the Ocean Foundation (https://voiceoftheocean.org) accessed from https://erddap.observations.voiceoftheocean.org/erddap/index.html',
        "title": "CTD from glider deployment/recovery",
        'disclaimer': "Data, products and services from VOTO are provided 'as is' without any warranty as to fitness for "
                      "a particular purpose.",
        'QC_indicator': 'L1',
        'qc_manual': 'https://cdn.ioos.noaa.gov/media/2020/03/QARTOD_TS_Manual_Update2_200324_final.pdf',
        'qc_method':'https://github.com/ioos/ioos_qc',
        'references': 'Voice of the Ocean Foundation',
        'source': 'Voice of the Ocean Foundation',
        'sourceUrl': 'https://observations.voiceoftheocean.org',
        'standard_name_vocabulary':'CF Standard Name Table v70',
        'source': 'Observational data from handheld CTD casts',
        'time_coverage_end': str(np.nanmax(ds.time)).split(".")[0],
        'time_coverage_start': str(np.nanmin(ds.time)).split(".")[0],
        'variables': list(ds)
    }
    return attrs

def ds_from_df(df):
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds['time'] = ('time', df["datetime"], time_attr)

    for col_name in list(df):
        if col_name in clean_names.keys():
            name = clean_names[col_name]
            ds[name] = ('time', df[col_name], attrs_dict[name])
    ds.attrs = get_attrs(ds)
    ds = encode_times(ds)
    for var_name in list(ds.variables):
        if not "URI" in ds[var_name].attrs.keys():
            continue
        new_name = ds[var_name].attrs["URI"].split("/")[-2]
        if new_name in list(ds.variables):
            continue
        ds[new_name] = ds[var_name]
        ds = ds.drop(var_name)
    ds.attrs["variables"] = list(ds.variables)
    ds = flag_ioos(ds)
    return ds