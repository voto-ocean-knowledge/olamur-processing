import datetime


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
                           'valid_min': '-90.0', },
              "longitude":
                  {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                   'long_name': 'longitude',
                   'observation_type': 'measured',
                   'platform': 'platform',
                   'reference': 'WGS84',
                   'standard_name': 'longitude',
                   'units': 'degrees_east',
                   'valid_max': '180.0',
                   'valid_min': '-180.0', },

              "oxygen_concentration":
                  {
                      'long_name': 'oxygen concentration',
                      'observation_type': 'calculated',
                      'standard_name': 'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water',
                      'units': 'mmol m-3',
                      'valid_max': '425.',
                      'valid_min': '0.',
                  },
              "chlorophyll":
                  {
                      'long_name': 'chlorophyll',
                      'observation_type': 'calculated',
                      'standard_name': 'concentration_of_chlorophyll_in_sea_water',
                      'units': 'mg m-3',
                      'valid_max': '50.',
                      'valid_min': '0.',
                  },
              "density": {'long_name': 'Density',
                          'standard_name': 'sea_water_density',
                          'units': 'kg m-3',
                          'comment': 'raw, uncorrected salinity',
                          'observation_type': 'calculated',
                          'sources': 'salinity temperature pressure',
                          'valid_min': '1000.0',
                          'valid_max': '1040.0',
                          },

              "conductivity": {
                  'instrument': 'instrument_ctd',
                  'long_name': 'water conductivity',
                  'observation_type': 'measured',
                  'standard_name': 'sea_water_electrical_conductivity',
                  'units': 'mS cm-1',
                  'valid_max': '85.',
                  'valid_min': '0.',
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
                           },
              "temperature":
                  {
                      'long_name': 'water temperature',
                      'observation_type': 'measured',
                      'standard_name': 'sea_water_temperature',
                      'units': 'Celsius',
                      'valid_max': '42',
                      'valid_min': '-5',
                  },
              "datetime":
                  {
                      'long_name': 'datetime',
                      'observation_type': 'measured',
                      'standard_name': 'datetime',
                  },
              }

date_created = str(datetime.datetime.now())
attrs = {
    'acknowledgement': 'This study used data collected and made freely available by Voice of the Ocean Foundation ('
                       'https://voiceoftheocean.org)',
    'creator_email': 'callum.rollo@voiceoftheocean.org',
    'source': 'Observational data from handheld CTD casts',
    'creator_name': 'Callum Rollo',
    'creator_url': 'https://observations.voiceoftheocean.org',
    'date_created': date_created,
    'date_issued': date_created,
    'institution': 'Voice of the Ocean Foundation',
    'keywords': 'CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, '
                'Conductivity, Density, Salinity',
    'keywords_vocabulary': 'GCMD Science Keywords',
    "title": "CTD from glider deployment/recovery",
    'disclaimer': "Data, products and services from VOTO are provided 'as is' without any warranty as to fitness for "
                  "a particular purpose."}