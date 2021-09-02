#!/usr/bin/python
"""geogen.py

Mark S. Bentley (mark@lunartech.org), 2019

A module of useful functions for working with geogen (PSA
geometry calculation package)

"""

import logging
log = logging.getLogger(__name__)
import json
import os


def generate_plf(config_file, files=None, directory='.', table=None, extras={}):
    """
    Generates a GEOGEN plf input file.

    pds4_utils.Database() is used to scrape meta-data according to the config_file.
    files= specifies the label file pattern (defaults to *.xml)
    directory= specified the root of the input files (and the output location)
    table= specifies the table name in case the input file is configured to
        produce more than one. Default (None) assumes only one table.
    extras = a dictionary which provides extra static key/value pairs to be added
        to every entry (e.g. product type or similar). If an identical value exists
        in the table and extras, extras has priority.
    """

    try:
        from pds4_utils import dbase as db
    except ModuleNotFoundError:
        log.error('pds4_utils module not available, please install before using psa_utils.geogen')
        return None

    # build a database of PDS4 meta-data using the specific config file
    dbase = db.Database(files=files, directory=directory, config_file=config_file)

    # if the config file builds more than one table, we have to select this
    if table is None:
        tables = dbase.dbase.keys()
        if len(tables)>1:
            log.error('there are >1 tables in the specified configuration, specify with table=')
            return None
        t = list(tables)
        if len(t)==0:
            log.error('no tables found, update the configuration file')
            return None
        t = t[0]

    # get the column names from the "extra" meta-data
    table_cols = list(dbase.dbase[t].columns)

    # get the table back (merge the index with the scraped data)
    table = dbase.get_table(t)

    # re-map standard columns according to the expected input for GeoGen
    oldnames = ['bundle', 'product_id', 'start_time', 'stop_time']
    newnames = ['DATA_SET_ID', 'PRODUCT_ID', 'START_TIME', 'STOP_TIME']
    table.rename(columns={i:j for i,j in zip(oldnames, newnames)}, inplace=True)

    # the full set of columns is from both renamed and extra meta-data
    cols = newnames + table_cols 

    # add static column data from extras, if present
    for key in extras.keys():
        table[key] = extras[key]
        if key not in cols:
            cols.append(key)

    j = json.loads(table[cols].to_json(orient='records', date_format='iso'))
    products = {"products": j}
    json_file = os.path.join(directory, t + '.json')
    with open(json_file, 'w') as f:
        json.dump(products, f, indent=4)
