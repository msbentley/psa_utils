#!/usr/bin/python
"""tap.py

Mark S. Bentley (mark@lunartech.org), 2021

A module to make TAP queries of the PSA
"""

from astropy.io.votable import parse_single_table
# from astroquery.utils.tap.core import Tap
# switching to PyVO since astroquery TAP doesn't support maxrecs
# see https://github.com/astropy/astroquery/issues/1581
import pyvo as vo
import pandas as pd
from . import common
import time
from requests.exceptions import HTTPError

job_wait_time = 2 # seconds
job_wait_cycles = 10
psa_tap_url = 'https://archives.esac.esa.int/psa/epn-tap/tap/'
import logging
log = logging.getLogger(__name__)
logging.getLogger("astroquery").setLevel(logging.WARNING)


class PsaTap:

    def __init__(self, tap_url=psa_tap_url):
        """Establish a connection to the PSA TAP server"""
        # self.tap = Tap(url=tap_url)
        self.tap = vo.dal.TAPService(tap_url)



    def query(self, q, sync=True, dropna=True, verbose=False, job_wait_cycles=job_wait_cycles, job_wait_time=job_wait_time):
        """Make a simple query and return the data as a pandas DataFrame"""
        
        if sync:
            try:
                data = self.tap.run_sync(q, maxrec=-1).to_table()
            except ValueError as err:
                log.error('query error: {0}'.format(err))
                return None
            except HTTPError as err:
                log.error('http error: {0}'.format(err))
                return None
            data = data.to_pandas()
        else:
            log.error('async jobs currently disabled')
            return None
            job = self.tap.run_async(q, maxrec=-1)
            for i in range(job_wait_cycles):
                time.sleep(job_wait_time)
                if job.is_finished():
                    break
            if not job.is_finished():
                log.error('asynchronous query did not complete')
                return None
            data = job.get_results().to_pandas()

        if 'time_min' in data.columns:
            data['time_min'] = pd.to_datetime(data['time_min'], origin='julian', unit='D') 

        if 'time_max' in data.columns:
            data['time_max'] = pd.to_datetime(data['time_max'], origin='julian', unit='D') 

        if dropna:
            data.dropna(inplace=True, axis=1, how='all')

        if len(data) == 2000:
            log.warn('results incomplete due to synchronous query limit - repeat with sync=false')

        return data# .squeeze()


def product_id_from_granule_uid(granule_uid):
    """Extracts ther PDS3 or PDS4 product ID from the granule_uid
    returned by EPN-TAP"""

    if granule_uid.startswith('urn:'): # it's PDS4
        product_id = granule_uid.split(':')[-3]
    else: # PDS3
        product_id = granule_uid.split(':')[-1]

    return product_id


def get_missions():
    tap = PsaTap()
    return tap.query('SELECT DISTINCT instrument_host_name from EPN_CORE').squeeze().tolist()

def get_instruments():
    tap = PsaTap()
    return tap.query('SELECT DISTINCT instrument_name from EPN_CORE').squeeze().tolist()

def get_collections(bundle_id):
     tap = PsaTap()
     return tap.query("select distinct granule_gid from epn_core where granule_gid like 'urn:esa:psa:{:s}:%%'".format(bundle_id))


def summarise_mission(mission_name, pretty=True):

    missions = get_missions()
    mission = [name for name in missions if name.lower()==mission_name.lower()]
    if len(mission)==0:
        log.error('mission name {:s} not found'.format(mission_name))
        return None
    else:
        tap = PsaTap()
        result = tap.query("SELECT instrument_name, count(*) FROM epn_core WHERE instrument_host_name='{:s}' GROUP BY instrument_name".format(mission[0]))
        if pretty:
            common.printtable(result)
        return result


def summarise_instrument(instrument_name, pretty=False):
 
    instruments = get_instruments()
    instrument = [name for name in instruments if name.lower()==instrument_name.lower()]
    if len(instrument)==0:
        log.error('instrument name {:s} not found'.format(instrument_name))
        return None
    else:
        tap = PsaTap()
        result = tap.query("SELECT processing_level, count(*) FROM epn_core WHERE instrument_name='{:s}' AND processing_level IS NOT NULL GROUP BY processing_level ORDER BY processing_level".format(instrument[0]))
        if pretty:
            common.printtable(result)
        return result

