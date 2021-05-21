
#!/usr/bin/python
"""pdap.py

Mark S. Bentley (mark@lunartech.org), 2021

A module to make PDAP queries of the PSA
"""

from astropy.io import votable

psa_pdap_url = 'https://archives.esac.esa.int/psa/pdap'
import logging
import functools
import requests
import warnings
import pandas as pd
from io import BytesIO


log = logging.getLogger(__name__)
warnings.simplefilter('ignore', category=votable.exceptions.VOTableSpecWarning)

def exception(function):
    """
    A decorator that wraps the passed in function and handles
    exceptions raised by requests
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            log.error(e)
        except requests.exceptions.RequestException as e: 
            log.error(e)
    return wrapper


class Pdap:

    def __init__(self, pdap_url=psa_pdap_url):

        self.url = pdap_url

    def _url(self, path):
        """Helper function to append the path to the base URL"""
        
        return self.url + path

    @exception
    def get_datasets(self):
        """Retrieves meta-data for the set of datasets/bundles"""

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURE_CLASS': 'DATA_SET'}) 
        r.raise_for_status()

        table = votable.parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)
        
        return data

    @exception
    def get_products(self, dataset_id):
        """Queries the meta-data endpoint for products in the dataset ID
        given in the call"""

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'DATA_SET_ID': dataset_id}) 
        r.raise_for_status()
        table = votable.parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)

        # extract VIDs from the download url
        data['VID'] = data['PRODUCT.DATA_ACCESS_REFERENCE'].apply(lambda url: url.split('::')[-1])
        
        return data

    @exception
    def get_product(self, product_id):

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'PRODUCT_ID': product_id}) 
        r.raise_for_status()
        table = votable.parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)

        # extract VIDs from the download url
        data['VID'] = data['PRODUCT.DATA_ACCESS_REFERENCE'].apply(lambda url: url.split('::')[-1])
        
        return data.squeeze()

    @exception
    def get_files(self, dataset_id):

        r = requests.get(
            self._url('/files'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'DATA_SET_ID': dataset_id}) 
        r.raise_for_status()
        table = votable.parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)

        return data


def latest_version(lid):
    """Uses PDAP to retrieve the highest VID for a given LID"""

    psa_pdap = Pdap()
    product = psa_pdap.get_product(lid)
    if len(product)==0:
        log.error('product with LID {:s} not found'.format(lid))
        return None
    else:
        version_list = product.VID.tolist()
        version_list.sort(key=lambda s: list(map(int, s.split('.'))))
        return version_list[-1]
