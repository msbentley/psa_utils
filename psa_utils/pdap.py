
#!/usr/bin/python
"""pdap.py

Mark S. Bentley (mark@lunartech.org), 2021

A module to make PDAP queries of the PSA
"""

from astropy.io.votable import parse_single_table

psa_pdap_url = 'https://archives.esac.esa.int/psa/pdap'
import logging
import requests
import yaml
import pandas as pd
from io import BytesIO


log = logging.getLogger(__name__)
logging.getLogger("astroquery").setLevel(logging.WARNING)


class pdap:

    def __init__(self, pdap_url=psa_pdap_url):

        self.url = pdap_url

    def _url(self, path):
        """Helper function to append the path to the base URL"""
        
        return self.url + path


    def get_datasets(self):
        """Retrieves meta-data for the set of datasets/bundles"""

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURE_CLASS': 'DATA_SET'}) 
        r.raise_for_status()
        table = parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)
        for col in data.columns:
            data[col] = data[col].str.decode('utf-8')
        
        return data


    def get_products(self, dataset_id):

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'DATA_SET_ID': dataset_id}) 
        r.raise_for_status()
        table = parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)
        for col in data.columns:
            data[col] = data[col].str.decode('utf-8')

        # extract VIDs from the download url
        data['VID'] = data['PRODUCT.DATA_ACCESS_REFERENCE'].apply(lambda url: url.split('::')[-1])
        
        return data


    def get_product(self, product_id):

        r = requests.get(
            self._url('/metadata'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'PRODUCT_ID': product_id}) 
        r.raise_for_status()
        table = parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)
        for col in data.columns:
            data[col] = data[col].str.decode('utf-8')

        # extract VIDs from the download url
        data['VID'] = data['PRODUCT.DATA_ACCESS_REFERENCE'].apply(lambda url: url.split('::')[-1])
        
        return data


    def get_files(self, dataset_id):

        r = requests.get(
            self._url('/files'),
            params={
                'RETURN_TYPE': 'VOTABLE',
                'RESOURCE_CLASS': 'PRODUCT',
                'DATA_SET_ID': dataset_id}) 
        r.raise_for_status()
        table = parse_single_table(BytesIO(r.content), pedantic=False)
        data = pd.DataFrame(table.array.data)
        #for col in data.columns:
        #    data[col] = data[col].str.decode('utf-8')

        return data


def latest_version(lid):
    """Uses PDAP to retrieve the highest VID for a given LID"""

    psa_pdap = pdap()
    product = psa_pdap.get_product(lid)
    if len(product)==0:
        log.error('product with LID {:s} not found'.format(lid))
        return None
    else:
        version_list = product.VID.tolist()
        version_list.sort(key=lambda s: list(map(int, s.split('.'))))
        return version_list[-1]
