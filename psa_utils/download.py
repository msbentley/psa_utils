#!/usr/bin/python
"""download.py

Mark S. Bentley (mark@lunartech.org), 2021

A module to download public PSA products
"""

import os
import requests
import re
import lxml
from lxml import etree

from . import pdap
from . import tap

import logging
log = logging.getLogger(__name__)


def download_label_by_granule_uid(granule_uid, output_dir='.'):

    label_url = get_label_urls([granule_uid])
    filename = os.path.basename(label_url)
    if label_url is None:
        log.error('could not retrieve label URL')
        return None
    local_filename = download_file(label_url, output_dir, filename)
    return local_filename



def download_file(url, output_dir='.', output_file=None):
    """
    Downloads the file specified by url to the local directory specified
    by output_dir.

    If output_file is set, this wil be used as the output filename.
    If output_file is None, an attempt will be made to get the filename
    from the content-disposition header.
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        if output_file is None:
            filename = get_filename_from_cd(r.headers.get('content-disposition'))
        else:
            filename = output_file
        local_filename = os.path.join(output_dir, filename)
        log.debug('downloading file {:s}'.format(filename))
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    log.info('downloaded file {:s}'.format(filename))
    return local_filename


def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None

    return fname[0].strip('\"')


def download_files(query, output_dir='.', use_dir=False, label_only=False):

    psa_tap = tap.psa_tap()
    psa_pdap = pdap.pdap()

    products = psa_tap.query(query)
    if products is None:
        log.error('no products matching query')
    for idx, product in products.iterrows():
        if product.access_url is None:
            continue



def download_lid(lid, output_dir='.', unzip=True, tidy=True):

    query = "select access_url, granule_uid from epn_core where granule_uid like '%%{:s}%%'".format(lid)
    download_products(query, output_dir, tidy)



def download_products(query, output_dir='.', unzip=True, tidy=True):
    """
    Runs a query against the PSA's EPN-TAP interface. Any products which match,
    and are public (have a download URL) will be downloaded and the zips placed
    into output_dir. If unzip=True they will be unzipped into output_dir and
    if tidy=True the zips will be removed after use
    """

    from zipfile import ZipFile

    psa_tap = tap.psa_tap()

    products = psa_tap.query(query)
    if products is None:
        log.error('no products matching query')
    if ('granule_uid' not in products.columns) or ('access_url' not in products.columns):
        log.error('queries have to return granule_uid and access_url for product download')
        raise ValueError
    for idx, product in products.iterrows():
        product_id = product_id_from_granule_uid(product.granule_uid)
        if product.access_url == '':
            log.warning('skipping proprietary product {:s}'.format(product_id))
            continue
        else:
            log.info('downloading product {:s}'.format(product_id))
            try:
                local_file = download_file(product.access_url, output_dir=output_dir)
            except:
                log.error('failure to download {:s}, skipping'.format(product_id))
                continue
                
        if unzip:
            with ZipFile(local_file, 'r') as zipObj:
                zipObj.extractall(output_dir)
        if tidy:
            os.remove(local_file)


def download_labels_by_query(query, output_dir='.'):

    psa_tap = tap.psa_tap()
    products = psa_tap.query(query)
    download_labels(products, output_dir)

    return

def download_labels(epn_tap_df, output_dir='.'):
    """Accepts a DataFrame as returned by psa_tap.query, uses 
    get_label_urls to add applicable URLs to the DataFrame and
    then downloads each label to output_dir
    """
    
    epn_tap_df = get_label_urls(epn_tap_df)
    for idx, url in epn_tap_df.label_url.iteritems():
        if url is None: # skip PDS3 or proprietary labels
            continue
        filename = os.path.basename(url)
        download_file(url, output_dir='.', output_file=filename)

    return

def get_label_urls(epn_tap_df):
    """Accepts a DataFrame as returned by psa_tap.query, filters for
    PDS4 products, and finds the unique bundles.
    
    For each bundle it retrieves the corresponding file list from PDAP
    and then adds the download URL for the label, returning the df.

    Note that this will only work for PDS4 where labels can easy be
    separated from data products.
    """

    epn_tap_df['label_url'] = None

    epn_tap_df['pds4'] = epn_tap_df.granule_uid.apply(
            lambda uid: True if uid.startswith('urn:') else False)

    pds4 = epn_tap_df[epn_tap_df.pds4]

    # In PDS4 granule_uid is the LIDVID, e.g.
    # urn:esa:psa:em16_tgo_acs:data_partially_processed:acs_par_sc_tir_20180314t075959-20180314t115950-1365-6-048865::1.0

    # The PDAP file service only works on datasets/bundles, so we have to query the whole thing:
    # e.g. urn:esa:psa:em16_tgo_acs

    pds4['bundle'] = pds4.granule_uid.apply(lambda uid: ':'.join(uid.split(':')[0:4]))
    pds4 = pds4[pds4.access_url != ''] # remove proprietary entries
    psa_pdap = pdap()

    for bundle in pds4.bundle.unique():

        log.debug('querying files in bundle {:s}'.format(bundle))
        files = psa_pdap.get_files(bundle)

        # loop through products from the query that are in this bundle
        bundle_products = pds4[pds4.bundle==bundle]
        for idx, product in bundle_products.iterrows():
                product_id = product_id_from_granule_uid(product.granule_uid)
                product_files = files[files.ProductId==product_id]
                if len(product_files)==0:
                    log.error('could not find product {:s} in bundle {:s}'.format(product_id, bundle))
                    continue

                # find the label
                product_files['ext'] = product_files.Filename.apply(lambda x: os.path.splitext(x)[-1])
                xml_files = product_files[product_files.ext.str.lower()=='.xml']
                if len(xml_files)==0:
                    log.error('could not find XML label for product {:s}'.format(product_id))
                    return None
                elif len(xml_files)>1:
                    log.error('more than one XML file found for product {:s}'.format(product_id))
                    return None
                else:
                    epn_tap_df['label_url'].loc[idx] = xml_files.Reference.squeeze()
        
    return epn_tap_df


def read_label_by_url(label_url):
    """Parses a PDS4 label into memory given its URL"""

    try:
        response = requests.get(label_url)
        root = etree.fromstring(bytes(response.text, encoding='utf-8'))
    except:
        log.error('problem retrieving label')
        # todo check exceptions raised here

    return root


def read_label_by_lid(lid):

    query = "select access_url, granule_uid from epn_core where granule_uid like '%%{:s}%%'".format(lid)
    psa_tap = tap.psa_tap()
    products = psa_tap.query(query)
    products = get_label_urls(products)
    root = read_label_by_url(products.label_url.squeeze())

    return root


