#!/usr/bin/python
"""packager.py

Mark S. Bentley (mark@lunartech.org), 2021

Packages PDS4 products into a delivery package for ingestion into the PSA
"""

from . import common

import os
import sys
import pathlib
import datetime
import numpy as np
import pandas as pd
import shutil
import tarfile
import hashlib
from lxml import etree

import logging
log = logging.getLogger(__name__)

try:
    from pds4_utils import dbase
except ModuleNotFoundError:
    log.error('pds4_utils module not available, please install before using psa_utils.packager')

class Packager():

    def __init__(self, products='*.xml', input_dir='.', recursive=True, output_dir='.', template=None, 
        use_dir=False, clean=True, sendfrom=None, sendto=None):
        """Initialise the packager class. Accepts the following:

        products - file pattern to match labels (*.xml default)
        input_dir - the root directory for the labels (default=.)
        use_dir - uses the product directory structure for the archive
            (if false, a minimal structure will be adopted)
        clean - if True, removes the generated files are generating the tarball"""

        self.products = products
        self.input_dir = input_dir
        self.recursive = recursive
        self.output_dir = output_dir
        self.use_dir = use_dir
        self.index = None
        self.data_files = {}

        # sequentially run everything we need to build the delivery package
        self.get_products()              # index the specified products, get bundle, collection, etc.
        if not self.check_products():    # sanity checks - >1 bundle? etc.
            log.error('product checks failed, aborting')
            return None
        self.get_delivery_name(sendfrom, sendto)         # build the delivery package name

        self.package_dir = os.path.join(self.output_dir, self.delivery_name)
        os.makedirs(self.package_dir, exist_ok=True)

        self.build_paths()               # build delivery paths, according to use_dir
        self.create_transfer_manifest()  # create the transfer manifest .tab file
        self.create_checksum_manifest()  # create the checksum manifest
        self.create_label(template=template)              # flesh out the template PDS4 label
        self.create_package(clean=clean)            # copy files into correct structure and build tarball



    def get_products(self):
        """Obtain the list of products to be packaged and also list data products"""

        self.index = dbase.index_products(directory=self.input_dir, pattern=self.products, recursive=self.recursive)


    def check_products(self):
        """Perform basic sanity checks"""

        bad_products = []

        # check for products from multiple bundles
        if len(self.index.bundle.unique()) > 1:
            log.error('cannot package products from more than one bundle - aborting!')
            return False
        else:
            self.bundle = self.index.bundle.unique()[0]
            self.mission = self.bundle.split('_')[0]

        # check for duplicate products
        if self.index.duplicated(['lid','vid']).sum() > 0:
            log.error('duplicated product LIDVIDs in this package - aborting!')
            return False

        # check that all referenced data files are present
        for idx, product in self.index.iterrows():

            product_file = pathlib.Path(product.filename)
            root = etree.parse(product.filename).getroot()
                
            ns = root.nsmap
            if None in ns and common.pds_ns == ns[None]:
                ns['pds'] = ns.pop(None)

            product_type = root.xpath('name(/*)', namespaces=ns)
            if not product_type.startswith('Product_'):
                log.warn('XML file {:s} is not a PDS4 label, skipping'.format(product_file.name))
                bad_products.append(idx)
                continue

            # data_files =  root.xpath('//pds:File/pds:file_name', namespaces=ns)
            data_files =  root.xpath('//pds:file_name', namespaces=ns)
            for data_file in data_files:
                if not pathlib.Path(os.path.join(product_file.parent, data_file.text)).exists():
                    log.error('cannot find data file {:s} referenced in product {:s}, aborting!'.format(data_file, product_file.name))
                    return False
            self.data_files.update( {product.lid: [f.text for f in data_files]})

        if len(bad_products)>0:
            log.warn('{:d} products removed as invalid'.format(len(bad_products)))
            self.index.drop(bad_products, inplace=True)

        return True
        

    def get_delivery_name(self, sendfrom=None, sendto=None):

        self.delivery_time = datetime.datetime.now()

        if sendfrom is None:
            # get mission acronym from bundle
            # bundle = self.index.bundle.unique()[0]
            # source = bundle.split('_')[0]
            sendfrom = self.mission
        if sendto is None:
            sendto = 'psa'


        self.delivery_name = '{:s}{:s}-pds4-PI-01-{:s}-{:s}'.format(sendfrom, sendto, self.bundle, self.delivery_time.strftime('%Y%m%dT%H%M%S'))


    def build_paths(self):

        self.index['path'] = None

        for idx, product in self.index.iterrows():

            label = pathlib.Path(product.filename)

            if not self.use_dir:
                # simply use the bundle/collection root
                self.index.path.loc[idx] = os.path.join(product.bundle, product.collection, label.name)
            else:
                # use the path relative to the input directory
                self.index.path.loc[idx] = os.path.join(product.bundle, os.path.relpath(label, start=self.input_dir))

        return



    def create_transfer_manifest(self):

        # create tab seaparated files
        manifest_file = self.delivery_name + '-transfer_manifest.tab'
        self.manifest_file = os.path.join(self.package_dir, manifest_file)

        path_len = self.index.path.str.len().max()
        lid_len = self.index.lid.str.len().max()
        self.transfer_fields = {
            'lid_start': 1,
            'lid_len': lid_len,
            'path_start': lid_len + 1,
            'path_len': path_len}

        # pandas to_string does weird things and pads strings, so using numpy instead
        np.savetxt(self.manifest_file, self.index[['lid','path']].values, fmt='%-{:d}s%-{:d}s'.format(lid_len+1, path_len), newline='\r\n')

        self.transfer_records =  len(self.index)

        return


    def create_checksum_manifest(self, template=None):

        checksum = pd.DataFrame([], columns=['checksum', 'filepath'])
        check = []
        filepath = []

        for idx, product in self.index.iterrows():
            # add the label checksum
            check.append(self.md5_hash(product.filename))
            filepath.append(product.path)

            # add the data file checksums
            for data_file in self.data_files[product.lid]:
                data_file_absolute = os.path.join(pathlib.Path(product.filename).parent, data_file)
                check.append(self.md5_hash(data_file_absolute))
                filepath.append(os.path.join(pathlib.Path(product.path).parent, data_file))

        checksum['checksum'] = check
        checksum['filepath'] = filepath

        checksum_file = self.delivery_name + '-checksum_manifest.tab'
        self.checksum_file = os.path.join(self.package_dir, checksum_file)
        checksum.to_csv(self.checksum_file, line_terminator='\r\n', sep='\t', header=False, index=False)
        
        self.checksum_records = len(checksum)

        return


    def create_label(self, template=None):

        if template is None:
            # if no template is given, look for defaults
            template_path = os.path.dirname(__file__)
            template_file = os.path.join(template_path, 'templates', 'product_delivery_template.xml')
        else:
            template_file = template
        
        if not pathlib.Path(template_file).exists():
            log.error('could not open template file {:s}'.format(template.name))
            return None
        else:
            tree = etree.parse(template_file)
            root = tree.getroot()
            ns = root.nsmap
            if None in ns and common.pds_ns == ns[None]:
                ns['pds'] = ns.pop(None)

        root.xpath('/pds:Product_AIP/pds:Identification_Area/pds:logical_identifier', namespaces=ns)[0].text = 'urn:esa:psa:{:s}:data_delivery:{:s}'.format(self.mission, self.delivery_name.lower())
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:Internal_Reference/pds:lid_reference', namespaces=ns)[0].text = 'urn:esa:psa:{:s}:{:s}'.format(self.mission, self.bundle)
        
        cf = pathlib.Path(self.checksum_file)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Checksum_Manifest/pds:File/pds:file_name', namespaces=ns)[0].text  = self.delivery_name + '-checksum_manifest.tab'
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Checksum_Manifest/pds:File/pds:creation_date_time', namespaces=ns)[0].text = self.delivery_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Checksum_Manifest/pds:File/pds:file_size', namespaces=ns)[0].text = str(cf.stat().st_size)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Checksum_Manifest/pds:File/pds:records', namespaces=ns)[0].text = str(self.checksum_records)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Checksum_Manifest/pds:File/pds:md5_checksum', namespaces=ns)[0].text = self.md5_hash(self.checksum_file)

        tf = pathlib.Path(self.manifest_file)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:File/pds:file_name', namespaces=ns)[0].text = self.delivery_name + '-transfer_manifest.tab'
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:File/pds:creation_date_time', namespaces=ns)[0].text = self.delivery_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:File/pds:file_size', namespaces=ns)[0].text =str(tf.stat().st_size)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:File/pds:records', namespaces=ns)[0].text = str(self.transfer_records)

        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:Transfer_Manifest/pds:Record_Character/pds:Field_Character[1]/pds:field_length', namespaces=ns)[0].text = str(self.transfer_fields['lid_len'])
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:Transfer_Manifest/pds:Record_Character/pds:Field_Character[2]/pds:field_location', namespaces=ns)[0].text = str(self.transfer_fields['path_start'])
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:Transfer_Manifest/pds:Record_Character/pds:Field_Character[2]/pds:field_length', namespaces=ns)[0].text = str(self.transfer_fields['path_len'])
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:Transfer_Manifest/pds:Record_Character[1]/pds:record_length', namespaces=ns)[0].text = str(self.transfer_fields['path_start'] + self.transfer_fields['path_len'] + 2)

        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:File/pds:md5_checksum', namespaces=ns)[0].text = self.md5_hash(self.manifest_file)
        root.xpath('/pds:Product_AIP/pds:Information_Package_Component/pds:File_Area_Transfer_Manifest/pds:Transfer_Manifest/pds:records', namespaces=ns)[0].text = str(self.transfer_records)
        root.xpath('/pds:Product_AIP/pds:Archival_Information_Package/pds:description', namespaces=ns)[0].text = 'Generated by bepicolombo.psa_utils.Packager'

        # write out the modified label
        label_file = os.path.join(self.package_dir, self.delivery_name + '.xml')
        tree.write(label_file, xml_declaration=True, encoding=tree.docinfo.encoding) 

        return

    def create_package(self, clean):

        # create a directory
        product_dir = os.path.join(self.package_dir, self.bundle)
        os.makedirs(product_dir, exist_ok=True)

        for idx, product in self.index.iterrows():

            # create the directory structure
            path = os.path.join(self.package_dir, pathlib.Path(product.path).parent)

            try:
                os.makedirs(path, exist_ok=True)
            except OSError:
                log.error ("creation of the directory %s failed" % path)
                return None

            # copy the label
            shutil.copy(product.filename, path)

            # copy the data files referenced
            for data_file in self.data_files[product.lid]:
                data_file_absolute = os.path.join(pathlib.Path(product.filename).parent, data_file)
                shutil.copy(data_file_absolute, path)

        tarball = os.path.join(self.output_dir, self.delivery_name + '.tar.gz')
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(self.package_dir, arcname=os.path.basename(self.package_dir))

        if clean:
            shutil.rmtree(self.package_dir)

        return


    def md5_hash(self, filename):

        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        with open(filename, 'rb') as f:
            buf = f.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(BLOCKSIZE)
        return hasher.hexdigest()
