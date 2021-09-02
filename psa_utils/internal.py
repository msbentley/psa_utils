#!/usr/bin/python
"""internal.py

Mark S. Bentley (mark@lunartech.org), 2019

A module to of utility functions likely only to be of interest to
PSA internal members (archive scientists etc.)

"""

from tarfile import HeaderError
from . import common
from . import packager
import pathlib
import logging
import copy
from lxml import etree
import shutil
import numpy as np
import pandas as pd
import datetime
import yaml
import os
from io import BytesIO

log = logging.getLogger(__name__)


# Class to produce systematic test ingestion products
proc_levels = {
    'raw': ('Raw', 'raw'),
    'par': ('Partially Processed', 'partially_processed'),
    'cal': ('Calibrated', 'calibrated'),
    'der': ('Derived', 'derived')
}

class Ingest_Test():
    """A class for generating test products from a label and data product
    template and a configuration file specifying the instrument-specific
    data"""

    def __init__(self, config_file='ingestion_test.yml', template_label='test_product.xml', output_dir='.', package=False):

        # read the configuration file
        self.read_config(config_file)
        if self.config is None:
            return

        self.load_template(template_label)
        if self.label is None:
            return

        self.generate_products(output_dir)

        log.info('Test products generated for {:d} bundles'.format(len(self.config)))

        if package:
            self.package(output_dir)


    def package(self, output_dir):
        """Produces one delivery package per bundle of the test data"""

        for bundle in self.config:
            prefix = self.config[bundle]['shortname']
            packager.Packager(input_dir=output_dir,  output_dir=output_dir, products='{:s}*.xml'.format(prefix))


    def read_config(self, config_file):
        """Read the YAML configuration file given by config_file and store in self.config"""

        try:
            f = open(config_file, 'r')
            self.config = yaml.load(f, Loader=yaml.BaseLoader)
        except FileNotFoundError:
            log.error('config file {:s} not found'.format(config_file))

        return


    def load_template(self, template_label):

        self.label = pathlib.Path(template_label)
        if not self.label.exists():
            log.error('could not open template file {:s}'.format(self.label.name))
            self.label = None

        self.tree = etree.parse(template_label)
        ns = self.tree.getroot().nsmap
        if None in ns and common.pds_ns == ns[None]:
            ns['pds'] = ns.pop(None)
        self.ns = ns


    def generate_products(self, output_dir):
        """Use the template_label and information in self.config to generate test products,
        one per configuration, and write the products to output_dir"""

        def get_product_id(bundle, level, sub_instr=None):

            if sub_instr is None:
                sub_instr = ''

            # product ID: {shortname}_{level}_sc_{sub_instr}_test_
            product_id = '{:s}_{:s}_sc_{:s}_test'.format(self.config[bundle]['shortname'], level, sub_instr)
            product_id = product_id.lower()

            return product_id


        def copy_data_file(root, output_dir):

            # copy and rename the template data product
            data_name = root.xpath("//pds:File_Area_Observational/pds:File/pds:file_name", namespaces=self.ns)[0].text
            data_file = pathlib.Path(os.path.join(self.label.parent.absolute(), data_name))
            if not data_file.exists():
                log.error('could not open data file file {:s}'.format(data_name))
                return None
            shutil.copy(data_file, os.path.join(output_dir, product_id + data_file.suffix))

            return data_file

        def update_template(root, bundle, product_id, level, data_file, sub_inst=None):

            mission, host, instrument = bundle.split('_')
            today = datetime.datetime.today()
            # update the template with the mission/instrument-relevant values

            root.xpath('//pds:Identification_Area/pds:logical_identifier', namespaces=self.ns)[0].text = 'urn:esa:psa:{:s}:data_{:s}:{:s}'.format(bundle, proc_levels[level][1], product_id)
            root.xpath('//pds:Identification_Area/pds:version_id', namespaces=self.ns)[0].text = '1.{:d}{:02d}'.format(today.year, today.month)
            root.xpath('//pds:Identification_Area/pds:Modification_History/pds:Modification_Detail/pds:modification_date', namespaces=self.ns)[0].text =today.strftime('%Y-%m-%d')
            root.xpath("//pds:Observing_System_Component[pds:type='Instrument']/pds:Internal_Reference/pds:lid_reference", namespaces=self.ns)[0].text = 'urn:esa:psa:context:instrument:{:s}.{:s}'.format(instrument, host)
            root.xpath("//pds:Observing_System_Component[pds:type='Instrument']/pds:name", namespaces=self.ns)[0].text = self.config[bundle]['fullname']
            root.xpath("//pds:Investigation_Area/pds:Internal_Reference[pds:reference_type='data_to_investigation']/pds:lid_reference", namespaces=self.ns)[0].text = 'urn:esa:psa:context:investigation:mission.{:s}'.format(mission)
            root.xpath("//pds:File_Area_Observational/pds:File/pds:file_name", namespaces=self.ns)[0].text = product_id + data_file.suffix
            root.xpath("//pds:Primary_Result_Summary/pds:processing_level", namespaces=self.ns)[0].text = proc_levels[level][0]
            if sub_inst:
            
                    # Need to insert new sub-instrument class, e.g.:
                    #
                    # <psa:Sub-Instrument>
                    #     <psa:identifier>STROFIO</psa:identifier>
                    #     <psa:name>STROFIO</psa:name>
                    #     <psa:type>Mass spectrometer</psa:type>
                    #     <psa:type>Gas analyser</psa:type>
                    # </psa:Sub-Instrument>

                    # find the Mission_Area
                    mission = root.xpath('//pds:Mission_Area', namespaces=self.ns)[0]

                    # create new elements, working down
                    sub = etree.SubElement(mission, '{{{:s}}}Sub-Instrument'.format(self.ns['psa']), nsmap=self.ns)
                    id = etree.SubElement(sub, '{{{:s}}}identifier'.format(self.ns['psa']), nsmap=self.ns)
                    id.text = sub_inst.upper()
                    name = etree.SubElement(sub, '{{{:s}}}name'.format(self.ns['psa']), nsmap=self.ns)
                    name.text = sub_inst
                    inst_type = etree.SubElement(sub, '{{{:s}}}type'.format(self.ns['psa']), nsmap=self.ns)
                    inst_type.text = self.config[bundle]['sub_instruments'][sub_inst]

                    # insert the new sub-instrument element into the mission element
                    mission.insert(idx, sub)

            return root

        def write_label(tree, output_dir, product_id):

            label_out = os.path.join(output_dir, product_id + '.xml')
            tree.write(label_out, xml_declaration=True, encoding=self.tree.docinfo.encoding) 

#########################

        for level in proc_levels:

            for bundle in self.config:

                if 'sub_instruments' in self.config[bundle].keys(): 
                    for idx, sub_inst in enumerate(self.config[bundle]['sub_instruments']):
                        tree = copy.deepcopy(self.tree)
                        root = tree.getroot()
                        product_id = get_product_id(bundle, level, sub_instr=sub_inst)
                        data_file = copy_data_file(root, output_dir)
                        if data_file is not None:
                            root = update_template(root, bundle, product_id, level, data_file, sub_inst)
                            write_label(tree, output_dir, product_id)

                else:
                    tree = copy.deepcopy(self.tree)
                    root = tree.getroot()
                    product_id = get_product_id(bundle, level, sub_instr=None)
                    data_file = copy_data_file(root, output_dir)
                    if data_file is not None:
                        root = update_template(root, bundle, product_id, level, data_file, None)
                        write_label(tree, output_dir, product_id)


def build_context_json(config_file, input_dir='.', output_dir='.', json_name='local_context_products.json', table='context_bundle'):
    """
    Generates a json file listing the name, type and LIDVID of all
    context files in input_dir. Generates a local context json file
    which can be used by the PDS validate tool and writes it to
    output_dir

    pds4_utils.Database() is used to scrape meta-data according to the config_file.
    """

    from pds4_utils import dbase
    import json

    context = []

    # build a database of context product meta-data
    dbase = dbase.Database(files='*.xml', directory=input_dir, config_file=config_file)
    table = dbase.get_table('context_bundle')

    if table is None:
        return None

    # create one entry for each unique LID
    for lid in table.lid.unique():
        c = {}
        # we only need the LATEST LID in the json file, since validate checks
        # for references to this or below
        product = table[table.lid==lid].sort_values(by='vid')
        product = product.iloc[-1]

        # the config file scrapes both instrument types, that from the IM
        # as type and that from the CTLI (newer IM versions) as ctli_type
        if product['type'] is None:
            prod_type = product['ctli_type']
        else:
            prod_type = product['type']
        
        if type(prod_type) == np.float:
            if pd.isna(prod_type):
                prod_type = ['N/A']

        # either way validate expects a list for type, even if it only has one value
        if type(prod_type) != list:
            t = [prod_type]
        else:
            t = prod_type

        # the json needs name, type, and lidvid
        c['type'] = t
        c['name'] = [product['name']]
        c['lidvid'] = "{:s}::{:s}".format(product.lid, product.vid)

        context.append(c)

    # the root of the json file needs to be Product_Context
    context = {"Product_Context": context}

    json_file = os.path.join(output_dir, json_name)

    # write to a file - indent=4 gives pretty printing
    with open(json_file, 'w') as f:
        json.dump(context, f, indent=4)
        log.info('written json file {:s}'.format(json_file))

    return 


def collection_summary(config_file, input_dir='.', output_dir='.', context_dir='.'):
    """
    collection_summary accesses meta-data in a collection label
    or referenced from it, to produce a set of summary information
    needed to register a DOI and/or create a Google Dataset
    Search landing page.
    """

    from pds4_utils import dbase

    collection_db = dbase.Database(
        files='collection_data*.xml', 
        config_file=config_file, 
        directory=input_dir, 
        recursive=True)

    collection_table = collection_db.get_table('collection')

    # strip carriage returns from the description
    collection_table.description = collection_table.description.apply(lambda desc: desc.replace('\n', ' '))

    # make a string list of keywords
    collection_table.keywords = collection_table.keywords.apply(lambda key: ', '.join(key))

    context_db = dbase.Database(
        files='*.xml', 
        config_file=config_file, 
        directory=context_dir, 
        recursive=True)

    context_table = context_db.get_table('context')
    # timeformatter = lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S.%f')

    for idx, entry in collection_table.iterrows():
        out_name = entry.bundle+'_'+entry.collection+'.html'
        out_file = os.path.join(output_dir, out_name)
        collection_cols = collection_db.dbase['collection'].columns.to_list()

        # tidy up some specific entries
        if entry.mission_lid is not None:
            mission_context = context_table[context_table.lid==entry.mission_lid]
            mission_descr = mission_context.mission_desc.squeeze()
            entry['mission_description'] = mission_descr.replace('\n', '')
        else:
            entry['mission_description'] = 'Mission description not found'
            log.warning('mission description not found for LID: {:s}'.format(entry.mission_lid))
        collection_cols.append('mission_description')
        collection_cols.remove('mission_lid')

        entry[collection_cols].to_frame().to_html(out_file, na_rep='')  
            # formatters={'start': timeformatter, 'stop': timeformatter})
        log.info('generated collection summary {:s}'.format(out_name))

    
def deletion_request(input_file, output_dir='.', delete_browse=True):
    """Accepts an input file generated from the PSA (<=6) table export
    function and generates a PSA deletion request"""

    import datetime
    import tarfile

    products = pd.read_table(input_file, delimiter=',', header=0)
    bundles = products['Dataset Identifier'].unique()
    for bundle in bundles:
        bundle_name = bundle.split(':')[-1]
        request_time = datetime.datetime.now()
        deletion_name = 'bcpsa-pds4-pd-01-{:s}-{:s}'.format(bundle_name, request_time.strftime('%Y%m%dT%H%M%S'))
        outfile = os.path.join(output_dir, deletion_name + '.tab')
        products[['LID','Version']].to_csv(outfile, sep='\t', index=False, header=False)
        tarball = os.path.join(output_dir, deletion_name + '.tar.gz')
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(outfile, arcname=deletion_name + '.tab', recursive=False)

    # For MCAM we need to delete browse also.
    # We have LID like:
    # urn:esa:psa:bc_mtm_mcam:data_raw:cam_raw_sc_cam3_image_20210810t232126_48_f__t0010
    #
    # And browse like:
    # urn:esa:psa:bc_mtm_mcam:browse:cam_raw_sc_cam3_browse_20210810t232126_48_f__t0010