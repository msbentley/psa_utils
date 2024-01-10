#!/usr/bin/python
"""internal.py

Mark S. Bentley (mark@lunartech.org), 2019

A module to of utility functions likely only to be of interest to
PSA internal members (archive scientists etc.)

"""

from socket import TCP_NODELAY
from tarfile import HeaderError
from . import common
from . import packager
from . import tap
import pathlib
import logging
import datetime
import tarfile
import copy
from lxml import etree
from lxml import html
import json
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

    def __init__(self, config_file='ingestion_test.yml', template_label='test_product.xml', output_dir='.', package=False, lblx=False):

        self.lblx=lblx

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
            suffix = '.lblx' if self.lblx else '.xml'
            packager.Packager(input_dir=output_dir,  output_dir=output_dir, products='{:s}*{:s}'.format(prefix, suffix))


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

            # product ID: {shortname}_{level}_sc_{sub_instr}_test_
            if sub_instr is None:
                product_id = '{:s}_{:s}_sc_test'.format(self.config[bundle]['shortname'], level)
            else:
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

            if len(bundle.split('_'))==3:
                mission, host, instrument = bundle.split('_')
            elif len(bundle.split('_'))==2:
                mission, instrument = bundle.split('_')
                host = mission
            else:
                log.error('invalid bundle name')
                return None
            
            today = datetime.datetime.today()
            



            # update the template with the mission/instrument-relevant values

            root.xpath('//pds:Identification_Area/pds:logical_identifier', namespaces=self.ns)[0].text = 'urn:esa:psa:{:s}:data_{:s}:{:s}'.format(bundle, proc_levels[level][1], product_id)
            root.xpath('//pds:Identification_Area/pds:version_id', namespaces=self.ns)[0].text = '1.{:d}{:02d}'.format(today.year, today.month)
            root.xpath('//pds:Identification_Area/pds:Modification_History/pds:Modification_Detail/pds:modification_date', namespaces=self.ns)[0].text =today.strftime('%Y-%m-%d')
            root.xpath("//pds:Observing_System_Component[pds:type='Instrument']/pds:Internal_Reference/pds:lid_reference", namespaces=self.ns)[0].text = 'urn:esa:psa:context:instrument:{:s}.{:s}'.format(host, instrument)
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

            suffix = '.lblx' if self.lblx else '.xml'
            label_out = os.path.join(output_dir, product_id + suffix)
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
        
        if type(prod_type) == float:
            if pd.isna(prod_type):
                prod_type = ['N/A']

        # either way validate expects a list for type, even if it only has one value
        if type(prod_type) != list:
            t = [prod_type]
        else:
            t = prod_type

        # the json needs name, type, and lidvid
        c['type'] = t
        c['name'] = ['N/A'] if product['name'] is None else [product['name']]
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


def collection_summary(config_file, input_dir='.', output_dir=None, context_dir='.'):
    """
    collection_summary accesses meta-data in a collection label
    or referenced from it, to produce a set of summary information
    needed to register a DOI and/or create a Google Dataset
    Search landing page.

    If output_dir = None then a DataFrame is returned with all of the scraped
    information, otherwise an html file is produced.

    """

    from pds4_utils import dbase

    collection_db = dbase.Database(
        files='collection_data*.xml', 
        config_file=config_file, 
        directory=input_dir, 
        recursive=False)

    collection_table = collection_db.get_table('collection')

    # strip carriage returns from the description
    # collection_table.description = collection_table.description.apply(lambda desc: desc.replace('\n', ' '))

    # make a string list of keywords (if any)
    if 'keywords' in collection_table.columns:
        collection_table.keywords = collection_table.keywords.apply(lambda key: ', '.join(key))

    context_db = dbase.Database(
        files='*.xml', 
        config_file=config_file, 
        directory=context_dir, 
        recursive=True)

    context_table = context_db.get_table('context')
    # timeformatter = lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S.%f')

    meta = []

    for idx, entry in collection_table.iterrows():

        collection_cols = collection_db.dbase['collection'].columns.to_list()
        collection_cols.extend(['lid', 'bundle', 'collection', 'vid'])

        # tidy up some specific entries
        if entry.mission_lid is not None:
            mission_context = context_table[context_table.lid==entry.mission_lid]
            mission_descr = mission_context.mission_desc.squeeze()
            entry['mission_description'] = mission_descr
        else:
            entry['mission_description'] = 'Mission description not found'
            log.warning('mission description not found for LID: {:s}'.format(entry.mission_lid))

        # clean up tabs, carriage returns and whitespace!!
        # entry.description = entry.description.replace('\n', '')
        #entry.description = entry.description.replace('\t', '').strip()
        collection_cols.append('mission_description')
        collection_cols.remove('mission_lid')
        if output_dir is None:
            meta.append(entry[collection_cols])
        else:
            out_name = entry.bundle+'_'+entry.collection+'.html'
            out_file = os.path.join(output_dir, out_name)
            entry[collection_cols].to_frame().to_html(out_file, na_rep='')  
                # formatters={'start': timeformatter, 'stop': timeformatter})
        
        log.info('generated collection summary {:s}'.format(entry.lid))
        print('generated collection summary {:s}'.format(entry.lid))

    if output_dir is None:
        return meta


def doi_landing(config_file, template_file, input_dir='.', output_dir='.', context_dir='.'):
    """DOI landing page generation using a marked-up version of the html template"""

    import re

    # map bundles to directories
    browse_root = 'https://archives.esac.esa.int/psa/ftp/'
    mission_paths = {
        'em16': 'ExoMars2016',
        'bc': 'BepiColombo',
        'juice': 'JUICE'
    }

    mission_logos = {
        'em16': 'EXOMARS_logo.png',
        'bc': 'BEPICOLOMBO_logo.png',
        'juice': 'JUICE_logo.png'
    }

    split_threshold = 2000 # characters

    # read the collection meta-data
    pages = collection_summary(config_file, input_dir, output_dir=None, context_dir=context_dir)

    # open the customised template html, read, and close it
    f = open(template_file, 'r')
    template = f.read()
    f.close()

    # use a regex to find all of the tags matching the form ${some_text}
    tags = re.findall('\${.*?}', template)

    # get the tag names from this (i.e. some_text)
    tag_names = [tag.strip('${}') for tag in tags]
    tag_names = set(tag_names)

    today = datetime.datetime.today().strftime('%d/%m/%Y')

    for page in pages:

        template_temp = template

        # create a dictionary of values expected
        vals = {}
        vals['name'] = page['name']
        vals['coverage'] = '{:s} - {:s}'.format(page.start, page.stop)
        vals['date'] = today
        vals['description'] = page['description'].replace('\n', '<div>')
        vals['instrument'] = page['instrument']
        vals['mission'] = page['mission']
        mission_description = page['mission_description'].replace('\n', '<div>')
        if len(mission_description)>split_threshold:
            vals['mission_description_1'] = mission_description[0:split_threshold]
            vals['mission_description_2'] = mission_description[split_threshold:]
        else:
            vals['mission_description_1'] = mission_description
            vals['mission_description_2'] = ''
        mission_id = page['lid'].split(':')[3].split('_')[0]
        vals['url'] = browse_root + '/' + mission_paths[mission_id] + '/' + page['bundle'] + '/' + page['collection']
        vals['altname'] = '{:s} {:s}'.format(page.mission, page.instrument)
        vals['doi'] = '10.5270/esa-xxxxxxx'
        vals['version'] = page.vid
        vals['author'] = page.author_list
        vals['logo'] = mission_logos[mission_id]

        # substitute the tags
        for tag in tag_names:
            template_temp = template_temp.replace('${{{:s}}}'.format(tag), vals[tag])

        output_name = page['name'] + '.html'
        outfile = os.path.join(output_dir, output_name)
        f = open(outfile, 'w')
        f.write(template_temp)
        f.close()




def doi_landing2(config_file, template_file, input_dir='.', output_dir='.', context_dir='.'):
    """DOI landing page generation using html editing - not used not due to the complexity
    of inserting html into html elements as text, and a lack of time to properly insert the
    needed elements"""

    # read the collection meta-data
    pages = collection_summary(config_file, input_dir, output_dir=None, context_dir=context_dir)

    # open and parse the html template
    template = html.parse(template_file)
    root = template.getroot()

    # extract the json GSD meta-data
    script = json.loads(template.xpath('//script')[0].text)

    today = datetime.datetime.today().strftime("%d/%m/%Y")

    for page in pages:

        # update GSD meta-data
        script['name'] = page['name']
        script['dateModified'] = today
        script['description'] = page['description']
        script['keywords'] = page.keywords.split(',')
        script['alternateName'] = page.lid
        script['temporalCoverage'] = '{:s} - {:s}'.format(page.start, page.stop)

        # write it back to the template
        template.xpath('//script')[0].text = json.dumps(script)

        # find all table rows in the template
        rows = root.findall('.//tr')
        for row in rows:
            name = row.find('.//td[1]').text

            if name=='Name':
                row.find('.//td[2]').text = page['lid']
            elif name=='Title':
                row.find('.//td[2]').text = page['name']
            elif name=='Author':
                row.find('.//td[2]').text = page['author_list']
            elif name=='Description':
                row.find('.//td[2]').text = page['description'].replace('\t','')
            elif name=='Mission Description':
                row.find('.//td[2]').text = page['mission_description'].replace('\t','')
            elif name=='Mission Instrument':
                row.find('.//td[2]').text = page['instrument']
            elif name=='Date Published':
                row.find('.//td[2]').text = today
            elif name=='Instrument':
                row.find('.//td[2]').text = page['instrument']
            elif name=='Temporal Coverage':
                row.find('.//td[2]').text = script['temporalCoverage']
            else:
                log.info('skipping landing page row {:s}'.format(name))

            # write it back
        html.open_in_browser(template)
            
        template.write('test.html', method='html')
        print(html.tostring(template, pretty_print=True))



    
def deletion_request_csv(input_file, output_dir='.', delete_browse=True):
    """Accepts an input file generated from the PSA (<=6) table export
    function and generates a PSA deletion request"""

    products = pd.read_table(input_file, delimiter=',', header=0, dtype={'Version': str})
    bundles = products['Dataset Identifier'].unique()
    for bundle in bundles:
        bundle_name = bundle.split(':')[-1]
        request_time = datetime.datetime.now()
        mission_name = bundle_name.split('_')[0]
        deletion_name = '{:s}psa-pds4-pd-01-{:s}-{:s}'.format(mission_name, bundle_name, request_time.strftime('%Y%m%dT%H%M%S'))
        outfile = os.path.join(output_dir, deletion_name + '.tab')
        product_list = products[['LID','Version']].drop_duplicates(keep='first')
        product_list.to_csv(outfile, sep='\t', index=False, header=False)
        tarball = os.path.join(output_dir, deletion_name + '.tar.gz')
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(outfile, arcname=deletion_name + '.tab', recursive=False)

    # For MCAM we need to delete browse also.
    # We have LID like:
    # urn:esa:psa:bc_mtm_mcam:data_raw:cam_raw_sc_cam3_image_20210810t232126_48_f__t0010
    #
    # And browse like:
    # urn:esa:psa:bc_mtm_mcam:browse:cam_raw_sc_cam3_browse_20210810t232126_48_f__t0010


def deletion_request_tap(query, dryrun=True, output_dir='.',
    tap_url='https://archives.esac.esa.int/psa-tap/tap', proxy=None):
    """
        Accepts either a single query (ADQL string) or a list of strings matching LIDs to delete.
        Check_aux passes the query also to the auxiliary product table. Start and stop time will
        limit the search by time (does not work for all aux products.

        When dryrun=True no deletion request will be made, but a list of matching products will
        be displayed.
    """

    if isinstance(query, str):
        query = [query]
    elif isinstance(query, list):
        pass
    else:
        log.error('query has to be either a string, or a list of strings')
        return None
        
    t = tap.PsaTap(tap_url=tap_url, proxy=proxy)
    results = []
    for q in query:
        try:
            results.append(t.query(q))
        except:
            log.error('query error')
            return None

    if all(v is None for v in results):
        log.warning('no matches found - no deletion request generated')
        return None
    
    results = pd.concat(results)
    results['bundle'] = results.logical_identifier.apply(lambda lid: lid.split(':')[3])

    bundles = results.bundle.unique()
    if len(bundles)>1:
        log.error('deletions from more than one bundle not allowed - make separate queries')
        return None

    bundle = bundles[0]

    bundle_name = bundle.split(':')[-1]
    request_time = datetime.datetime.now()
    mission_name = bundle_name.split('_')[0]
    deletion_name = '{:s}psa-pds4-pd-01-{:s}-{:s}'.format(mission_name, bundle_name, request_time.strftime('%Y%m%dT%H%M%S'))
    outfile = os.path.join(output_dir, deletion_name + '.tab')
    product_list = results[['logical_identifier','version_id']].drop_duplicates(keep='first')

    if dryrun:
        log.info('this request would delete {:d} products'.format(len(product_list)))
        print(product_list)
        return
    else:
        product_list.to_csv(outfile, sep='\t', index=False, header=False)
        tarball = os.path.join(output_dir, deletion_name + '.tar.gz')
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(outfile, arcname=deletion_name + '.tab', recursive=False)

    return 



def show_archive_status(mission, instrument=None):
    """
    Plots a bar chart showing the availability of data for a given mission or instrument.

    Data which are public, on rolling release, or on hold will be coloured differently.
    """
    from . import tap
    import matplotlib.pyplot as plt
    import matplotlib.dates as md
    import matplotlib.patches as mpatches

    log.debug('Querying PSA for mission list')
    missions = tap.get_missions()
    if mission not in missions:
        log.error('mission {:s} not found - choices are {:s}'.format(mission, ', '.join(missions)))
        return None

    log.debug('Querying PSA for instrument list')
    instruments = tap.get_instruments(mission=mission)

    if instrument is not None:
        if instrument not in instruments:
            log.error('instrument {:s} not found - choices are {:s}'.format(instrument, ', '.join(instruments)))
            return None
        else:
            instruments = [instrument]

    # mapping of EPN-TAP processing_level to PDS4:
    tap_levels = {
        0: 'Partially processed', # hack since these are set to null in the db (pp doesn't map 1:1 to EPN-TAP)
        1: 'Telemetry',
        2: 'Raw',
        3: 'Calibrated',
        5: 'Derived'
    }

    psa = tap.PsaTap()
    date_format = '%Y-%m-%d %H:%M:%S'
    now = pd.Timestamp.now()
    now_str = now.strftime(date_format)
    min_date = now
    max_date = now

    # Logic is as follows:
    # 1. products with release_date <= today are public - green
    # 2. products with release_date > today but < 2098 are ready for release
    # 3. products with release_date > 2098 are private

    for instr in instruments:

        log.info('Building summary for {:s}'.format(instr))

        log.debug('Querying processing levels available for {:s}'.format(instr))
        levels = psa.query("select distinct processing_level from epn_core where instrument_name='{:s}'".format(instr))
        levels = sorted(levels.processing_level.to_list())

        legend = {}
        fig, ax = plt.subplots()

        for idx, level in enumerate(levels):

            log.info('Processing level: {:d} ({:s})'.format(level, tap_levels[level]))

            start = []
            duration = []
            colours = []

            if level == 0:
                public = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level is null and release_date<='{:s}' and instrument_name='{:s}'".format(now_str, instr))
            else:
                public = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level={:d} and release_date<='{:s}' and instrument_name='{:s}'".format(level, now_str, instr))
            if not public.empty:
                public_start = public.time_min.squeeze()
                min_date = min(min_date, public_start)
                public_stop = public.time_max.squeeze()
                max_date = max(max_date, public_stop)
                public_dur = public_stop-public_start
                start.append(public_start)
                duration.append(public_dur)
                legend['Public'] = 'green'
                colours.append('green')

                log.debug('{:s} public data range {:s} - {:s}'.format(tap_levels[level], public_start.strftime(date_format), public_stop.strftime(date_format)))
            else:
                log.warning('no public data found for {:s}'.format(instr))

            if level == 0:
                ready = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level is null and release_date>'{:s}' and release_date<'2098-01-01' and instrument_name='{:s}'".format(now_str, instr))
            else:
                ready = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level={:d} and release_date>'{:s}' and release_date<'2098-01-01' and instrument_name='{:s}'".format(level, now_str, instr))
            if not ready.empty:
                ready_start = ready.time_min.squeeze()
                min_date = min(min_date, ready_start)
                ready_stop = ready.time_max.squeeze()
                max_date = max(max_date, ready_stop)
                ready_dur = ready_stop-ready_start
                start.append(ready_start)
                duration.append(ready_dur)
                legend['Ready-to-release'] = 'orange'
                colours.append('orange')
                log.debug('{:s} release-ready data range {:s} - {:s}'.format(tap_levels[level], ready_start.strftime(date_format), ready_stop.strftime(date_format)))
            else:
                log.warning('no ready-to-release data found for {:s}'.format(instr))

            if level == 0:
                on_hold = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level is null and release_date>'2098-01-01' and instrument_name='{:s}'".format(instr))
            else:
                on_hold = psa.query("select min(time_min) as time_min, max(time_max) as time_max from epn_core where processing_level={:d} and release_date>'2098-01-01' and instrument_name='{:s}'".format(level, instr))
            if not on_hold.empty:
                on_hold_start = on_hold.time_min.squeeze()
                min_date = min(min_date, on_hold_start)
                on_hold_stop = on_hold.time_max.squeeze()
                max_date = max(max_date, on_hold_stop)
                on_hold_dur = on_hold_stop-on_hold_start
                start.append(on_hold_start)
                duration.append(on_hold_dur)
                legend['On hold'] = 'red'
                colours.append('red')
                log.debug('{:s} on-hold data range {:s} - {:s}'.format(tap_levels[level], on_hold_start.strftime(date_format), on_hold_stop.strftime(date_format)))
            else:
                log.warning('no private data found for {:s}'.format(instr))

            ax.broken_barh(list(zip(start, duration)), (idx, 0.9), facecolors=colours, alpha=0.5)

        log.debug('Plot range: {:s} - {:s}'.format(min_date.strftime(date_format), max_date.strftime(date_format)))
        ax.set_xlim(min_date, max_date)
        ax.set_ylim(0, len(levels))

        xfmt = md.DateFormatter('%Y-%m-%d')
        ax.xaxis.set_major_formatter(xfmt)
        ax.xaxis.grid(True)
        ax.axvline(x=now-pd.Timedelta(6*30, unit='d'), lw=3, c='black') # 6 bankers' months ;-)
        
        patches = []
        for entry in legend.keys():
            patches.append(mpatches.Patch(color=legend[entry], label=entry))
        leg = ax.legend(handles=patches, loc=0, fancybox=True)
        ypos = np.arange(len(levels))+0.5
        ax.set_yticks(ypos, [*map(tap_levels.get, levels)])
        fig.autofmt_xdate()
        ax.set_title('{:s} {:s} archive status as of {:s}'.format(mission, instr, now_str))
        plt.subplots_adjust(left=0.15)
        plt.tight_layout()
        plt.show()

    return