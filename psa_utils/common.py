#!/usr/bin/python
"""common.py

Mark S. Bentley (mark@lunartech.org), 2019

Common functions for the package

"""

pds_ns = 'http://pds.nasa.gov/pds4/pds/v1'
import logging
log = logging.getLogger(__name__)
import os

# def select_files(wildcard, directory='.', recursive=False):
#     """Create a file list from a directory and wildcard - recusively if
#     recursive=True"""

#     # recursive search
#     # result = [os.path.join(dp, f) for dp, dn, filenames in os.walk('.') for
#     # f in filenames if os.path.splitext(f)[1] == '.DAT']

#     if recursive:
#         selectfiles = locate(wildcard, directory)
#         filelist = [file for file in selectfiles]
#     else:
#         import glob
#         filelist = glob.glob(os.path.join(directory, wildcard))

#     filelist.sort()

#     return filelist


# def locate(pattern, root_path):
#     """Returns a generator using os.walk and fnmatch to recursively
#     match files with pattern under root_path"""

#     import fnmatch

#     for path, dirs, files in os.walk(os.path.abspath(root_path)):
#         for filename in fnmatch.filter(files, pattern):
#             yield os.path.join(path, filename)


def printtable(df, float_fmt=None):
    """Accepts a pd.DataFrame() prints a pretty-printed table, rendered with
    PrettyTable"""

    from prettytable import PrettyTable
    table = PrettyTable(list(df.columns))

    if float_fmt is not None:
        table.float_format = float_fmt

    for row in df.itertuples():
            table.add_row(row[1:])
    print(table)
    return
