import argparse, sys, os, re, getpass, psycopg2, importlib.util, csv, pprint

library_path = os.path.split(__file__)[0]
if library_path not in sys.path:
    sys.path.append(library_path)
from data_fixin import import_fixed_width, import_csv

from shutil import copyfileobj
from itertools import zip_longest
from collections import OrderedDict

if __name__ == "__main__":
    from data_fixin.fixed_width_configs import CONFIGS, config_dir

    conn = psycopg2.connect(database="db-19-g01", host="gaboury.popdata.bc.ca")
    cur = conn.cursor()

    for config in CONFIGS.values():
        for column_name, column_attrs in config['CONFIG'].items():
            cur.execute("UPDATE pg_attribute SET atttypmod = '{}' WHERE attname = '{}'".format(column_attrs['end_pos'] - column_attrs['start_pos'] + 5, column_name))

    conn.commit()
    conn.close()
