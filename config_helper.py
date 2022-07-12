import argparse, csv, json, os, re, sys

from collections import OrderedDict

parser = argparse.ArgumentParser(
    description="Import a standard fomat data dictionary and produce"
                "a JSON config file for use by data_fixin."
)

parser.add_argument('in_filename', help="The path to data dictionary",)
parser.add_argument(
    '-o',
    '--origin',
    action="store",
    help="Fills the origin string in the config."
)
parser.add_argument(
    '-n',
    '--short-name',
    action="store",
    help="Fills the short name string in the config."
)
parser.add_argument(
    '-d',
    '--description',
    action="store",
    help="Fills the description string in the config."
)
parser.add_argument(
    '-f',
    '--filename',
    action="store",
    help="The what name to save the config under."
)
parser.add_argument(
    '-l',
    '--library',
    help="Location of the data_fixin library. Allows running of this script without adding it to the system "
         "PYTHON_PATH. Defaults to the folder containing this script.",
    action="store",
    dest="library_path",
)

if __name__ == "__main__":

    args = parser.parse_args()

    library_path = args.library_path or os.path.split(__file__)[0]
    if library_path not in sys.path:
        sys.path.append(library_path)

    out_json = OrderedDict()
    out_json['ORIGIN'] = args.origin
    out_json['SHORT_NAME'] = args.short_name
    out_json['DESCRIPTION'] = args.description
    out_json['CONFIG'] = OrderedDict()

    with open(args.in_filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            column_name = re.sub('[^0-9a-zA-Z]+', '_', row['variable_name'],)
            out_json['CONFIG'][column_name] = OrderedDict(
                type=row['type'],
                start_pos=int(row['start']),
                end_pos=int(row['end']),
                length=int(row['end']) - int(row['start']),
            )
            if row['format']:
                out_json['CONFIG'][column_name]['format'] = row['format']

            try:
                bool_true = json.loads(row['bool_true'])
            except:
                pass
            else:
                if bool_true:
                    out_json['CONFIG'][column_name]['true_values'] = bool_true

            try:
                bool_false = json.loads(row['bool_false'])
            except:
                pass
            else:
                if bool_false:
                    out_json['CONFIG'][column_name]['false_values'] = bool_false

            try:
                bool_null = json.loads(row['bool_null'])
            except:
                pass
            else:
                if bool_null:
                    out_json['CONFIG'][column_name]['null_values'] = bool_null

    out_name = args.filename or 'temp_config_name.json'
    from data_fixin.fixed_width_configs import config_dir
    with open(os.path.join(config_dir, out_name), 'w') as f:
        f.write(json.dumps(out_json, indent=4))
