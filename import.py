import argparse, sys, os, re, getpass, psycopg2, importlib.util, csv, pprint

from shutil import copyfileobj
from itertools import zip_longest
from collections import OrderedDict

class Password(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass()

        setattr(namespace, self.dest, values)

parser = argparse.ArgumentParser(
    description="Check a data in_file against all known formats, "
                "prompt the user to choose between matches, then "
                "create a table in PostgreSQL and import the file to it."
)
parser.add_argument(
    'in_filenames',
    metavar="FN",
    nargs="*",
    help="The path(s) to the fixed width data file(s)",
)
parser.add_argument(
    '-sd'
    '--source-directory',
    action="store",
    help="The path to a directory containing nothing but files to be imported.",
    dest="in_directory",
)
parser.add_argument(
    '-ed'
    '--error-directory',
    action="store",
    help="If the in-file contains any rows that throw errors, this utility "
         "will parse as many columns as possible for each of those rows, and "
         "save a file containing a mix of valid data and error messages, "
         "named after the in-file, in this directory. Defaults to PWD.",
    dest="error_directory",
)
parser.add_argument(
    '-dp',
    '--depth',
    action="store",
    type=int,
    help="If --in-directory was provided, also import files from sub-directories up to this depth.",
    dest="depth",
)
parser.add_argument(
    '-t',
    '--table-name',
    help="the name of the table created to store your data, defaults to the name of the in_file. "
         "Note that if more than one in-file is present, setting this flag will cause all data "
         "to be stored in one table.",
    action="store",
    dest="table_name",
)
parser.add_argument(
    '-T',
    '--test',
    help="Only parse the first 10,000 rows of each file, and don't write to the database. Make sure "
         "to include an error-directory when using this flag.",
    action="store_true",
    dest="test",
)
parser.add_argument(
    '-db',
    '--dbname',
    help="the name of the database we're connecting to. Passed directly to psycopg2.connect().",
    action="store",
    dest="dbname",
)
parser.add_argument(
    '-u',
    '--user',
    help="The user (or role) that own the PostgreSQL database. Passed directly to psycopg2.connect().",
    action="store",
    dest="user",
)
parser.add_argument(
    '-pw',
    '--password',
    help="Password for PostgreSQL. Passed directly to psycopg2.connect(). You can give it as an "
         "argument after the flag, or leave the flag by itself and you'll be prompted to enter a password.",
    action="store",
    dest="password",
)
parser.add_argument(
    '-o',
    '--host',
    help="Host for PostgreSQL. Passed directly to psycopg2.connect().",
    action="store",
    dest="host",
)
parser.add_argument(
    '-p',
    '--port',
    help="Port for PostgreSQL. Passed directly to psycopg2.connect().",
    action="store",
    dest="port",
)
parser.add_argument(
    '-l',
    '--library',
    help="Location of the data_fixin library. Allows running of this script without adding it to the system "
         "PYTHON_PATH. Defaults to the folder containing this script.",
    action="store",
    dest="library_path",
)
parser.add_argument(
    '-c',
    '--config',
    help="Bypass searching and choose a specific config file{s). "
         "If exactly one is specified, it will be used for all."
         "If more than one is specified, they'll be matched to the filenames in order,"
         "then any remaining filenames will use the regular search proceedure.",
    nargs="*",
    action="store",
    dest="configs",
)
parser.add_argument(
    '-d',
    '--delimiter',
    help="If your file is a CSV, specify the delimiter",
    action="store",
    dest="delimiter",
)
parser.add_argument(
    '-i',
    '--inspect',
    help="For each file, parse and print the first row as a dict before importing. "
         "Allow the user to cancel or continue at that point.",
    action="store_true",
    dest="inspect",
)
parser.add_argument(
    '--build-csv-config',
    help="Interactive mode for building a CSV config. Will ask you how to parse each column, "
         "and save the table declaration that arises if the import is successful.",
    action="store_true",
    dest="build_csv_config",
)
parser.add_argument(
    '-cs',
    '--config-spec',
    help="Print the spec for config files then exit",
    action="store_true",
    dest="print_config_spec",
)

config_spec = """
Config files for this program are JSON files with a few required keys.

    "ORIGIN":
        where this data you're trying to import came from
        for all our current data, this is PopData BC
    "SHORT_NAME":
        the shortest unique name possible
        include the year range
    "DESCRIPTION":
        a longer description
        be as descriptive as you can about what this data is
    "CONFIG":
        the meat of the file, with its own prescribed format
        (https://github.com/ShawnMilo/fixedwidth)
        "<column_name>":
            "required":
                whether this column is allowed to be blank
                governs whether the database column will have be nullable or not
                False by default
            "type": (required)
                may  be text, int, numeric, boolean, time, timestamp, interval, or date
                these are PostgreSQL types, though not all types are supported
                additional python validation is also done based on this type
            "format":
                required when "type" is date, time, timestamp or inteval, unused otherwise
                a format string as used by python's datetime.strptime()
                see https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
                interval format only supports %d, %H, %M, %S, %f
            "null_values":
                if input matches one of these, the field will be parsed as None
            "true_values":
                boolean only column
                if input matches one of these, the field will be parsed as True
            "false_values":
                boolean only column
                if input matches one of these, the field will be parsed as False
            "start_pos": (required)
                the character count where the column value starts
            "end_pos": (required)
                the character count where the column value endswith
            "default":
                if field is not required, the default value
                will be added as a default on the PostgreSQL column
"""

if __name__ == "__main__":
    args = parser.parse_args()

    if args.print_config_spec:
        print(config_spec)
        sys.exit()

    library_path = args.library_path or os.path.split(__file__)[0]
    if library_path not in sys.path:
        sys.path.append(library_path)
    from data_fixin import import_fixed_width, import_csv

    if not args.in_filenames and not args.in_directory:
        parser.error("No file or directory supplied")

    filenames = args.in_filenames or []

    depth = args.depth or 1
    if args.in_directory:
        num_sep = args.in_directory.rstrip(os.path.sep).count(os.path.sep)
        for i, (path, dirs, files) in enumerate(os.walk(args.in_directory)):
            if num_sep + depth <= path.count(os.path.sep):
                continue
            filenames += [os.path.join(path, file) for file in sorted(files)]

    matches = OrderedDict()

    if args.configs:
        from data_fixin.fixed_width_configs import CONFIGS, config_dir

        args.configs = [
            os.path.join(config_dir, path) if not os.path.isabs(path)
            else path
            for path in args.configs
        ]

        try:
            chosen_configs = [
                CONFIGS[config_filename]
                for config_filename in args.configs
            ]
        except KeyError as e:
            raise ValueError("{config_name} is not a valid config file".format(config_name=str(e)))

        if len(chosen_configs) == 1:
            matches = OrderedDict([(filename, chosen_configs[0]) for filename in filenames])
            filenames = []
        else:
            matches = OrderedDict([
                (filename, config)
                for filename, config in zip(filenames, chosen_configs)
            ])
            filenames = filenames[len(chosen_configs):]

    for in_filename in filenames:
        with open(in_filename, 'r') as f:
            first_row = next(f)

        matching_fixed_width_configs = import_fixed_width.get_matching_configs(first_row)
        matching_csv_configs = import_csv.get_matching_configs(first_row, args.delimiter)

        matching_configs = matching_fixed_width_configs + matching_csv_configs

        # define text template for displaying a config to the user
        config_description_template = (
            "\n\t{SHORT_NAME} ({CONFIG_TYPE})\n"
            "\tOrigin: {ORIGIN}\n"
            "\t{DESCRIPTION}\n"
        )

        if not matching_configs:
            print("No config files match {in_filename}, you'll have to make your own.")
            print(config_spec.format(in_filename=in_filename))
            print("Good luck.")
            matches[in_filename] = None
            proceed = input("Continue to next file (y|n)?\n").lower()
            if proceed not in ['y', 'yes']:
                sys.exit()
        elif len(matching_configs) > 1:

            config_names = []
            config_descriptions = []
            for config in matching_configs:
                config_names.append(config['SHORT_NAME'])
                config_descriptions.append(config_description_template.format(**config))

            choose_message = (
                "Multiple configs match {in_filename}. They are as follows:\n"
                "{config_description_templates}"
                "\nWhich would you like to use?\n"
                "({input_choices})\n".format(
                    in_filename=in_filename,
                    config_description_templates='\n'.join(config_descriptions),
                    input_choices='|'.join(config_names)
                )
            )

            def clean_index(index):
                try:
                    return int(index)
                except (TypeError, ValueError):
                    return index

            index = None
            while index not in matching_configs:
                index = clean_index(input(choose_message))

                if index == "q":
                    sys.exit()

            matches[in_filename] = matching_configs[index]

        elif len(matching_configs) == 1:
            config = matching_configs[0]
            proceed = input(
                "\nFound one config matching {in_filename}, described as follows:\n"
                "{config_description_template}"
                "\nProceed (y|n)?\n".format(
                    in_filename=in_filename,
                    config_description_template=config_description_template.format(**config),
                )
            )
            matches[in_filename] = config
            if proceed.lower() not in ['y', 'yes']:
                sys.exit()

    print("All files matched to configs or skipped, beginning import.")

    # TODO: once we've defined synthetic columns, show the users which ones are available for their data

    for in_filename, config in matches.items():
        if not config:
            continue

        if args.inspect:
            print("Data from first line of {in_filename} is as follows:\n".format(in_filename=in_filename))

            with open(in_filename) as f:
                first_line = next(f)

            fixed_width_parser = import_fixed_width.FixedWidth(config['CONFIG'])
            fixed_width_parser.line = first_line,
            printer = pprint.PrettyPrinter()
            printer.pprint(fixed_width_parser.data)

            proceed = input("Continue, skip, or quit (c|s|q)?\n").lower()
            if proceed in ['c', 'continue']:
                pass
            elif proceed in ['s', 'skip']:
                continue
            else:
                sys.exit()

        table_name = args.table_name or os.path.splitext(os.path.basename(in_filename))[0]
        # strip out invalid characters. Replace with underscores because the invalid characters
        # will most commonly be ' ', '-', or '.'
        table_name = re.sub('[^0-9a-zA-Z]+', '_', table_name,)

        # note args.x is None if the user omitted the argument, so we'll get the pscopg2 default in that case
        connection = psycopg2.connect(
            dsn='',
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
        )

        try:
            if not args.test:
                import_fixed_width.create_table_from_config(
                    table_name,
                    config,
                    connection,
                )

            if config['CONFIG_TYPE'] == 'fixed width':
                with open(in_filename) as in_file:
                    head = None
                    if args.test:
                        head = []
                        for i in range(10000):
                            try:
                                head.append(next(in_file))
                            except StopIteration:
                                break

                    psql_copy_stream, error_stream = import_fixed_width.import_fixed_width(
                        in_file=head or in_file,
                        table_name=table_name,
                        config=config,

                        prefix="Reading {0}".format(os.path.basename(in_filename)),
                    )
            else:
                error_stream = import_csv.import_csv(
                    in_filename=in_filename,
                    table_name=table_name,
                    config=config,
                    delimiter=args.delimiter,

                    prefix="Reading {0}".format(os.path.basename(in_filename)),
                )

            if not args.test:
                import_fixed_width.psql_copy(
                    psql_copy_stream,
                    table_name,
                    connection,
                    args.error_directory or os.path.dirname(__file__),
                )

            connection.commit()

        finally:
            connection.close()

        if error_stream.tell() > 0:
            root, ext = os.path.splitext(os.path.basename(in_filename))
            error_basename = root + '_error' + ext
            error_filename = os.path.join(
                args.error_directory or os.path.dirname(__file__),
                error_basename
            )

            with open(error_filename, 'w', newline='') as error_file:
                error_stream.seek(0)
                error_file.write(config["FILE_PATH"] + '\n')
                error_file.write(table_name + '\n')
                copyfileobj(error_stream, error_file)
