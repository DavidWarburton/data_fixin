import re, argparse, psycopg2, os, sys, csv, json

from io import StringIO
from collections import OrderedDict
from cmd import Cmd

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
    help="The path(s) to the error file(s)",
)
parser.add_argument(
    '-sd'
    '--source-directory',
    action="store",
    help="The path to a directory containing nothing but error files to be fixed.",
    dest="in_directory",
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

class FixErrors(Cmd):

    def __init__(self, *args, config, lines, table_name, connection, **kwargs):
        self.table_name = table_name
        self.connection = connection
        self.cur_field = None
        self.cur_errors = []
        self.lines = lines
        self.solutions = {}

        self.config = config

        self.compile()

        super(FixErrors, self).__init__(*args, **kwargs)

    def compile(self):
        self.parser = import_fixed_width.FixedWidth(self.config['CONFIG'])

        self.corrected_stream = StringIO()
        psql_copy_writer = csv.writer(self.corrected_stream)

        self.unique_errors = {}
        for line in self.lines:
            data, errors = self.parser.parse_line(line)

            psql_copy_writer.writerow(self.parser.get_psql_copy_values(data))

            for field_name, error in errors.items():
                self.unique_errors.setdefault(field_name, {})
                self.unique_errors[field_name].setdefault(error['error'], [])
                self.unique_errors[field_name][error['error']].append(error['value'])

    def do_display(self, _arg):
        print(self.unique_errors.keys())

    def do_select(self, field_name):
        if field_name in self.parser.ordered_fields:
            self.cur_field = field_name
            print(self.unique_errors.get(self.cur_field).keys())
        else:
            print("Not a valid field")

    def do_fix(self, raw_pattern, fix_str):
        """
        Enters a new value for errors matching the given RegEx.
        """

        if self.cur_field is None:
            print("Select a field first")
            return True

        try:
            pattern = re.compile(raw_pattern)
        except re.error:
            print("Not a valid RegEx")
            return True

        self.config['CONFIG'][self.cur_field].setdefault('solutions', OrderedDict())
        self.config['CONFIG'][self.cur_field]['solutions'][raw_pattern] = fix_str

        self.compile()

        for error, values in self.unique_errors[self.cur_field].items():
            for value in values:
                if pattern.fulmatch(value):
                    try:
                        self.parser.ordered_fields[self.cur_field].parse(value)
                    except ValueError as e:
                        print(
                            "{error} occurred for {value}. Proceed (Y|n)?".format(
                                error=e,
                                value=value,
                            )
                        )

                        proceed = input()
                        if proceed.lower() not in {'y', 'yes'}:
                            break
        else:
            self.solutions[self.cur_field][raw_pattern] = fix_str


    def do_commit(self, _arg):
        """
        Takes all our solutions, applies them to our data and saves that data to PostgreSQL.
        Also saves our solutions and back to the config.
        """

        for field, solution_dict in self.solutions.items():
            self.config['CONFIG'][field]['solutions'].setdefault({})
            for pattern, solution in solution_dict.items():
                self.config['CONFIG'][field]['solutions'][pattern] = solution

        self.compile()

        if self.unique_errors:
            print("There are still errors. Proceed (Y|n)?")

            proceed = input()
            if proceed.lower() not in {'y', 'yes'}:
                return False

        psql_copy(self.corrected_stream, self.table_name, self.connection)

        with open(self.config['CONFIG_FILEPATH'], 'w') as config_file:
            config_file.write(json.dumps(self.config))

        return True

    def do_quit(self, _arg):
        sys.exit()

    do_EOF = do_quit # allows user to quit with ctrl-C

if __name__ == "__main__":
    args = parser.parse_args()

    library_path = args.library_path or os.path.split(__file__)[0]
    if library_path not in sys.path:
        sys.path.append(library_path)
    from data_fixin import import_fixed_width, import_csv
    from data_fixin.fixed_width_configs import CONFIGS

    filenames = args.in_filenames or []

    depth = args.depth or 1
    if args.in_directory:
        for i, (path, dirs, files) in enumerate(os.walk(args.in_directory)):
            filenames += [os.path.join(path, file) for file in files]
            if i == depth - 1:
                break

    connection = psycopg2.connect(
        dsn='',
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
    )

    for filename in filenames:
        with open(filename) as file:

            config_filename = next(file).strip('\n')
            table_name = next(file).strip('\n')

            fix_errors_cmd = FixErrors(
                config=CONFIGS[config_filename],
                table_name=table_name,
                lines=[line for line in file],
                connection=connection,
            )

            fix_errors_cmd.cmdloop()
