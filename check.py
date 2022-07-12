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
    '-sd',
    '--source-directory',
    action="store",
    help="The path to a directory containing nothing but files to be imported.",
    dest="in_directory",
)
parser.add_argument(
    '-ed',
    '--error-directory',
    action="store",
    help="The path to a directory containing error files.",
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
    '-E',
    '--exhaustive',
    help="Exhaustive mode tries to find the first line of each file in the table."
         "It takes a long time, but doesn't rely on assumptions about order of import.",
    action="store_true",
    dest="exhaustive",
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
    help="The config file used to parse these files. Supports exactly one.",
    action="store",
    dest="config",
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


def count_file_lines(filename):
    with open(filename) as in_file:
        first_line = next(in_file)
    return os.path.getsize(filename) // len(first_line)

def count_error_lines(filename):
    with open(filename) as in_file:
        return len(in_file.readlines()) - 2 # first two lines are meta data

if __name__ == "__main__":
    args = parser.parse_args()

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
    filenames = sorted(filenames)

    error_filenames = []

    if args.in_directory:
        num_sep = args.error_directory.rstrip(os.path.sep).count(os.path.sep)
        for i, (path, dirs, files) in enumerate(os.walk(args.error_directory)):
            if num_sep + 1 <= path.count(os.path.sep):
                continue
            error_filenames += [os.path.join(path, file) for file in sorted(files)]
    error_filenames = sorted(error_filenames)

    connection = psycopg2.connect(
        dsn='',
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
    )
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM {table}".format(table=args.table_name))

    count_in_table = cursor.fetchone()[0]

    if args.config:
        from data_fixin.fixed_width_configs import CONFIGS, config_dir

        parser = import_fixed_width.FixedWidth(
            CONFIGS[
                os.path.join(config_dir, args.config) if not os.path.isabs(args.config)
                else config
            ]['CONFIG']
        )

    count_in_files = 0
    error_count = 0
    ok_count = 0
    for filename in filenames:

        base_filename = os.path.splitext(os.path.basename(filename))[0]

        cur_file_count = count_file_lines(filename)

        for error_filename in error_filenames:
            if base_filename in error_filename:
                error_count += count_error_lines(error_filename)
                file_was_processed = True
                break
        else:
            if args.exhaustive:

                with open(filename) as f:
                    first_line = next(f)

                data, _errors = parser.parse_line(first_line)
                sql_statement = "SELECT COUNT(*) FROM {table} WHERE{data_conditions}".format(
                        table=args.table_name,
                        data_conditions=" AND".join([" {column} = %({column})s".format(column=column) for column in data.keys()])
                    )
                import pdb; pdb.set_trace()
                cursor.execute(sql_statement, data)
                if cursor.fetchone() == 1:
                    file_was_processed = True
                else:
                    file_was_processed = False
            else:
                file_was_processed = count_in_files + cur_file_count <= count_in_table + error_count

        if file_was_processed:
            count_in_files += cur_file_count

        print("{filename}: {file_was_processed}".format(
            filename=filename,
            file_was_processed=file_was_processed,
        ))
    connection.close()

    print("Files: {count}".format(count=count_in_files))
    print("Table: {count}".format(count=count_in_table))
    print("Errors: {count}".format(count=error_count))

    if count_in_files != count_in_table + error_count:
        print("File counts don't match the table count. Use --exhaustive to figure out which files are missing.")

