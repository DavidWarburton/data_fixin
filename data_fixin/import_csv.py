"""
Import Fixed Width

this file defines several utility functions designed to help take a large fixed width file
and import it into a PostgreSQL database.

Fixed width files require a config file to decode, so this script relies on a well maintained
set of config files stored in ./configs. At run time, we search this folder for any that match
the import file.

If more than one config is a match, user input will be required to choose between them.

If none match, a new one must be created. Config files for this program are JSON files with a
few required keys.

    "ORIGIN":
        where this data you're trying to import came from
        should be unique, so include the year
    "SHORT_NAME":
        the shortest unique name possible
        used when building the prompt for users to choose between matching configs
    "CLEANUP_SCRIPT":
        there should be a folder in the same directory as this script called cleanup_scripts
        it contains python scripts defining data transformations
        this key should be the name of one of those files
        the user will be prompted to accept or decline each transformation in that file
        each transformation will display a reason it should be applied
    "README":
        there should be a folder in the same directory as this script called readmes
        it contains readme files for each type of data this script is aware of
        which in turn contain everything someone working with that data should know
        this key should be the name of one of those files
    "CONFIG":
        the meat of the file, with its own prescribed format inherited from FixedWidth.py
        (https://github.com/ShawnMilo/fixedwidth)
        "<column_name>":
            "required": (required)
                whether this column is allowed to be blank
                governs whether the database column will have be nullable or not
            "type": (required)
                may  be string, integer, decimal, or date
                PostgreSQL types text, int, real, and timestamp respectively
                additional python validation is also done based on this type
            "format":
                required when "type" is date, unused otherwise
                a format string as used by python's datetime.strptime()
                see https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
            "default":
                if field is not required, the default value
                will be added as a default on the PostgreSQL column

"""

import csv

from io import StringIO

from .csv_configs import CONFIGS
from .utils import print_progres_bar, table_declaration_from_config

def get_matching_configs(first_row, delimiter):
    reader_kwargs = {
        'delimiter': delimiter,
    }
    if not delimiter:
        reader_kwargs.pop('delimiter')
    parsed_row = next(csv.reader(StringIO(first_row), **reader_kwargs))

    matching_configs = []
    for config in CONFIGS:
        if config['ROW_LENGTH'] == len(parsed_row):
            matching_configs.append(config)
    return matching_configs

def import_csv(
    in_filename,
    table_name,
    config,
    delimiter,

    dbname=None,
    user=None,
    password=None,
    host=None,
    port=None,

    **progress_bar_kwargs
):
    buffer = StringIO()
    # with open('./' + table_name + '.csv', 'w') as csv_outfile:
    # writer = csv.writer(csv_outfile)
    writer = csv.writer(buffer)

    with open(in_filename) as in_file:
        number_of_lines_approx = os.path.getsize(in_filename) // config['ROW_LENGTH']

        reader_kwargs = {
            'delimiter': delimiter,
        }
        if not delimiter:
            reader_kwargs.pop('delimiter')
        reader = csv.reader(in_file, **reader_kwargs)

        progress_bar_defaults = {
            'total': number_of_lines_approx,
            'prefix': 'Reading File:',
            'suffix': 'Complete',
            'length': 40,
        }
        progress_bar_defaults.update(progress_bar_kwargs)

        cur_line = print_progres_bar(0, **progress_bar_defaults)

        log_stats = {
            'filename': in_filename,
            'file_size': os.path.getsize(in_filename),
            'rows': number_of_lines_approx,
            'writer_time': 0,
            'mogrify_time': 0,
            'progress_bar_time': 0,
            'copy_time': 0,
            'total_time': 0,
        }

        total_start = timeit.default_timer()

        conn = psycopg2.connect(
            dsn='', # define dsn in case all other args are None
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        cur = conn.cursor()

        for i, line in enumerate(reader):
            start = timeit.default_timer()
            writable_row = [cur.mogrify('%s', (x,)) for x in line]
            end = timeit.default_timer()
            log_stats['mogrify_time'] += (end - start)

            start = timeit.default_timer()
            writer.writerow(writable_row)
            end = timeit.default_timer()
            log_stats['writer_time'] += (end - start)

            start = timeit.default_timer()
            cur_line = print_progres_bar(i, cur_line=cur_line, **progress_bar_defaults)
            end = timeit.default_timer()
            log_stats['progress_bar_time'] += (end - start)

    start = timeit.default_timer()
    create_table_command = table_declaration_from_config(table_name, config['CONFIG'])
    cur.execute(create_table_command)

    # with open('./' + table_name + '.csv') as csv_outfile:
    buffer.seek(0)
    cur.copy_expert("COPY {table_name} FROM STDIN WITH (FORMAT CSV)".format(table_name=table_name), buffer)
    end = timeit.default_timer()
    log_stats['copy_time'] += (end - start)

    conn.commit()
    conn.close()

    total_end = timeit.default_timer()
    log_stats['total_time'] = (total_end - total_start)

    print_progres_bar(number_of_lines_approx, **progress_bar_defaults)

    with open('./%s_import_stringIO_log.txt' % table_name, 'a') as out_file:
        out_file.write(str(log_stats))
        out_file.write('\n')
