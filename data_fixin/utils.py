import psycopg2, os

from shutil import copyfileobj


def print_progres_bar(
    iteration, total,
    num_errors=0,
    cur_line=None,
    prefix='',
    suffix='',
    error_text='',
    decimals=1,
    length=100,
    empty='-',
    fill='\u2588',
    error_fill='\u2592',
):
    """
    Call in a loop to create a terminal progress bar
    @params:
        iteration    - required  :  current iteration (int)
        total        - required  :  total iterations (int)
        cur_line     - optional  :  the last thing this function printed, used to avoid double printing the same line
        prefix       - optional  :  string to display before the progress bar
        suffix       - optional  :  string to display after the progress bar
        decimals     - optional  :  positive number of decimals in percent complete (int)
        length       - optional  :  character length of bar (int)
        empty        - optional  :  bar empty character, will be replaced by the fill character as we load
        fill         - optional  :  bar fill character, default is unicode "FULL BLOCK"
    """

    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))

    filled_length = int(length * iteration / total)
    error_length = int(length * num_errors / total)
    filled_length -= error_length

    bar = (
        (error_fill * error_length) +
        fill * filled_length +
        empty * (length - error_length - filled_length)
    )

    printable_line = (
        "\r{prefix} |{bar}| {percent}% {suffix} "
        "| {num_errors} {error_text}".format(
            prefix=prefix,
            bar=bar,
            percent=percent,
            suffix=suffix,
            num_errors=num_errors,
            error_text=error_text,
        )
    )
    if printable_line != cur_line:
        print(printable_line, end='\r')
    # print new line on complete
    if iteration == total:
        print("\n")
    return printable_line

def sql_declaration_from_column_config(column_name, column_config):
    """
    Takes part of a valid FixedWidth config dictionary, and returns part of a SQL statement.

    @params
        column_name   - required  :  can be any string, but typically a key from a FixedWidth config
        column_config - required  :  the dictionary that defines a column in a FixedWidth config

    The string returned is meant to define a column in a CREATE TABLE statement. Eg.

        CREATE TABLE some_table (
            <return value goes here>
        )
    """

    null_text = ''
    if not column_config.get('required', False):
        null_text = " null"

    default_text = ''
    if column_config.get('default'):
        default_text = " DEFAULT " + str(column_config['default'])

    return "{column_name} {type}{null_text}{default_text},\n".format(
        column_name=column_name,
        type=column_config['type'],
        null_text=null_text,
        default_text=default_text,
    )

def table_declaration_from_config(table_name, config):
    """
    Takes a valid FixedWidth config dictionary and returns a create table statement to match.
    calls column_config_to_sql_declaration.
    """

    sql_command = "CREATE TABLE " + table_name + " (\n"
    for column_name, column_config in config.items():
        sql_command += sql_declaration_from_column_config(column_name, column_config)
    sql_command = sql_command.rstrip(',\n') + '\n)'
    return sql_command

def create_table_from_config(
    table_name,
    config,
    connection,
    fail_silently=True
):
    cur = connection.cursor()

    create_table_command = table_declaration_from_config(table_name, config['CONFIG'])

    try:
        cur.execute(create_table_command)
    except psycopg2.ProgrammingError as e:
        if fail_silently:
            connection.rollback()
        else:
            raise e
    finally:
        cur.close()

def psql_copy(
    data_stream,
    table_name,
    connection,
    error_dir=None,
):
    cur = connection.cursor()

    data_stream.seek(0)
    try:
        cur.copy_expert("COPY {table_name} FROM STDIN WITH (FORMAT CSV)".format(
            table_name=table_name
        ), data_stream)
    except (psycopg2.OperationalError, psycopg2.DataError):
        if error_dir:
            error_basename = table_name + '_copy_error.csv'
            error_filename = os.path.join(
                error_dir,
                error_basename
            )

            with open(error_filename, 'w', newline='') as error_file:
                copyfileobj(data_stream, error_file)
        else:
            raise
    finally:
        cur.close()
