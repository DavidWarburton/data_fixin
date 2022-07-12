This is a command line utility for importing and cleaning up data from fixed
width files. There were ambitions that it also handle CSV, but was mostly
wishful thinking because PostgreSQL COPY makes importing CSV much faster.

We probably won't get CSV files, so that work isn't important. There is
provision for it in the code, but it's unfinished and can be safely ignored.

At it's most basic, can be run as:

    <python> import.py <path_to_data_file or files>
        -db db-18-g03
        -o projectdb.popdata.bc.ca
        -p 5432

To parse the fixed width files, we rely on a library of config files stored in

    R:\working\data_fixin\data_fixin\fixed_width_configs

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

There's a helper utility for creating these config files from the data
dictionaries we were provided, though it's very rough, and isn't smart
enough to just take one of those dictionaries as input.

Instead, convert the dictionary into a CSV file with the following header:

    start, end, variable_name, type, format, bool_true, bool_false, bool_null

Start, end, and variable_name can be coppied directly from the data dictionary.

Type must be one of the types accepted by CONFIG. Most of the data dictionaries
have a type field that will give you a good idea which type to use, but boolean
is never included, and many of the number fields contain values like '003m9',
so caution is warranted. It's hard to get this right on the first try.

Format is a format string matching python's strftime-strptime behavior.

Bool_true, bool_false, and bool_null should be syntactically valid json lists.
For bool_true, ["Y", "YES"] might be a common value. Note that despite its name
bool_null can be defined for any field. This is especially important for number
fields, since many of them contain "." when null, and so should have ["."] as
their bool_null.

Once this csv document is prepared, create the config file with

    <python> config_helper.py <path_to_csv_file>
        -o "<A string to populate the ORIGIN key>"
        -n "<Short name>"
        -d "<Description>"
        -f "<full path where the config file should be saved>"

There are a lot of flags for the utility at this point, and using the -h flag
should give you a helpful descriptions for all of them. Here's a bit more depth
for some of the more important ones. Also, sorry about the abbreviations.
I'm not good at making them both unique and intuitive.

    --source-directory (-sd)
        Specify a directory and try to import everything in it. If there's
        anything in there that isn't a fixed width data file, we'll still try
        to import it, and that might cause an error, so try to avoid that.
        Note that you can still list files while specifying this flag, and
        those files will still be imported.

    --error-directory (-ed)
        When the importer encounters a problem line, it continues along and
        then saves a csv file with all lines that contained errors, named
        after the file it was trying to import, into this folder. Each line
        in this file have valid data in a form recognizable to PSQL COPY for
        each field that did not contain errors, and an error message and the
        raw data for each field that did.

    --table-name (-t)
        Specify the name of the table you want to import to. When importing
        more than one file, this flag causes all files to be imported into
        a single table, rather than individual tables named after the
        filenames.

    --config (-c)
        Specify a full path to one or more config files. If this isn't used
        the tool will try to match your input files to config files, and
        will require you confirm each match. That should probably get changed
        at some point, but for now use this to save yourself a headache.

    --inspect (-i)
        For each file, parse and print the first row before importing. Mostly
        handy when first using a new config.


Work in Progress

fix.py, and LazyParsedValue are part of a scheme to process the error files
created during import, and then store the solutions to those errors back in
the config files. It doesn't work yet.


General Approach

This utility is pretty slow, but it's the best of the three things I tried.

Currently, we convert the fixed width data into CSV data (stored in a StringIO)
then use PostgreSQL COPY to import it.

Unsurprisingly, this is a huge time savings compaired to running an insert on
each line, which I tried just for comparison.

When I tried to use psycopg2.Connection.Cursor.mogrify to build a single insert
that could be run after all lines were parsed, an odd quirk caused out of memory
errors for the majority of the files I was trying to import. Based on testing
with small files, this approach was comparable in speed anyway.


Outstanding Issues

I tried to import the coursemark file (idomed1991-2017.ft_crsmrk.A.dat) because
Dad wanted to do some analysis on it, and I got a unicode error. It complained
about a \U character with an invalid code, but I couldn't find that character
anywhere in the file.

This is also the largest file I've tried to import, so that might have
something to do with it. No sure.

Just because I want this line somewhere for reference:

    psycopg2.connect(
        database="db-18-g03",
        host="projectdb.popdata.bc.ca",
        port="5432",
    )