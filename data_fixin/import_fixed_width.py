"""
Import Fixed Width

This file defines several functions designed to help take a large fixed width file
and import it into a PostgreSQL database.

Fixed width files require a config file to decode, so this script relies on a well maintained
set of config files stored in ./fixed_width_configs. At run time, we search this folder for
any that match the import file.

If more than one config is a match, user input will be required to choose between them.

If none match, a new one must be created. Config files for this program are JSON files with a
few required keys.

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

import os, argparse, getpass, psycopg2, importlib.util, csv, re, json

from io import StringIO
from decimal import Decimal, ROUND_HALF_EVEN, InvalidOperation
from collections import OrderedDict
from datetime import datetime, date, time, timedelta
from six import string_types, integer_types

from .fixed_width_configs import CONFIGS
from .utils import print_progres_bar, create_table_from_config, psql_copy


class LazyParseDict(dict):

    def get_evaluated_and_error_dicts(self):
        evaluated_dict = {}
        error_dict = {}
        for key, lazy_value in self.items():
            try:
                evaluated_dict[key] = lazy_value.evaluate(row_data=self)
            except AttributeError:
                # Turns out this isn't a lazy value, so assume it's a correctly parsed value
                evaluated_dict[key] = lazy_value
            except ValueError:
                error_dict[key] = {
                    'error': str(lazy_value.original_error),
                    'value': lazy_value.raw_string,
                }
        return evaluated_dict, error_dict


class LazyParsedValue(object):
    slice_pattern = re.compile(r'\{([0-9a-zA-Z_]+)\}\[(-?\d*?):(-?\d*?)\]')
    field_pattern = re.compile(r'\{([0-9a-zA-Z_]+)\}')

    def __init__(
        self,
        validation_field,
        raw_string,
        parsed_value=None,
        original_error=None,
        solution_strings=None,
    ):
        self.original_error = original_error
        self.solution_strings = solution_strings
        self.validation_field = validation_field
        self.raw_string = raw_string
        self.parsed_value = parsed_value

    def __getitem__(self, index):
        return self.raw_string[index]

    def evaluate(self, row_data):

        if self.parsed_value is not None:
            return self.parsed_value

        for solution_string in self.solution_strings:
            string_length_adjust = 0
            for match in re.finditer(self.slice_pattern, solution_string):
                match_start, match_end = match.span()
                match_start -= string_length_adjust
                match_end -= string_length_adjust
                field, slice_start, slice_end = match.groups()
                field_value = row_data[field]
                if isinstance(field_value, LazyParsedValue):
                    field_value = field_value.raw_string
                slice_start = int(slice_start or 0)
                slice_end = int(slice_end or len(field_value))
                field_value = field_value[slice(slice_start, slice_end)]
                solution_string = (
                    solution_string[:match_start] +
                    field_value +
                    solution_string[match_end:]
                )
                string_length_adjust += (match_end - match_start) - len(field_value)

            for match in re.finditer(self.field_pattern, solution_string):
                match_start, match_end = match.span()
                field, = match.groups()
                field_value = row_data[field]
                if isinstance(field_value, LazyParsedValue):
                    field_value = field_value.raw_string
                solution_string = (
                    solution_string[:match_start] +
                    field_value +
                    solution_string[match_end:]
                )

            try:
                return self.validation_field.parse(solution_string, lazy=False)
            except ValueError:
                continue

        else:
            raise self.original_error

"""
The FixedWidth parser class definitions.
"""

class Field(object):
    parameters = {
        'null_values': [],
        'alignment': 'center',
        'padding': ' ',
    }

    def _setup(self):
        raise NotImplementedError

    def _pre_format(self):
        raise NotImplementedError

    def _psql_format(self):
        raise NotImplementedError

    def _type_test(self):
        raise NotImplementedError

    def _parse(self):
        raise NotImplementedError

    def __init__(self, **kwargs):

        # Load values into parameters.
        self.parameters = dict(self.parameters, **kwargs)

        #required values
        if self.parameters.get('start_pos') is None:
            raise ValueError(
                "start_pos not provided for field %s" % (self.parameters.get('field_name'),))

        #end position or length required
        if self.parameters.get('end_pos') is None and self.parameters.get('length') is None:
            raise ValueError("An end position or length is required for field %s" % (self.parameters.get('field_name'),))

        #end position and length must match if both are specified
        if self.parameters.get('end_pos') is not None and self.parameters.get('length') is not None:
            if self.parameters.get('length') != self.parameters.get('end_pos') - self.parameters.get('start_pos') + 1:
                raise ValueError(
                    "Field %s length (%d) does not coincide with "
                    "its start and end positions." % (self.parameters.get('field_name'), self.parameters.get('length'))
                )

        #fill in length and end_pos
        if self.parameters.get('end_pos') is None:
            self.parameters['end_pos'] = self.parameters.get('start_pos') + self.parameters.get('length') - 1
        if self.parameters.get('length') is None:
            self.parameters['length'] = self.parameters.get('end_pos') - self.parameters.get('start_pos') + 1

        #end_pos must be greater than start_pos
        if self.parameters.get('end_pos') < self.parameters.get('start_pos'):
            raise ValueError("%s end_pos must be *after* start_pos." % (self.parameters.get('field_name'),))

        if 'default' in kwargs and kwargs.get('required', False):
            raise ValueError(
                "Field %s is required; "
                "can not have a default value" % (self.parameters.get('field_name'),)
            )


        #make sure alignment is 'left', 'right', or 'center'
        if self.parameters.get('alignment') not in ('left', 'right', 'center',):
            raise ValueError(
                "Field %s has an invalid alignment (%s). "
                "Allowed: 'left', 'right' or 'center'" % (self.parameters.get('field_name'), self.parameters.get('alignment'))
            )

        if self.parameters.get('null_values') and self.parameters.get('required'):
            raise ValueError("Cannot have null_values on required field %s" % self.parameters.get('field_name'))

        # Do setup here so we can be sure calling self.parse will work
        self._setup()

        # if a default value was provided, parse it
        if self.parameters.get('default') is not None:
            try:
                self.parameters['default'] = self.parse(self.parameters.get('default'))
            except TypeError:
                raise ValueError("Default for field %s is of the wrong type" % (self.parameters.get('field_name'),))

    def validate(self, value):
        """
        Check if a given python value is valid input for this field.
        Relies on self._type_test() for type specific processing.
        """
        if value is not None:
            if not self._type_test(value):
                raise ValueError(
                    "%s is defined as a %s, "
                    "but the value is not of that type." \
                    % (self.parameters.get('field_name'), self.parameters.get('type'),)
                )

            #ensure value passed in is not too long for the field
            field_data = self._pre_format(value)
            if len(str(field_data)) > self.parameters.get('length'):
                raise ValueError(
                    "%s is too long (limited to %d "
                    "characters)." % (self.parameters.get('field_name'), self.parameters.get('length'))
                )

        else: #no value passed in

            #if required but not provided
            if self.parameters.get('required'):
                raise ValueError(
                    "Field %s is required, but was "
                    "not provided." % (self.parameters.get('field_name'),)
                )

            if self.parameters.get('default') and not self._type_test(self.parameters.get('default')):
                raise ValueError("Default value for %s is not valid" % self.parameters.get('field_name'))

        return True

    def format(self, value):
        """
        Take a python value and turn it into a segment of a fixed width string.
        Relies on self._pre_format for processing related to type.
        """
        if value is not None:
            formated_value = self._pre_format(value)
        else:
            formated_value = ''

        if self.parameters.get('alignment') == 'left':
            justify = formated_value.ljust
        elif self.parameters.get('alignment') == 'right':
            justify = formated_value.rjust
        else:
            justify = formated_value.center
        return justify(self.parameters.get('length'), self.parameters.get('padding'))

    def find_value(self, fw_string):
        """
        Given a fixed width string, pull out the value corresponding to this field.
        Result will typically be passed directly to self.parse()
        """
        relevant_string =  fw_string[self.parameters.get('start_pos') - 1:self.parameters.get('end_pos')]

        if self.parameters.get('alignment') == 'left':
            # if the value's on the left, strip out padding to the right
            strip = relevant_string.rstrip
        elif self.parameters.get('alignment') == 'right':
            # and vice versa
            strip = relevant_string.lstrip
        else:
            # or if centered, strip out padding from both sides
            strip = relevant_string.strip
        return strip(self.parameters.get('padding'))

    def parse(self, value, lazy=True):
        """
        Take a string and parse it into a python value.

        If an error occurs, and a solutions dict is defined, returns a list
        of solutions that match the value.

        Relies on self._parse() for processing related to type.
        """

        if value == '' or value in self.parameters.get('null_values', []):
            # If there is no default, then self.parameters.get('default') is None,
            # which is still the appropriate return value
            return self.parameters.get('default')
        else:
            try:
                parsed_value = self._parse(value)
            except ValueError as e:
                full_error = ValueError("Field {0} received invalid value '{1}'. Reported error was: {2}".format(
                    self.parameters.get('field_name'),
                    value,
                    str(e),
                ))

                if not lazy:
                    raise full_error

                solutions = [
                    solution
                    for pattern, solution in self.parameters.get('solutions', {}).items()
                    if re.compile(pattern).fullmatch(value)
                ]

                return LazyParsedValue(
                    original_error=full_error,
                    solution_strings=solutions,
                    validation_field=self,
                    raw_string=value,
                )

            else:
                if lazy:
                    return LazyParsedValue(
                        parsed_value = parsed_value,
                        validation_field=self,
                        raw_string=value,
                    )
                else:
                    return parsed_value

    def psql_format(self, value):
        """
        Take a python value and return a string in a format that PostgreSQL COPY can parse.
        Relies on self._psql_format() for type specific processing.
        """
        if value is not None:
            formated_value = self._psql_format(value)
        else:
            formated_value = ''
        return formated_value


class IntField(Field):
    type_key = 'int'

    def _setup(self):
        pass

    def _pre_format(self, value):
        return str(value)

    def _psql_format(self, value):
        return str(value)

    def _type_test(self, value):
        return isinstance(value, integer_types)

    def _parse(self, value):
        return int(value)


class TextField(Field):
    type_key = 'text'

    def _setup(self):
        pass

    def _pre_format(self, value):
        return str(value)

    def _psql_format(self, value):
        return str(value)

    def _type_test(self, value):
        return isinstance(value, string_types)

    def _parse(self, value):
        return str(value)


class IntervalField(Field):
    type_key = 'interval'

    number_pattern = '(\-?\d+?\.?\d*)' # this is a regex, but we're going to sub it into
                                       # the format string so we don't compile it yet

    marker_pattern = re.compile(r'\\%(\w)') # finds a marker in the format string
                                            # extra backslashes are because we escape the format string
                                            # so % becomes \\%

    marker_legend = {
        'd': 'days',
        'H': 'hours',
        'M': 'minutes',
        'S': 'seconds',
        'f': 'microseconds',
    }

    def _setup(self):
        if self.parameters.get('format') is None:
            raise ValueError("No format string provided for field %s" % (self.parameters.get('field_name'),))

        if not isinstance(self.parameters.get('format'), list):
            self.parameters['format'] = [self.parameters.get('format')]

        self.parameters['matchable_format'] = []
        self.parameters['ordered_markers'] = []

        for format in self.parameters.get('format'):

            format = re.escape(format)
            self.parameters.get('matchable_format').append(re.compile(
                '^' + re.sub(
                    self.marker_pattern,
                    self.number_pattern,
                    format
                ) + '$')
            )

            self.parameters['ordered_markers'].append([])
            for marker in re.findall(self.marker_pattern, format):
                try:
                    self.parameters['ordered_markers'][-1].append(self.marker_legend[marker])
                except KeyError:
                    raise ValueError(
                        "Incorrect format string provided for field %s, can only contain"
                        " %d, %H, %M, %S, or %f" % (self.parameters.get('field_name'),)
                    )

    def _pre_format(self, value):
        remaining_seconds = Decimal(value.total_seconds())
        periods = OrderedDict([
            ('days',         Decimal('86400')),
            ('hours',        Decimal('3600')),
            ('minutes',      Decimal('60')),
            ('seconds',      Decimal('1')),
            ('microseconds', Decimal('0.000001')),
        ])

        # cut out any periods not in the format
        periods = OrderedDict(
            [
                (key, periods[key])
                for key in periods.keys() & set(self.parameters['ordered_markers'][0])
            ]
        )

        period_values = {}

        for period_name, period_seconds in periods.items():
            period_value, remaining_seconds = divmod(remaining_seconds, period_seconds)
            period_values[period_name] = period_value

        if remaining_seconds > 0:
            last_period = next(reversed(periods))
            period_values[last_period] += remaining_seconds / periods[last_period]

        def replace_marker(match):
            return str(period_values[self.marker_legend[match.groups()[0]]])

        return re.sub(self.marker_pattern, replace_marker, re.escape(self.parameters['format'][0]))

    def _psql_format(self, value):
        psql_string = "%s S" % value.total_seconds()
        if value.total_seconds() < 0:
            psql_string += " AGO"
        return psql_string

    def _type_test(self, value):
        return isinstance(value, timedelta)

    def _parse(self, value):
        timedelta_kwargs = {}
        for matchable_format, ordered_markers in zip(
            self.parameters.get('matchable_format'),
            self.parameters.get('ordered_markers'),
        ):
            match = re.match(matchable_format, value)
            if not match:
                continue
            matched_groups = match.groups()

            if len(matched_groups) != len(ordered_markers):
                continue

            for marker, match in zip(ordered_markers, matched_groups):
                timedelta_kwargs[marker] = float(match)

            return timedelta(**timedelta_kwargs)

        raise ValueError("Format did not match value")


class BaseTimeyField():

    def _get_arbitrary_instance(self):
        """
        Get an instance of the type this field parses, eg datetime.now() for datetimes.
        Used to validate the format string.
        """
        raise NotImplementedError

    def _parse_as_format(self, value, format):
        """
        Parse using a given format string. Type specific parse logic goes here, so that
        _parse can handle multiple format strings.
        """
        raise NotImplementedError

    def _setup(self):
        if self.parameters.get('format') is None:
            raise ValueError("No format string provided for field %s" % (self.parameters.get('field_name'),))

        if not isinstance(self.parameters.get('format'), list):
            self.parameters['format'] = [self.parameters.get('format')]

        for format in self.parameters.get('format'):
            try:
                self._pre_format(self._get_arbitrary_instance())
            except ValueError:
                raise ValueError(
                    "Incorrect format string '{0}' provided for field {1}".format(
                        format,
                        self.parameters.get('field_name'),
                    )
                )

    def _pre_format(self, value):
        return value.strftime(self.parameters.get('format')[0])

    def _parse(self, value):
        for format in self.parameters.get('format'):
            try:
                return self._parse_as_format(value, format)
            except ValueError as error:
                last_error = error
        else:
            raise last_error


class DateField(BaseTimeyField, Field):
    type_key = 'date'

    def _get_arbitrary_instance(self):
        return datetime.now().date()

    def _psql_format(self, value):
        return value.strftime('%Y-%m-%d')

    def _type_test(self, value):
        return isinstance(value, date)

    def _parse_as_format(self, value, format):
        return datetime.strptime(value, format).date()


class TimeField(BaseTimeyField, Field):
    type_key = 'time'

    def _get_arbitrary_instance(self):
        return datetime.now().time()

    def _psql_format(self, value):
        psql_format = '%H:%M:%S.%f'
        # check if value is timezone aware
        if value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None:
            psql_format += ' %z'
        return value.strftime(psql_format)

    def _type_test(self, value):
        return isinstance(value, time)

    def _parse_as_format(self, value, format):
        return datetime.strptime(value, format).time()


class TimestampField(BaseTimeyField, Field):
    type_key = 'timestamp'

    def _get_arbitrary_instance(self):
        return datetime.now()

    def _psql_format(self, value):
        psql_format = '%Y-%m-%d %H:%M:%S'
        # check if value is timezone aware
        if value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None:
            psql_format += ' %z'
        return value.strftime(psql_format)

    def _type_test(self, value):
        return isinstance(value, datetime)

    def _parse_as_format(self, value, format):
        return datetime.strptime(value, format)


class NumericField(Field):
    type_key = 'numeric'

    def _setup(self):
        if self.parameters.get('precision') is not None and type(self.parameters.get('precision')) != int:
            raise ValueError("Precision parameter for field %s must be an int" % (key,))

        self.parameters.setdefault('rounding', ROUND_HALF_EVEN)

    def _pre_format(self, value):
        """
        quantizes field if precision is set
        """
        if self.parameters.get('precision'):
            return str(Decimal(
                str(value).quantize(
                    Decimal(
                        '0.%s' % ('0' * self.parameters.get('precision')),
                        self.parameters.get('rounding'),
                    )
                )
            ))
        else:
            return str(value)

    def _psql_format(self, value):
        if self.parameters.get('precision'):
            return str(Decimal(
                str(value).quantize(
                    Decimal(
                        '0.%s' % ('0' * self.parameters.get('precision')),
                        self.parameters.get('rounding'),
                    )
                )
            ))
        else:
            return str(Decimal(value))

    def _type_test(self, value):
        return isinstance(value, Decimal)

    def _parse(self, value):
        try:
            return Decimal(value)
        except InvalidOperation as e:
            raise ValueError(str(e))


class BooleanField(Field):
    type_key = 'boolean'

    def _setup(self):
        if (
            self.parameters.get('true_values') is None and
            self.parameters.get('false_values') is None
        ):
            raise ValueError(
                "No true_values or false_values provided for field {0}".format(
                    self.parameters.get('field_name'),
                )
            )

        if not isinstance(self.parameters.get('true_values'), list):
            self.parameters['true_values'] = [self.parameters.get('true_values')]

        if not isinstance(self.parameters.get('false_values'), list):
            self.parameters['false_values'] = [self.parameters.get('false_values')]

    def _parse(self, value):
        if value in self.parameters.get('true_values'):
            return True
        elif value in self.parameters.get('false_values'):
            return False
        else:
            raise ValueError("Boolean field received unspecified value.")

    def _pre_format(self, value):
        if value is True:
            return str(self.parameters.get('true_values')[0])
        elif value is False:
            return str(self.parameters.get('false_values')[0])

    def _psql_format(self, value):
        if value is True:
            return 't'
        elif value is False:
            return 'f'

    def _type_test(self, value):
        return isinstance(value, bool)


class FixedWidth(object):
    """
    Class for converting between Python dictionaries and fixed-width
    strings.

    Requires a 'config' dictonary.
    Each key of 'config' is the field name.
    Each item of 'config' is itself a dictionary with the following keys:
        required    a boolean; required
        type        a string; required
        value       (will be coerced into 'type'); hard-coded value
        default     (will be coerced into 'type')
        start_pos   an integer; required
        length      an integer
        end_pos     an integer
        format      a string, to format dates, required for date fields
    The following keys are only used when emitting fixed-width strings:
        alignment   a string; required
        padding     a string; required
        precision   an integer, to format decimals numbers
        rounding    a constant ROUND_xxx used when precision is set

    Notes:
        A field must have a start_pos and either an end_pos or a length.
        If both an end_pos and a length are provided, they must not conflict.

        A field may not have a default value if it is required.

        Type may be text, int, numeric, date, time, timestamp, or boolean.

        Alignment and padding are required.

    """

    def __init__(self, config, **kwargs):
        """
        Arguments:
            config: required, dict defining fixed-width format
            kwargs: optional, dict of values for the FixedWidth object
        """

        self.line_end = kwargs.pop('line_end', '\r\n')
        self.config = config

        self.data = OrderedDict()
        if kwargs:
            self.data = kwargs

        fields_from_type = {
            cls.type_key: cls
            for cls in Field.__subclasses__()
            if hasattr(cls, 'type_key')
        }

        unordered_fields = {}

        #Raise exception for bad config
        for field_name, field_config in self.config.items():

            #make sure authorized type was provided
            type = field_config['type'].lower()
            if not type in fields_from_type:
                raise ValueError(
                    "Field {0} has an invalid type ({1}). Allowed: {2}".format(
                        field_name,
                        field_config['type'],
                        fields_from_type.keys()
                    )
                )

            if field_name in unordered_fields:
                raise ValueError("Duplicate field name: %s" % field_name)

            unordered_fields[field_name] = fields_from_type[type](field_name=field_name, **field_config)

        self.ordered_fields = OrderedDict(sorted(unordered_fields.items(), key=lambda x: x[1].parameters['start_pos']))

        # ensure start_pos and end_pos or length is correct in config
        current_pos = 1
        for field in self.ordered_fields.values():

            if field.parameters.get('start_pos') != current_pos:
                raise ValueError(
                    "Field %s starts at position %d; "
                    "should be %d (or previous field definition is incorrect)." \
                    % (field_name, field.parameters.get('start_pos'), current_pos)
                )

            current_pos = current_pos + field.parameters.get('length')

    def validate(self, data):
        """
        Compile and return a list of all validation errors in data
        """

        errors = {}

        for field_name, field in self.ordered_fields.items():
            try:
                field.validate(data.get(field_name))
            except Exception as e:
                errors[field_name] = {
                    'value': data.get(field_name),
                    'error': e,
                }

        return errors

    def get_fixed_width_line(self, data):
        """
        Returns a fixed-width line made up of data, using self.config.
        """

        errors = self.validate(data)
        if errors:
            raise ValueError(
                "Data provided had the following errors: {errors}".format(
                    errors=errors,
                )
            )

        line = ''
        for field_name, field in self.ordered_fields.items():

            line += self.ordered_fields[field_name].format(data.get(field_name))

        return line + self.line_end

    def parse_line(self, line):
        """
        Take a fixed-width string and use it to create two dicts,
        one of python values parsed from the string with self.config,
        and one of dictionaries with the following format:

        {
            "error": The error object that was raised by trying to parse the field,
            "value": The value that was found in the field.
        }

        """

        data = LazyParseDict()

        for field_name, field in self.ordered_fields.items():
            value = field.find_value(line)
            data[field_name] = field.parse(value)

        return data.get_evaluated_and_error_dicts()

    def get_psql_copy_values(self, data, bytes=False):
        """
        Take data and convert it to a format that PSQL COPY will recognize.

        Returns a list of such values, the idea being that they can be passed
        directly to csv.writer.writerow().
        """

        validation_errors = self.validate(data)
        if validation_errors:
            raise ValueError(
                "Data provided had the following errors: {errors}".format(
                    errors=validation_errors,
                )
            )

        psql_copy_values = []
        for field_name, field in self.ordered_fields.items():
            psql_copy_values.append(field.psql_format(data.get(field_name)))

        return psql_copy_values


def get_matching_configs(first_row):
    matching_configs = []
    for config in CONFIGS.values():
        if config['ROW_LENGTH'] == len(first_row):
            fixed_width_parser = FixedWidth(config['CONFIG'])
            _data, errors = fixed_width_parser.parse_line(first_row)
            if not errors:
                matching_configs.append(config)
            """
            try:
                fixed_width_parser.validate():
            except ValueError:
                pass
            else:
                matching_configs.append(config)
            """
    return matching_configs

def import_fixed_width(
    in_file,
    table_name,
    config,

    **progress_bar_kwargs
):
    fixed_width_parser = FixedWidth(config['CONFIG'])

    psql_copy_data = StringIO()
    broken_row_data = StringIO()

    psql_copy_writer = csv.writer(psql_copy_data)

    try:
        number_of_lines = os.path.getsize(in_file.name) // config['ROW_LENGTH']
    except:
        number_of_lines = len(in_file)

    progress_bar_defaults = {
        'total': number_of_lines,
        'prefix': 'Reading File:',
        'suffix': 'Complete',
        'error_text': 'rows had errors',
        'length': 30,
    }
    progress_bar_kwargs = dict(progress_bar_defaults, **progress_bar_kwargs)

    cur_progress_bar = print_progres_bar(0, **progress_bar_kwargs)

    num_errors = 0
    for i, line in enumerate(in_file):
        data, errors = fixed_width_parser.parse_line(line)

        if not errors:
            psql_copy_writer.writerow(fixed_width_parser.get_psql_copy_values(data))
        else:
            num_errors += 1
            broken_row_data.write(line)

        cur_progress_bar = print_progres_bar(
            i,
            num_errors=num_errors,
            cur_line=cur_progress_bar,
            **progress_bar_kwargs
        )

    print_progres_bar(number_of_lines, num_errors=num_errors, **progress_bar_kwargs)

    return psql_copy_data, broken_row_data
