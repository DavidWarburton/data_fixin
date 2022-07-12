import os, json

from collections import OrderedDict


def row_length(fixed_width_config):
    """
    Given a valid csv config dictionary, returns the number of columns.
    Used to do a quick first check if to see if a config matches the in_file.
    """
    last_col = next(reversed(fixed_width_config.values()))
    last_char = last_col.get('end_pos') or last_col['start_pos'] + last_col['length']
    return last_char + 1 # Extra character for line feed

CONFIGS = {}
config_dir = os.path.split(__file__)[0]
for config_filename in os.listdir(config_dir):
    if os.path.splitext(config_filename)[1] == '.json':
        config_fullpath = os.path.join(config_dir, config_filename)
        with open(config_fullpath) as config_file:
            try:
                config_dict = json.load(config_file, object_pairs_hook=OrderedDict)
            except json.decoder.JSONDecodeError as e:
                raise json.decoder.JSONDecodeError(
                    "{config} is broken. {error}".format(
                        config=config_filename,
                        error=str(e),
                    ),
                    doc=e.doc,
                    pos=e.pos,
                )
        config_dict['ROW_LENGTH'] = row_length(config_dict['CONFIG'])
        config_dict['CONFIG_TYPE'] = 'fixed width'
        config_dict['FILE_PATH'] = config_fullpath

        CONFIGS[config_fullpath] = config_dict
