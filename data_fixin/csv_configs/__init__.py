import os, json

from collections import OrderedDict

CONFIGS = []
config_dir = os.path.split(__file__)[0]
for config_filename in os.listdir(config_dir):
    if os.path.splitext(config_filename)[1] == '.json':
        with open(os.path.join(config_dir, config_filename), 'rb') as config_file:
            config_dict = json.loads(config_file.read().decode('utf-8'), object_pairs_hook=OrderedDict)
        config_dict['ROW_LENGTH'] = len(config_dict['CONFIG'])

        CONFIGS.append(config_dict)
