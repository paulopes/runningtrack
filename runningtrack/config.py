# -*- coding: utf-8 -*-

""" Helper functions to deal with configuration files
"""

from __future__ import print_function, division, unicode_literals

from collections import OrderedDict

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import json
import os

from .changes import sleep


def load_config_ini(full_path):
    config_ini = configparser.ConfigParser(dict_type=OrderedDict)
    config_ini.read(full_path)
    config = OrderedDict()
    for section in config_ini.sections():
        config[section] = OrderedDict()
        for option in config_ini.options(section):
            config[section][option] = config_ini.get(section, option)
    return config


def merge_defaults(config):
    """ Merge a dictionary that has an item with a "DEFAULT" key,
        if that item is itself a dictionary, then merge that
        item's subitems with all the other items in the dictionary
        that are also themselves dictionaries.

        The purpose is to mimic the behaviour of the [DEFAULT]
        sections of .ini or .conf files when using .json or .yaml
        files, or when using OrderedDict or dict data structures.

        Parameters
        ----------
        config : dict or OrderedDict
            The dictionary that is to have its "DEFAULT" key merged
            with the other keys.

        Returns
        -------
        OrderedDict or dict
            The merged version of the dictionary as an OrderedDict,
            or the original config (be it a dict or an OrderedDict)
            if it had no "DEFAULT" key to begin with, or it was not
            a dict or an OrderedDict.
    """
    if isinstance(config, dict) and "DEFAULT" in config:
        default = config["DEFAULT"]
        merged_config = OrderedDict()
        for item in config:
            if item != "DEFAULT":
                if isinstance(config[item], dict):
                    merged_item = default.copy()
                    merged_item.update(config[item])
                    merged_config[item] = merged_item
                else:
                    merged_config[item] = config[item]
        return merged_config
    else:
        return config




def load_config_json(full_path):
    try:
        with open(full_path) as config_json:
            config = merge_defaults(json.load(config_json, object_pairs_hook=OrderedDict))
    except IOError:
        sleep(1.0)
        with open(full_path) as config_json:
            config = merge_defaults(json.load(config_json, object_pairs_hook=OrderedDict))
    return config


try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


if YAML_AVAILABLE:

    class OrderedDictYAMLLoader(yaml.Loader):
        """
        A YAML loader that loads mappings into ordered dictionaries.
        """

        def __init__(self, *args, **kwargs):
            yaml.Loader.__init__(self, *args, **kwargs)

            self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
            self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

        def construct_yaml_map(self, node):
            data = OrderedDict()
            yield data
            value = self.construct_mapping(node)
            data.update(value)

        def construct_mapping(self, node, deep=False):
            if isinstance(node, yaml.MappingNode):
                self.flatten_mapping(node)
            else:
                raise yaml.constructor.ConstructorError(None, None,
                                                        'expected a mapping node, but found %s' % node.id,
                                                        node.start_mark)
            mapping = OrderedDict()
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                try:
                    hash(key)
                except TypeError as exc:
                    raise yaml.constructor.ConstructorError('while constructing a mapping',
                                                            node.start_mark, 'found unacceptable key (%s)' % exc,
                                                            key_node.start_mark)
                value = self.construct_object(value_node, deep=deep)
                mapping[key] = value
            return mapping


def load_config_yaml(full_path):
    if YAML_AVAILABLE:
        try:
            with open(full_path) as config_yaml:
                config = merge_defaults(yaml.load(config_yaml, Loader=OrderedDictYAMLLoader))
        except IOError:
            sleep(1.0)
            with open(full_path) as config_yaml:
                config = merge_defaults(yaml.load(config_yaml, Loader=OrderedDictYAMLLoader))
        return config
    else:
        return dict()



def get_file_type_and_mtime(path_with_name):
    latest_mtime = -1.0
    latest_type = ''
    for file_type in ('.json', '.yaml', '.yml', '.conf', '.ini'):
        try:
            mtime = os.path.getmtime(path_with_name + file_type)
            if mtime > latest_mtime:
                latest_mtime = mtime
                latest_type = file_type
        except OSError:
            pass
    return latest_type, latest_mtime


def get_config(path_with_name, file_type):
    if file_type == '.json':
        return load_config_json(path_with_name + file_type)
    elif YAML_AVAILABLE and file_type in ('.yaml', '.yml'):
        return load_config_yaml(path_with_name + file_type)
    elif file_type in ('.conf', '.ini'):
        return load_config_ini(path_with_name + file_type)
    return OrderedDict()  # No config file found


def validate_logging_config(config):
    if "version" not in config:
        config["version"] = 1
    if "disable_existing_loggers" not in config:
        config["disable_existing_loggers"] = False
    return config


def load_logging_config(path_with_name, config_type):
    if config_type == '.json':
        logging_config.dictConfig(validate_logging_config(load_config_json(path_with_name + config_type)))
    elif config_type in ('.yaml', '.yml'):
        logging_config.dictConfig(validate_logging_config(load_config_yaml(path_with_name + config_type)))
    elif config_type in ('.conf', '.ini'):
        try:
            logging_config.fileConfig(path_with_name + config_type, disable_existing_loggers=False)
        except IOError:
            sleep(1.0)
            logging_config.fileConfig(path_with_name + config_type, disable_existing_loggers=False)
