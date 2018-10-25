# -*- coding: utf-8 -*-

""" Unit tests for the running.config module
"""

from __future__ import print_function, division, unicode_literals

import os

import running.config as config


def test_load_config_ini():
    """ Load a test configuration file in .ini format, and
        check if the DEFAULT section propagated correctly.
    """
    final_dict_should_be = {
        "ABC": {
            "def": '123',
            "ghi": 'a okay',
            "vwx": 'one',
            "yz": 'two',
        },
        "MNO": {
            "pqr": 'yes',
            "vwx": 'one',
            "yz": 'two',
            "def": '456',
        },
    }
    test_file_path = os.path.join('tests', 'files', 'load_config_ini.ini')
    loaded_dict = dict(config.load_config_ini(test_file_path))
    for item in loaded_dict:
        loaded_dict[item] = dict(loaded_dict[item])
    assert loaded_dict == final_dict_should_be


def test_merge_defaults():
    """ A dictionary that has an item with a "DEFAULT" key, if
        that item is itself a dictionary, then it should merge
        that item's subitems with all the other items in the
        dictionary that are also themselves dictionaries.
    """
    original_dict = {
        "ABC": {
            "def": 123,
            "ghi": 'a okay',
        },
        "JKL": 9.25,
        "MNO": {
            "pqr": True,
        },
        "DEFAULT": {
            "vwx": 'one',
            "yz": 'two',
            "def": 456,
        },
    }
    merged_dict = dict(config.merge_defaults(original_dict))
    merged_dict_should_be = {
        "ABC": {
            "def": 123,
            "ghi": 'a okay',
            "vwx": 'one',
            "yz": 'two',
        },
        "JKL": 9.25,
        "MNO": {
            "pqr": True,
            "vwx": 'one',
            "yz": 'two',
            "def": 456,
        },
    }
    assert merged_dict == merged_dict_should_be
