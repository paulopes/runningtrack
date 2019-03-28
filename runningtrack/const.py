#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Dictionary in which the elements can be accessed as if they
    were attributes, and cannot be changed.

    When you have defined a Const instance like this:

    .. code-block:: python

        const = Const(ABC="123")
        
    Then ``const["ABC"]`` is the same as ``const.ABC``

    You can set a new constant item in the same way by assigning
    to it, but you CANNOT change items that already exist,
    because they are intended to be constant.

    You can define items in the constructor, as in the example,
    but the value of such an item can be overridden by an OS
    environment value if it has the same name.

    For example, if there is an OS environment variable called
    WEB_PORT with a value of 8080, and you define a const in
    this way:

    .. code-block:: python

        const = Const(WEB_PORT=3000)

    Then ``const.WEB_PORT`` will have the value 8080, not 3000.

    If you want to set a constant item that cannot be overriden
    by an envirnment variable, then set it after instanciating,
    like this:

    .. code-block:: python

        const = Const()
        const.WEB_PORT = 3000
    
    You can pass one or more dictionaries to the constructor
    for when that is convenient for defining the values:

    .. code-block:: python

        const = Const({
            "ONE": 1,
            "TWO": 2,
        }, {
            "THREE": 3,
            "FOUR": 4,
        }, FIVE=5)

    
    This is the same as:
    
    .. code-block:: python

        const = Const(ONE=1, TWO=2, THREE=3, FOUR=4, FIVE=5)
    
"""

from __future__ import division

import os

class Const(dict):

    def __init__(self, *args, **kwargs):
        self._exceptions = set(self.__dict__.keys())
        for item in args + (kwargs,):
            if isinstance(item, dict):
                for key in item:
                    if key not in self.__dict__:
                        self.__dict__[key] = os.environ.get(key, item[key])

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        if name not in self.__dict__ or name in self._exceptions:
            self.__dict__[name] = value
        else:
            raise AttributeError("Not allowed to set attribute: " + name)

    def __setitem__(self, key, value):
        if key not in self.__dict__ or key in self._exceptions:
            self.__dict__[key] = value
        else:
            raise KeyError("Not allowed to set key: " + key)

    def __getitem__(self, key): 
        return self.__dict__[key]
