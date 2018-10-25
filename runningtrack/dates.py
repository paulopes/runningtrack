# -*- coding: utf-8 -*-

""" Helper functions to deal with dates
"""

from __future__ import print_function, division, unicode_literals

import time
import datetime


def now(utc_setting=True, timezone=None, timestamp=None):
    """ The current moment in time.

        :sig: (bool, Union[datetime.timezone, None], Union[float, None]) -> datetime.datetime

        Parameters
        ----------
        utc_setting : bool, optional

            Whether we want the current moment to be timezone agnostic.
            (the default is True, which denotes to be timezone agnostic.)

        timezone : datetime.timezone, optional

            Which timezone we want the current moment to be reported in.
            (the default is None, which denotes the local machine's timezone.)

        timestamp : float, optional
        
            POSIX timestamp of the moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        the_now_moment : datetime.datetime

            The current moment.
    """
    if timestamp is None:
        timestamp = time.time()
    if utc_setting:
        the_now_moment = datetime.datetime.utcfromtimestamp(timestamp)
    else:
        the_now_moment = datetime.datetime.fromtimestamp(timestamp, tz=timezone)
    return the_now_moment


def current_date(utc_setting=True, moment=None):
    """ The current date as text in the ISO format.

        :sig: (bool, Union[datetime.datetime, None]) -> str

        Parameters
        ----------
        utc_setting : bool, optional

            Whether we want the current moment to be timezone agnostic.
            (the default is True, which denotes to be timezone agnostic.)

        moment : datetime.datetime, optional

            The moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        str

            The current date as text.
    """
    if moment is None:
        moment = now(utc_setting)
    return str(moment.date())


def current_day(utc_setting=True, moment=None):
    """ The current day of the week.

        :sig: (bool, Union[datetime.datetime, None]) -> int

        Parameters
        ----------
        utc_setting : bool, optional

            Whether we want the current moment to be timezone agnostic.
            (the default is True, which denotes to be timezone agnostic.)

        moment : datetime.datetime, optional

            The moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        int

            The current day of the week in range [0, 6], Monday is 0.
    """
    if moment is None:
        moment = now(utc_setting)
    return datetime.datetime.timetuple(moment)[6]


def current_time(utc_setting=True, moment=None):
    """ The current time of the day in the HH:MM format (no seconds).

        :sig: (bool, Union[datetime.datetime, None]) -> str

        Parameters
        ----------
        utc_setting : bool, optional

            Whether we want the current moment to be timezone agnostic.
            (the default is True, which denotes to be timezone agnostic.)

        moment : datetime.datetime, optional

            The moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        str

            The current time of the day as text.
    """
    if moment is None:
        moment = now(utc_setting)
    return str(moment.time())[:5]


def current_seconds_milliseconds(moment=None):
    """ The seconds and milliseconds of the current minute.

        :sig: (Union[datetime.datetime, None]) -> float

        Parameters
        ----------
        moment : datetime.datetime, optional

            The moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        float

            The current seconds, with the milliseconds after the decimal point.
    """
    if moment is None:
        moment = datetime.datetime.now()
    return int(float(str(moment.time()).split(':')[-1]) * 1000.0) / 1000.0


def current_seconds(moment=None):
    """ The seconds of the current minute.

        :sig: (Union[datetime.datetime, None]) -> int

        Parameters
        ----------
        moment : datetime.datetime, optional

            The moment considered to be the "now" moment.
            (the default is None, which denotes the use of the current moment.)
            This parameter is mostly useful for testing purposes, otherwise it
            shouldn't be provided.

        Returns
        -------
        int

            The current seconds within the current minute.
    """
    if moment is None:
        moment = datetime.datetime.now()
    return int(str(moment.time()).split(':')[-1].split('.')[0])
