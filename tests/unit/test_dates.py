# -*- coding: utf-8 -*-

""" Unit tests for the running.dates module
"""

from __future__ import print_function, division, unicode_literals

import time
import datetime
import pytz

import running.dates as dates


def test_now_utc():
    """ Using the UTC default timezone.
    """
    now_timestamp = time.time()
    now_utc_moment = datetime.datetime.utcfromtimestamp(now_timestamp)
    assert dates.now(timestamp=now_timestamp) == now_utc_moment


def test_now_local():
    """ Using the local machine's timezone.
    """
    now_timestamp = time.time()
    now_moment = datetime.datetime.fromtimestamp(now_timestamp)
    assert dates.now(utc_setting=False, timestamp=now_timestamp) == now_moment


def test_now_timezones():
    """ Time returned matches specific timezones.
    """
    now_timestamp = time.time()
    utc_timezone = pytz.utc
    now_utc_time = datetime.datetime.fromtimestamp(now_timestamp, tz=utc_timezone).time()
    assert dates.now(timestamp=now_timestamp).time() == now_utc_time
    assert dates.now(utc_setting=False, timezone=utc_timezone,
                     timestamp=now_timestamp).time() == now_utc_time
    est_timezone = pytz.timezone('US/Eastern')
    now_est_time = datetime.datetime.fromtimestamp(now_timestamp, tz=est_timezone).time()
    assert dates.now(utc_setting=False, timezone=est_timezone,
                     timestamp=now_timestamp).time() == now_est_time


def test_current_date():
    """ Current Date matches various provided moments' dates.
    """
    now_moment = dates.now()
    now_date = str(now_moment.date())
    assert dates.current_date(moment=now_moment) == now_date
    now_local_moment = dates.now(utc_setting=False)
    now_local_date = str(now_local_moment.date())
    assert dates.current_date(utc_setting=False, moment=now_local_moment) == now_local_date
    other_utc_moment = datetime.datetime(2017, 12, 30, tzinfo=pytz.utc)
    assert dates.current_date(moment=other_utc_moment) == '2017-12-30'


def test_current_day():
    """ Day of the week of various dates (Monday = 0 and Sunday = 6).
    """
    monday_moment = datetime.datetime(2017, 12, 25, tzinfo=pytz.utc)
    assert dates.current_day(moment=monday_moment) == 0
    tuesday_moment = datetime.datetime(2017, 12, 26, tzinfo=pytz.utc)
    assert dates.current_day(moment=tuesday_moment) == 1
    wednesday_moment = datetime.datetime(2017, 12, 27, tzinfo=pytz.utc)
    assert dates.current_day(moment=wednesday_moment) == 2
    thursday_moment = datetime.datetime(2017, 12, 28, tzinfo=pytz.utc)
    assert dates.current_day(moment=thursday_moment) == 3
    friday_moment = datetime.datetime(2017, 12, 29, tzinfo=pytz.utc)
    assert dates.current_day(moment=friday_moment) == 4
    saturday_moment = datetime.datetime(2017, 12, 30, tzinfo=pytz.utc)
    assert dates.current_day(moment=saturday_moment) == 5
    sunday_moment = datetime.datetime(2017, 12, 31, tzinfo=pytz.utc)
    assert dates.current_day(moment=sunday_moment) == 6


def test_current_time():
    """ Time of the day in HH:MM.
    """
    midnight_moment = datetime.datetime(2017, 12, 25, tzinfo=pytz.utc)
    assert dates.current_time(moment=midnight_moment) == '00:00'
    six_am_moment = datetime.datetime(2017, 12, 25, 6, tzinfo=pytz.utc)
    assert dates.current_time(moment=six_am_moment) == '06:00'
    noon_moment = datetime.datetime(2017, 12, 25, 12, tzinfo=pytz.utc)
    assert dates.current_time(moment=noon_moment) == '12:00'
    six_pm_moment = datetime.datetime(2017, 12, 25, 18, tzinfo=pytz.utc)
    assert dates.current_time(moment=six_pm_moment) == '18:00'


def test_current_seconds_milliseconds():
    """ Various moments with milliseconds inside of a minute.
    """
    half_a_second_moment = datetime.datetime(2017, 12, 25, 1, 2, 0, 500000)
    assert dates.current_seconds_milliseconds(half_a_second_moment) == 0.5
    sixteenth_second_moment = datetime.datetime(2017, 12, 25, 1, 2, 0, 62500)
    assert dates.current_seconds_milliseconds(sixteenth_second_moment) == 0.062
    five_seconds_eighth_moment = datetime.datetime(2017, 12, 25, 1, 2, 5, 125000)
    assert dates.current_seconds_milliseconds(five_seconds_eighth_moment) == 5.125
    nine_seconds_quarter_moment = datetime.datetime(2017, 12, 25, 1, 2, 9, 250000)
    assert dates.current_seconds_milliseconds(nine_seconds_quarter_moment) == 9.25


def test_current_seconds():
    """ Various moments in seconds inside of a minute.
    """
    zero_seconds_moment = datetime.datetime(2017, 12, 25, 1, 2)
    assert dates.current_seconds_milliseconds(zero_seconds_moment) == 0
    twenty_seconds_moment = datetime.datetime(2017, 12, 25, 1, 2, 20)
    assert dates.current_seconds_milliseconds(twenty_seconds_moment) == 20
    fifty_nine_seconds_moment = datetime.datetime(2017, 12, 25, 1, 2, 59)
    assert dates.current_seconds_milliseconds(fifty_nine_seconds_moment) == 59
