# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals

import os
import sys

from .const import Const


if os.name == 'nt':
    try:
        from colorama import init
        init()
    except:
        pass


class Track(Const):
    DETAIL_NONE = 0
    DETAIL_BRIEF = 1
    DETAIL_FULL = 2

    def __init__(self, *args, **kargs):
        self.override = self.DETAIL_NONE
        self.current_line = 0
        self.total_lines = 0
        self.line_owners = dict()
        self.last_text = dict()
        self.tracking_initialized = False
        self.first_time = True
        super(Track, self).__init__(*args, **kargs)

    def brief(self):
        self.override = self.DETAIL_BRIEF
        return self

    def full(self):
        self.override = self.DETAIL_FULL
        return self

    def kickoff_tracking(self):
        if not self.tracking_initialized:
            detail_levels = {
                'BRIEF': self.DETAIL_BRIEF,
                'FULL':  self.DETAIL_FULL,
            }
            try:
                self.DETAIL = self.override if self.override else (
                    detail_levels[self.TRACK_DETAIL.strip().upper()])
            except:
                self.DETAIL = self.DETAIL_BRIEF
            self.tracking_initialized = True
        return self

    def print_line(self, text=None, name=None):
        if type(text) is not str:
            text = ''

        if self.first_time:
            # Initial push-down. Push-downs are needed for Mac's Terminal.
            sys.stdout.write('\n\033[1A')
            self.first_time = False
            
        if name:
            push_down = False
            if name not in self.line_owners:
                self.total_lines += 1
                self.line_owners[name] = self.total_lines
                self.last_text[name] = ''
                push_down = True

            if self.last_text[name] != text or not text:
                self.last_text[name] = text
                difference = self.current_line - self.line_owners[name]
                if difference > 0:
                    sys.stdout.write('\033[{}A'.format(difference))
                elif difference < 0:
                    sys.stdout.write('\033[{}B'.format(-difference))
                self.current_line = self.line_owners[name]
                if push_down:
                    sys.stdout.write('\n\033[1A')
                sys.stdout.write('\r\033[K\033[0m' + text)
        else:
            self.total_lines += 1
            difference = self.current_line - self.total_lines
            if difference < 0:
                sys.stdout.write('\033[{}B'.format(-difference))
            self.current_line = self.total_lines
            sys.stdout.write('\n\033[1A\r\033[K\033[0m' + text)

        sys.stdout.flush()

    @staticmethod
    def percentage_bar(value, length=50):
        if value > 102:
            # Red
            return '\033[41m' + ' '*length + '\033[0m'
        elif value == 102:
            # Yellow
            return '\033[43m' + ' '*length + '\033[0m'
        elif value == 101:
            # Green
            return '\033[42m' + ' '*length + '\033[0m'
        elif value == 100:
            # Cyan
            return '\033[46m' + ' '*length + '\033[0m'
        else:
            ratio = 100.0 / length
            complete = int(value // ratio)
            remaining = length - 1 - complete
            # Cyan over Grey
            return '\033[46m' + ' '*complete + '\033[36;47m ' + ' '*remaining + '\033[0m'

# This can be overidden by an environment variable:
track = Track(
    TRACK_DETAIL='',
)
