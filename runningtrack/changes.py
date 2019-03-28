# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals

import errno
import sys
import os
import time
import imp
from glob import glob

from .dates import current_seconds_milliseconds, current_seconds


if sys.version_info[0] >= 3:
    xrange = range

    try:
        # Python >= 3.4
        from importlib.util import find_spec

        # Determine if a top-level python module or package has been pre-installed in site-packages.
        def module_is_installed(module_name):
            return find_spec(module_name) is not None

    except ImportError:
        # for Python 3.3
        from importlib import find_loader

        # Determine if a top-level python module or package has been pre-installed in site-packages.
        def module_is_installed(module_name):
            return find_loader(module_name) is not None

else:
    # Determine if a top-level python module or package has been pre-installed in site-packages.
    def module_is_installed(module_name):
        try:
            imp.find_module(module_name)
            found = True
        except ImportError:
            found = False
        return found


# Example usage of the function module_is_installed().
USING_WIN32 = module_is_installed("win32file")


def sleep(seconds):
    okay = True
    try:
        time.sleep(seconds)
    except IOError as e:
        if e.errno != errno.EINTR:
            raise
    except KeyboardInterrupt:
        okay = False
    return okay


if USING_WIN32:
    import win32file
    import win32con
    import win32event
    import pywintypes

    class FolderChanges(object):

        flags = (win32con.FILE_NOTIFY_CHANGE_FILE_NAME |
                 win32con.FILE_NOTIFY_CHANGE_DIR_NAME |
                 win32con.FILE_NOTIFY_CHANGE_ATTRIBUTES |
                 win32con.FILE_NOTIFY_CHANGE_SIZE |
                 win32con.FILE_NOTIFY_CHANGE_LAST_WRITE |
                 win32con.FILE_NOTIFY_CHANGE_SECURITY)

        def __init__(self, path):
            try:
                self.change_handle = win32file.FindFirstChangeNotification(
                        path, 0, FolderChanges.flags)
            except pywintypes.error:
                pass  # This happens when the config folder doesn't exist
            self.changed = False


    class ChangingFolders(object):

        def get_callback(self, callback_path):
            if callback_path in self.paths:
                return self.callbacks[callback_path]
            else:
                return lambda path: {"error": 'Path ' + path + ' not found in callbacks.'}

        def add(self, path, callback=None):
            folder_changes = FolderChanges(path)
            self.paths.append(path)
            self.callbacks[path] = callback
            self.folder_changes.append(folder_changes)
            try:
                self.win32_handles.append(folder_changes.change_handle)
            except AttributeError:
                pass  # This happens when the config folder doesn't exist

        def __init__(self):
            self.paths = list()
            self.callbacks = dict()
            self.folder_changes = list()
            self.win32_handles = list()


    def wait_for_beat_and_get_changed_folders(wait_period, changing_folders):

        returning_changed_folders = dict()

        # Make sure that the interval does not include milliseconds, yet still promoting
        # that calculations using interval can include milliseconds in their results.
        interval = round(wait_period, 0)
        this_second = current_seconds_milliseconds()
        max_seconds = float(int(this_second + interval))
        if max_seconds > 60.0:
            max_seconds = 1.0
        wait_for = round(interval - this_second % interval, 3)
        interrupter = None
        while (wait_for >= 60.0) or (0.0 <= this_second < max_seconds):
            try:
                interrupter = win32event.WaitForMultipleObjects(
                    changing_folders.win32_handles, 0,
                    int(1000.0 * wait_for)
                ) - win32event.WAIT_OBJECT_0
            except IOError as e:
                if e.errno != errno.EINTR:
                    raise
            except KeyboardInterrupt:
                return None
            except pywintypes.error:
                pass  # This happens when the config folder doesn't exist

            this_second = current_seconds_milliseconds()
            if max_seconds > 59.0:
                max_seconds = 0.0
            wait_for = round(interval - this_second % interval, 3)
            
            if interrupter is not None and interrupter < len(changing_folders.win32_handles):
                returning_changed_folders[changing_folders.paths[interrupter]] =\
                    changing_folders.callbacks[changing_folders.paths[interrupter]]
                win32file.FindNextChangeNotification(changing_folders.win32_handles[interrupter])

        return returning_changed_folders

else:

    class FolderChanges(object):

        def check(self):
            try:
                timestamp = os.stat(self.path).st_mtime
            except OSError:
                timestamp = 0
            if timestamp != self.timestamp:
                self.changed = True
                self.timestamp = timestamp

        def reset(self):
            self.changed = False

        def __init__(self, path):
            self.path = path
            try:
                self.timestamp = os.stat(path).st_mtime
            except OSError:
                self.timestamp = 0
            self.changed = False


    class ChangingFolders(object):

        def get_callback(self, callback_path):
            if callback_path in self.callbacks:
                return self.callbacks[callback_path]
            else:
                return lambda path: {"error": 'Path ' + path + ' not found in callbacks.'}

        def add(self, path, callback=None):
            folder_changes = FolderChanges(path)
            self.paths.append(path)
            self.callbacks[path] = callback
            self.folder_changes.append(folder_changes)

        def __init__(self):
            self.paths = list()
            self.callbacks = dict()
            self.folder_changes = list()


    def wait_for_beat_and_get_changed_folders(wait_period, changing_folders):

        returning_changed_folders = dict()

        # Make sure that the interval does not include milliseconds, yet still promoting
        # that calculations using interval can include milliseconds in their results.
        interval = round(wait_period, 0)
        wait_for = round(interval - current_seconds() % interval, 3)

        if not sleep(wait_for):
            return None
        else:

            for i in xrange(0, len(changing_folders.paths)):
                folder_changes = changing_folders.folder_changes[i]
                folder_changes.check()
                if folder_changes.changed:
                    returning_changed_folders[changing_folders.paths[i]] =\
                        changing_folders.callbacks[changing_folders.paths[i]]
                    folder_changes.reset()

            return returning_changed_folders


def every_file_in(path, filter=None):
    changing_folders = ChangingFolders()
    changing_folders.add(path)

    if filter:
        path_filter = os.path.join(path, filter)
    else:
        path_filter = os.path.join(path, '*')

    file_list = sorted(glob(path_filter))
    for file_path in file_list:
        file_name = file_path[len(path):]
        if len(file_name) > 0 and file_name[0] == os.sep:
            yield file_name[1:]
        else:
            yield file_name
    file_path_set = set(file_list)

    wait_period = 1
    while wait_for_beat_and_get_changed_folders(wait_period, changing_folders) is not None:
        file_list = sorted(glob(path_filter))
        for file_path in file_list:
            if file_path not in file_path_set:
                file_name = file_path[len(path):]
                if len(file_name) > 0 and file_name[0] == os.sep:
                    yield file_name[1:]
                else:
                    yield file_name
        file_path_set = set(file_list)


class FileConsumer(object):
    
    def __init__(self, path, filter=None):
        self.cancel = False
        self.path = path
        self.chunk = None

        self._changing_folders = None
        self._file_path_set = set()
        self._iterable = None
        self._wait_period = 1

        if filter:
            self._path_filter = os.path.join(path, filter)
        else:
            self._path_filter = os.path.join(path, '*')

    def _get_first_chunk(self):
        self._changing_folders = ChangingFolders()
        self._changing_folders.add(self.path)

        self.chunk = sorted(glob(self._path_filter))

    def poll(self, timeout=None):
        if self.is_idle():
            polling_set = set(glob(self._path_filter))
            if polling_set <= self._file_path_set:
                waiting_for = 0
                while timeout is None or waiting_for < timeout:
                    waiting_for += self._wait_period
                    if wait_for_beat_and_get_changed_folders(self._wait_period, self._changing_folders) is not None:
                        polling_chunk = sorted(glob(self._path_filter))
                        got_one = False
                        file_path = None
                        while not got_one and len(polling_chunk) > 0:
                            file_path = polling_chunk.pop(0)
                            if file_path not in self._file_path_set:
                                got_one = True
                        if got_one:
                            if self._iterable is None:
                                self._iterable = self._generator()
                            return next(self._iterable)
                return None  # Timed out
            else:
                if self._iterable is None:
                    self._iterable = self._generator()
                return next(self._iterable)
        else:
            if self._iterable is None:
                self._iterable = self._generator()
            return next(self._iterable)

    def _generator(self):
        if not self.cancel:

            if self.chunk is None:
                self._get_first_chunk()

            while len(self.chunk) > 0:
                file_path = self.chunk.pop(0)
                self._file_path_set.add(file_path)
                file_name = file_path[len(self.path):]
                if len(file_name) > 0 and file_name[0] == os.sep:
                    yield file_name[1:]
                else:
                    yield file_name
                if self.cancel:
                    break

            while wait_for_beat_and_get_changed_folders(self._wait_period, self._changing_folders) is not None:
                self.chunk = sorted(glob(self._path_filter))
                while len(self.chunk) > 0:
                    file_path = self.chunk.pop(0)
                    if file_path not in self._file_path_set:
                        self._file_path_set.add(file_path)
                        file_name = file_path[len(self.path):]
                        if len(file_name) > 0 and file_name[0] == os.sep:
                            yield file_name[1:]
                        else:
                            yield file_name
                    if self.cancel:
                        break

    def __iter__(self):
        if self.chunk is None:
            self._get_first_chunk()
        if self._iterable is None:
            self._iterable = self._generator()
        return self._iterable

    def __len__(self):
        if self.chunk is None:
            self._get_first_chunk()
        return len(self.chunk)

    def is_idle(self):
        if len(self) == 0:
            if len(set(glob(self._path_filter)) - self._file_path_set) > 0:
                return False
            else:
                return True
        else:
            return False
