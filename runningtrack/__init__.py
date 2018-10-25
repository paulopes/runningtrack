# -*- coding: utf-8 -*-

""" "runningtrack" package
"""

from __future__ import print_function, division

import datetime
import os
import sys
import threading
import time
import logging
from inspect import isgenerator
from collections import OrderedDict
from distutils import util
from threading import Thread
from multiprocessing import Process, Queue
from uuid import uuid4


if sys.version_info[0] >= 3:
    xrange = range


from .changes import ChangingFolders, sleep, wait_for_beat_and_get_changed_folders
from .config import load_logging_config, get_config, get_file_type_and_mtime
from .dates import current_date, current_day, current_time
from .const import Const


const = Const(
    DEFAULT_UTC_SETTING=False,
    DEFAULT_BPM_SETTING=0,
    DEFAULT_TIMEOUT_SETTING=300,  # 5 minutes
    DEFAULT_RETRIES_SETTING=2,
    CONFIG_FOLDER='config',
)


try:
    if 'ipykernel' in str(get_ipython()):  # Detect if running inside a Jupyter notebook
        const.IS_A_JUPYTER_NOTEBOOK = True
        from IPython.display import display, HTML
        from ipywidgets import FloatProgress, HBox, Label, Layout
    else:
        const.IS_A_JUPYTER_NOTEBOOK = False
        display = HTML = lambda a: None
        FloatProgress = lambda value, min, max, description: { "value": value }
except NameError:
    const.IS_A_JUPYTER_NOTEBOOK = False
    display = HTML = lambda a: None
    FloatProgress = lambda value, min, max, description: { "value": value }


if not const.IS_A_JUPYTER_NOTEBOOK:
    if os.name == 'nt':
        try:
            from colorama import init
            init()
            const.USING_ANSI = True
        except NameError:
            const.USING_ANSI = False
    else:
        const.USING_ANSI = True
else:
    const.USING_ANSI = False


const.WEEK_DAYS = ('Monday:    ', 'Tuesday:   ', 'Wednesday: ', 'Thursday:  ',
                   'Friday:    ', 'Saturday:  ', 'Sunday:    ')


def wait(track, *tracks):
    # Multiple tracks can be provided as parameters
    # or as a list, tuple, or dict object.
    if type(track) is Track:
        track.wait(*tracks)
    else:
        if type(track) is dict:
            tracks = list(track.values())
        else:
            tracks = list(track)
        tracks[0].wait(*tracks[1:])


def now(track, *tracks):
    # Multiple tracks can be provided as parameters
    # or as a list, tuple, or dict object.
    if type(track) is Track:
        track.now(*tracks)
    else:
        if type(track) is dict:
            tracks = list(track.values())
        else:
            tracks = list(track)
        tracks[0].now(*tracks[1:])


def trace(level):
    from logging import config, NOTSET
    if level != NOTSET:
        config.dictConfig({
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {
                    "format": "[%(asctime).19s,%(msecs)03d] %(levelname)s %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout"
                }
            },
            "root": {
                "level": level,
                "handlers": ["console"]
            }
        })
    else:
        config.dictConfig({
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {
                "console": {
                    "class": "logging.NullHandler"
                }
            },
            "root": {
                "handlers": ["console"]
            }
        })


def debug(enabled=True):
    from logging import DEBUG, NOTSET
    if enabled:
        trace(DEBUG)
    else:
        trace(NOTSET)
    

def info(enabled=True):
    from logging import INFO, NOTSET
    if enabled:
        trace(INFO)
    else:
        trace(NOTSET)


def warning(enabled=True):
    from logging import WARNING, NOTSET
    if enabled:
        trace(WARNING)
    else:
        trace(NOTSET)


def error(enabled=True):
    from logging import ERROR, NOTSET
    if enabled:
        trace(ERROR)
    else:
        trace(NOTSET)


def critical(enabled=True):
    from logging import CRITICAL, NOTSET
    if enabled:
        trace(CRITICAL)
    else:
        trace(NOTSET)


# Multiprocessing logging
# Inspired by: https://gist.github.com/schlamar/7003737
# and: https://gist.github.com/blah238/8ab79c4fe9cdb254f5c37abfc5dc85bf


def log_handler_thread(log_queue):
    while True:
        try:
            record_data = log_queue.get()
            if record_data is None:
                break
            record = logging.makeLogRecord(record_data)

            logger = logging.getLogger(record.name)
            if logger.isEnabledFor(record.levelno):
                logger.handle(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except EOFError:
            break
        except:
            logging.exception('Error in log handler.')


class ConcurrentLogger(logging.Logger):
    log_queue = None
    session = ''

    def isEnabledFor(self, level):
        return True

    def handle(self, record):
        ei = record.exc_info
        if ei:
            # to get traceback text into record.exc_text
            logging._defaultFormatter.format(record)
            record.exc_info = None  # not needed any more
        d = dict(record.__dict__)
        if hasattr(self, "session"):
            d["msg"] = self.session + record.getMessage()
        else:
            d["msg"] = record.getMessage()
        d["args"] = None
        self.log_queue.put(d)


def runner_process(progress_queue, log_queue, track_name, track_args, track_kargs, runner, runner_name, run_id):

    global ConcurrentLogger

    Logger = ConcurrentLogger
    Logger.log_queue = log_queue

    Logger.session = '| ' + track_name + ' ' + runner_name + ' | '
    logging.setLoggerClass(Logger)

    # Monkey patch root logger and already defined loggers
    logging.root.__class__ = Logger
    for logger in logging.Logger.manager.loggerDict.values():
        if not isinstance(logger, logging.PlaceHolder):
            logger.__class__ = Logger

    result = runner(track_name, *track_args, **track_kargs)
    
    # If the runner yields intermediate progress information
    # then it is a generator, and we need to retrieve each
    # yielded progress value (0-100) using a for loop.
    if isgenerator(result):
        progress_value = 0
        for yielded_value in result:
            progress_value = yielded_value
            if type(progress_value) in {list, tuple}:
                progress_value = progress_value[0]
            if isinstance(progress_value, str):
                if progress_value.isnumeric():
                    progress_value = float(progress_value)
                else:
                    progress_value = 0
            elif isinstance(progress_value, float) and 0.0 < progress_value <= 1.0:
                progress_value = int(100.0 * progress_value)
            if progress_value < 0:
                progress_queue.put({runner_name: 0})
            elif progress_value > 100:
                progress_queue.put({runner_name: 100})
            else:
                progress_queue.put({runner_name: yielded_value})

        if progress_value < 100:
            # Signal that this runner is finished and should be terminated
            # even though it didn't yield a final 100 progress indication.
            progress_queue.put({runner_name: 100})
    else:
        # Signal that this runner is finished and should be terminated
        # even though it did not provide any progress information.
        progress_queue.put({runner_name: 100})


def ansi_named_line(text, name=None):
    if name:
        if name not in ansi_named_line.line_owners:
            ansi_named_line.total_lines += 1
            ansi_named_line.line_owners[name] = ansi_named_line.total_lines
            ansi_named_line.last_text[name] = ''

        if ansi_named_line.last_text[name] != text:
            ansi_named_line.last_text[name] = text
            difference = ansi_named_line.current_line - ansi_named_line.line_owners[name]
            if difference > 0:
                sys.stdout.write('\033[{}A'.format(difference))
            elif difference < 0:
                sys.stdout.write('\033[{}B'.format(-difference))
            ansi_named_line.current_line = ansi_named_line.line_owners[name]
            sys.stdout.write(text)
            sys.stdout.flush()
    else:
        ansi_named_line.total_lines += 1
        difference = ansi_named_line.current_line - ansi_named_line.total_lines
        if difference > 0:
            sys.stdout.write('\033[{}A'.format(difference))
        elif difference < 0:
            sys.stdout.write('\033[{}B'.format(-difference))
        ansi_named_line.current_line = ansi_named_line.total_lines
        sys.stdout.write(text)
        sys.stdout.flush()

ansi_named_line.current_line = 0
ansi_named_line.total_lines = 0
ansi_named_line.line_owners = dict()
ansi_named_line.last_text = dict()


class Track(object):

    def watch_folder(self, path, callback=None):
        self._changingFolders.add(path, callback)
        return self

    def load_config(self, config_name):
        if config_name in self._configurators:
            configurators = self._configurators[config_name]
        else:
            configurators = None

        config_path = self._configPath
        path_with_name = os.path.join(config_path, config_name)

        file_type, file_mtime = get_file_type_and_mtime(path_with_name)
        # is_logging_file = config_name == 'logging'
        is_service_file = config_name == self._trackName

        if path_with_name in self._configMtimes:
            if file_mtime > self._configMtimes[path_with_name]:
                # File was changed
                self._configMtimes[path_with_name] = file_mtime
                self._config[config_name] = get_config(path_with_name, file_type)
                if configurators is not None:
                    for configurator in configurators:
                        configurator(self._config)
            else:
                if file_mtime < 0.0:
                    # File was removed
                    del self._configMtimes[path_with_name]
                    self._config[config_name] = OrderedDict()
                    if configurators is not None:
                        for configurator in configurators:
                            configurator(self._config)
        elif file_mtime < 0.0:
            self._config[config_name] = OrderedDict()
            if configurators is not None:
                for configurator in configurators:
                    configurator(self._config)
        else:
            # First time file is to be read
            self._configMtimes[path_with_name] = file_mtime
            self._config[config_name] = get_config(path_with_name, file_type)
            if configurators is not None:
                for configurator in configurators:
                    configurator(self._config)

    def load_config_all(self):
        configs = list(self._configurators.keys())

        if self._trackName:
            config_name = self._trackName
            if config_name in configs:
                self.load_config(config_name)
                configs.remove(config_name)

        for config_name in configs:
            self.load_config(config_name)

        del configs
        return self

    def _run(self, starting_log_message):
        run_timestamp = datetime.datetime.utcnow().isoformat()[:19]  # Strip the fractions of a second

        run_id = uuid4().hex

        logger = logging.getLogger()
        logger.info(starting_log_message)
        logger.debug('Starting run ' + run_id + ' of ' + self._trackName + 'at UTC ISO time ' + run_timestamp)

        runners_progress_queue = Queue()
        run_log_queue = Queue()
        run_log_thread = threading.Thread(target=log_handler_thread, args=(run_log_queue,))
        run_log_thread.daemon = True
        run_log_thread.start()

        self._runs[run_id] = {
            "timestamp": run_timestamp,
            "process_info": dict(),
            "runners_processes": dict(),
            "runners_progress": dict(),
            "runners_progress_queue": runners_progress_queue,
            "log_queue": run_log_queue,
            "log_thread": run_log_thread,
        }

        if const.USING_ANSI:
            ansi_named_line('\r\033[K{}'.format(self._trackName))

        for runner_name in self._runners:
            runner = self._runners[runner_name]
            process = Process(target=runner_process,
                              args=(runners_progress_queue,
                                    run_log_queue,
                                    self._trackName,
                                    self._trackArgs,
                                    self._trackKArgs,
                                    runner,
                                    runner_name,
                                    run_id))
            process.start()
            start_time = time.time()
            self._runs[run_id]["process_info"][runner_name] = (process, start_time, start_time, runner, 0)
            self._runs[run_id]["runners_processes"][runner_name] = process
            self._runs[run_id]["runners_progress"][runner_name] = 0
            if const.IS_A_JUPYTER_NOTEBOOK:
                self._runners_progress_bars[runner_name].value = 0
                self._runners_progress_bars_previous_value[runner_name] = 0
                self._runners_progress_descriptions[runner_name].value = ''
            elif const.USING_ANSI:
                name = self._trackName + runner_name
                ansi_named_line('\r\033[K  {}: 0%'.format(runner_name), name)

        if run_id in self._runs:
            process_count = len(self._runs[run_id]["process_info"].keys())
            if process_count > 1:
                logger.debug(str(process_count) + ' processes for run ' + run_id + ' have been launched.')
            elif process_count == 1:
                logger.debug('One process for run ' + run_id + ' has been launched.')
            else:
                logger.debug('No processes for run ' + run_id + ' have been launched.')

        return self

    def _timeoutRuns(self):
        logger = logging.getLogger()
        runs_to_delete = list()
        for run_id in self._runs:
            
            # Make sure that the runnner's progress information for each run
            # has been updated with the latest yielded progress values.
            progress_queue = self._runs[run_id]["runners_progress_queue"]
            while not progress_queue.empty():
                progress_updates = progress_queue.get()
                self._runs[run_id]["runners_progress"].update(progress_updates)

                # Reset the time of the last_progress_update
                # of each runner that provided a new progress value
                # because that means that it is not stuck.
                for runner_name in progress_updates:
                    process, start, last_progress_update, runner, timeout = \
                        self._runs[run_id]["process_info"][runner_name]
                    self._runs[run_id]["process_info"][runner_name] = \
                        (process, start, time.time(), runner, timeout)

                if const.IS_A_JUPYTER_NOTEBOOK:
                    for runner_name in progress_updates:
                        if type(progress_updates[runner_name]) in {list, tuple}:
                            value = progress_updates[runner_name][0]
                            if value != self._runners_progress_bars_previous_value[runner_name]:
                                self._runners_progress_bars[runner_name].value = value
                                self._runners_progress_bars_previous_value[runner_name] = value
                            label = progress_updates[runner_name][1]
                            self._runners_progress_descriptions[runner_name].value = label
                        else:
                            value = progress_updates[runner_name]
                            if value != self._runners_progress_bars_previous_value[runner_name]:
                                self._runners_progress_bars[runner_name].value = value
                                self._runners_progress_bars_previous_value[runner_name] = value
                            if value >= 100:
                                self._runners_progress_descriptions[runner_name].value = ''
                elif const.USING_ANSI:
                    for runner_name in progress_updates:
                        if type(progress_updates[runner_name]) in {list, tuple}:
                            value = progress_updates[runner_name][0]
                            label = progress_updates[runner_name][1]
                        else:
                            value = progress_updates[runner_name]
                            label = ''
                        name = self._trackName + runner_name
                        if label:
                            ansi_named_line('\r\033[K  {}: {}% - {}'.format(runner_name, value, label), name)
                        else:
                            ansi_named_line('\r\033[K  {}: {}%'.format(runner_name, value), name)

            no_processes_running = True
            timeout = self._timeoutSetting
            retries = self._retriesSetting

            for runner_name in self._runs[run_id]["process_info"]:
                process, start, last_progress_update, runner, retry_count = self._runs[run_id]["process_info"][runner_name]
                runner_progress = self._runs[run_id]["runners_progress"][runner_name]
                if type(runner_progress) in {list, tuple}:
                    runner_progress = runner_progress[0]
                if process.is_alive():
                    if runner_progress >= 100:
                        # Rare case when process may still be alive after the
                        # runner is finished. We give it a second more before
                        # explicitly terminating it.
                        if time.time() - last_progress_update > 1:
                            process.terminate()

                    if time.time() - last_progress_update > timeout > 0:
                        logger.error('Timeout of {} {} at {} seconds.'.format(self._trackName, runner_name, timeout))
                        process.terminate()
                        if retry_count < retries:
                            new_retry = retry_count + 1
                            logger.error('Retry number {} of {} {}.'.format(new_retry, self._trackName, runner_name))
                            run_log_queue = self._runs[run_id]["log_queue"]
                            new_process = Process(target=runner_process,
                                                    args=(progress_queue,
                                                            run_log_queue,
                                                            self._trackName,
                                                            self._trackArgs,
                                                            self._trackKArgs,
                                                            runner,
                                                            runner_name,
                                                            run_id))
                            new_process.start()
                            start_time = time.time()
                            self._runs[run_id]["process_info"][runner_name] = (new_process, start_time, start_time, runner, new_retry)
                            self._runs[run_id]["runners_processes"][runner_name] = new_process
                            self._runs[run_id]["runners_progress"][runner_name] = 0
                            no_processes_running = False
                        else:
                            logger.error('The {} {} runner was canceled after {} retries.'.format(self._trackName, runner_name, retry_count))
                    else:
                        no_processes_running = False

            if no_processes_running:
                for runner_name in self._runs[run_id]["process_info"]:
                    process, start, last_progress_update, runner, retry_count = self._runs[run_id]["process_info"][runner_name]
                    process.join()
                    runs_to_delete.append(run_id)

            # Provide progress of this run based on the progress of its runners.
            runner_count = len(self._runs[run_id]["runners_progress"])
            if runner_count > 0:
                sum_of_runners_progress = 0
                for runner_name in self._runs[run_id]["runners_progress"]:
                    runner_progress = self._runs[run_id]["runners_progress"][runner_name]
                    if type(runner_progress) in {list, tuple}:
                        runner_progress = runner_progress[0]
                    sum_of_runners_progress += runner_progress
                progress = int(sum_of_runners_progress / runner_count)

                self._progressQueue.put((run_id, progress))

        for run_id in runs_to_delete:
            if run_id in self._runs:
                logger.debug('Ending {} run {}'.format(self._trackName, run_id))
                self._runs[run_id]["log_queue"].put(None)
                del self._runs[run_id]["timestamp"]
                del self._runs[run_id]["log_queue"]
                del self._runs[run_id]["process_info"]
                del self._runs[run_id]["runners_processes"]
                del self._runs[run_id]["runners_progress"]
                del self._runs[run_id]["log_thread"]
                del self._runs[run_id]

        return self

    def _scheduled(self):

        def normalized_time(moment):
            pm_delta = 0
            am = False
            pm = False
            clean_moment = moment
            if moment.endswith('PM'):
                clean_moment = moment[:-2]
                pm = True
                pm_delta = 12
            elif clean_moment.endswith('AM'):
                am = True
                clean_moment = moment[:-2]
            items = clean_moment.split(':')
            if len(items) < 1:
                return '00:00'
            else:
                hours = int(items[0])
                if hours is 12:
                    if am:
                        pm_delta = -12
                    elif pm:
                        pm_delta = 0
                if len(items) < 2:
                    return str(pm_delta + hours).zfill(2) + ':00'
                else:
                    return str(pm_delta + hours).zfill(2) + ':' + str(int(items[1])).zfill(2)

        def schedule_parse(day_schedule):
            return ' '.join(day_schedule.split()).upper().replace(' AM', 'AM').replace(' PM', 'PM').split()

        def get_schedule_from_config():
            service_config = self._config[self._trackName]
            if "schedule" in self._override:
                service_config["schedule"] = self._override["schedule"]

            if "schedule" in service_config:
                service_schedule = service_config["schedule"]

                if "sunday" in service_schedule:
                    sunday = service_schedule["sunday"]
                else:
                    sunday = ''

                if "monday" in service_schedule:
                    monday = service_schedule["monday"]
                else:
                    monday = ''

                if "tuesday" in service_schedule:
                    tuesday = service_schedule["tuesday"]
                else:
                    tuesday = ''

                if "wednesday" in service_schedule:
                    wednesday = service_schedule["wednesday"]
                else:
                    wednesday = ''

                if "thursday" in service_schedule:
                    thursday = service_schedule["thursday"]
                else:
                    thursday = ''

                if "friday" in service_schedule:
                    friday = service_schedule["friday"]
                else:
                    friday = ''

                if "saturday" in service_schedule:
                    saturday = service_schedule["saturday"]
                else:
                    saturday = ''

            else:
                sunday = monday = tuesday = wednesday = thursday = friday = saturday = ''

            times = [
                sorted(( normalized_time(x) for x in schedule_parse(monday) ) if monday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(tuesday) ) if tuesday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(wednesday) ) if wednesday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(thursday) ) if thursday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(friday) ) if friday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(saturday) ) if saturday else tuple()),
                sorted(( normalized_time(x) for x in schedule_parse(sunday) ) if sunday else tuple()),
            ]

            for day in xrange(0, 7):
                schedule_description = self._trackName + '  ' + const.WEEK_DAYS[day]
                for schedule in times[day]:
                    schedule_description += schedule + ' '
                logger.info(schedule_description)

            logger.info('Waiting for next scheduled run of {}.'.format(self._trackName))

            for day in xrange(0, 7):
                times[day] = set(times[day])

            return times

        # Here begins the heart of the functionality of this class.
        logger = logging.getLogger()
        logger.info(current_date(self._utcSetting) + ' ' +
                    current_time(self._utcSetting) + ' ' +
                    const.WEEK_DAYS[current_day(self._utcSetting)])
        logger.info('Starting {} according to the following schedule:'.format(self._trackName))

        scheduled_times = get_schedule_from_config()
        previous_occurrence = ''
        okay = True
        while okay and not self._terminateNow:
            today = current_date(self._utcSetting)
            now = current_time(self._utcSetting)
            week_day = current_day(self._utcSetting)

            todays_schedule = scheduled_times[week_day]
            items_in_schedule = len(todays_schedule)

            if (items_in_schedule > 0 and now in todays_schedule and today + now != previous_occurrence or
                    items_in_schedule == 0 and self._bpmSetting > 0):
                self._run(today + ' ' + now + ' ' + const.WEEK_DAYS[week_day])
                previous_occurrence = today + now

            if self._bpmSetting <= 0:
                wait_period = 1
            else:
                wait_period = 60 / self._bpmSetting
            changed_folders = wait_for_beat_and_get_changed_folders(wait_period, self._changingFolders)
            if self._terminateNow or (changed_folders is None):
                okay = False  # May have been interrupted by the keyboard (Control-C or Break key in Windows)
            else:
                for path in changed_folders:
                    # This is the only place where folder change callbacks will be automatically called
                    # but there may be other places where a particular callback is specifically called.
                    if self._configPath and path == self._configPath:
                        self.load_config_all()
                        scheduled_times = get_schedule_from_config()
                    else:
                        callback = changed_folders[path]
                        if callback is not None:
                            callback(path)

            self._timeoutRuns()

        today = current_date(self._utcSetting)
        now = current_time(self._utcSetting)
        week_day = current_day(self._utcSetting)
        logger.info(today + ' ' + now + ' ' + const.WEEK_DAYS[week_day])
        logger.info('Stopping {} scheduled runs.'.format(self._trackName))
        return self

    def _serviceOptionsConfigurator(self, config):
        service_config = config[self._trackName]
        if "options" not in service_config:
            service_config["options"] = OrderedDict()
        options_service_config = service_config["options"]

        if "timeout" in self._override:
            self._timeoutSetting = self._override["timeout"]
        elif "options" in service_config and (options_service_config is not None and
                                              "timeout" in options_service_config):
            timeout_setting = options_service_config["timeout"]
            if type(timeout_setting) is str:
                self._timeoutSetting = bool(util.strtobool(timeout_setting))
            else:
                self._timeoutSetting = timeout_setting
        else:
            self._timeoutSetting = const.DEFAULT_TIMEOUT_SETTING

        if "retries" in self._override:
            self._retriesSetting = self._override["retries"]
        elif "options" in service_config and (options_service_config is not None and
                                              "retries" in options_service_config):
            retries_setting = options_service_config["retries"]
            if type(timeout_setting) is str:
                self._retriesSetting = bool(util.strtobool(retries_setting))
            else:
                self._retriesSetting = retries_setting
        else:
            self._retriesSetting = const.DEFAULT_RETRIES_SETTING

        if "bmp" in self._override:
            self._bpmSetting = self._override["bmp"]
        if "options" in service_config and (options_service_config is not None and
                                            "bpm" in options_service_config):
            self._bpmSetting = int(float(options_service_config["bpm"]))
        else:
            self._bpmSetting = const.DEFAULT_BPM_SETTING

        if "utc" in self._override:
            self._utcSetting = self._override["utc"]
        elif "options" in service_config and (options_service_config is not None and
                                              "utc" in options_service_config):
            utc_setting = options_service_config["utc"]
            if type(utc_setting) is str:
                self._utcSetting = bool(util.strtobool(utc_setting))
            else:
                self._utcSetting = utc_setting
        else:
            self._utcSetting = const.DEFAULT_UTC_SETTING

        if self._utcSetting:
            logging.Formatter.converter = time.gmtime

        return self

    def _bootstrap(self):
        if self._notBootstrapped:
            # First time config is loaded, if there is a config file.
            self.add_config(self._trackName, self._serviceOptionsConfigurator)

            try:
                script_path = os.path.dirname(os.path.realpath(sys.modules["__main__"].__file__))
            except AttributeError:
                script_path = os.getcwd()

            self._configPath = os.path.join(script_path, const.CONFIG_FOLDER)
            self.load_config_all()
            self.watch_folder(self._configPath)

            self.watch_folder(self._configPath)

            self._progressQueue = Queue()

            self._notBootstrapped = False
        return self

    def _await(self):
        self._bootstrap()._scheduled()
        return self

    def wait(self, *other_tracks):
        self._start()
        try:
            for other_track in other_tracks:
                other_track._start()
                other_track._awaitThread.join()
            self._awaitThread.join()
        except KeyboardInterrupt:
            pass  # This would be an expected exception that should not be raised.

        for other_track in other_tracks:
            other_track._stop()
        self._stop()
        return self

    def now(self, *other_tracks):
        self._bootstrap()
        self._run('Immediate {} run.'.format(self._trackName))

        progress = {
            self._trackName: 0,
        }
        if const.IS_A_JUPYTER_NOTEBOOK:
            progress_bar = FloatProgress(value=0, min=0, max=100, description=self._trackName)
            progress_bars = {
                self._trackName: progress_bar,
            }
            display(progress_bar)
        previous_progress_message = '{} total progress: {}%'.format(
            self._trackName,
            progress[self._trackName])

        run_count = len(self._runs)
        for other_track in other_tracks:
            other_track._bootstrap()
            other_track._run('Immediate {} run.'.format(other_track._trackName))
            run_count += len(other_track._runs)
            progress[other_track._trackName] = 0
            if const.IS_A_JUPYTER_NOTEBOOK:
                progress_bar = FloatProgress(value=0, min=0, max=100, description=other_track._trackName)
                progress_bars[other_track._trackName] = progress_bar
                display(progress_bar)
            previous_progress_message += '{} total progress: {}%'.format(
                other_track._trackName,
                progress[other_track._trackName])

        if not const.IS_A_JUPYTER_NOTEBOOK:
            if const.USING_ANSI:
                ansi_named_line('\r\033[K' + previous_progress_message, self._trackName)
            else:
                sys.stdout.write(previous_progress_message)
                sys.stdout.flush()

        while run_count > 0:
            # Check for timeouts in any of the processes of each run.
            if len(self._runs) > 0: self._timeoutRuns()

            # Show progress.
            progress_message = ''
            if self._progressQueue and not self._progressQueue.empty():
                while not self._progressQueue.empty():
                    run_id, progress_value = self._progressQueue.get()
                    progress[self._trackName] = progress_value
                    if const.IS_A_JUPYTER_NOTEBOOK:
                        progress_bars[self._trackName].value = progress_value
                    elif const.USING_ANSI:
                        ansi_named_line('\r\033[K' + 
                            '{} total progress: {}%'.format(
                                self._trackName, progress_value),
                            self._trackName)

            progress_message += '{} total progress: {}%'.format(
                self._trackName,
                progress[self._trackName])

            for other_track in other_tracks:
                if len(other_track._runs) > 0: other_track._timeoutRuns()

                if other_track._progressQueue and not other_track._progressQueue.empty():
                    while not other_track._progressQueue.empty():
                        run_id, progress_value = other_track._progressQueue.get()
                        progress[other_track._trackName] = progress_value
                        if const.IS_A_JUPYTER_NOTEBOOK:
                            progress_bars[other_track._trackName].value = progress_value
                progress_message += '{} total progress: {}%'.format(
                    other_track._trackName,
                    progress[other_track._trackName])

            if progress_message != previous_progress_message:
                if not const.IS_A_JUPYTER_NOTEBOOK:
                    if const.USING_ANSI:
                        ansi_named_line('\r\033[K' + progress_message,
                                        self._trackName)
                    else:
                        sys.stdout.write('\b' * len(previous_progress_message) +
                                         progress_message)
                        sys.stdout.flush()
                previous_progress_message = progress_message

            sleep(0.1)
            
            # After .1 seconds check again if there are still any active runs.
            run_count = len(self._runs)
            for other_track in other_tracks:
                run_count += len(other_track._runs)

        return self

    def _start(self):
        if self._awaitThread is None:
            self._terminateNow = False
            self._awaitThread = Thread(target=self._await, args=tuple())
            self._awaitThread.start()
        return self

    def _stop(self):
        if self._awaitThread is not None:
            self._terminateNow = True
            self._awaitThread.join()
            self._awaitThread = None
        return self

    def add_config(self, name, now=lambda config: None):
        config_name = name.strip().lower()
        if config_name not in self._configurators:
            self._configurators[config_name] = [now]
        else:
            self._configurators[config_name].append(now)
        return self

    def lanes(self, *runners, **runners_by_name):
        for runner in runners:
            self._runners[runner.__name__] = runner

        for runner_name in runners_by_name:
            self._runners[runner_name] = runners_by_name[runner_name]

        if const.IS_A_JUPYTER_NOTEBOOK:
            display(HTML('<h4>{}</h4>'.format(self._trackName)))
            for runner_name in self._runners:
                progress_bar = FloatProgress(value=0, min=0, max=100)
                progress_runner_name = Label(runner_name, layout=Layout(width='15em'))
                progress_description = Label('', layout=Layout(width='65em'))
                self._runners_progress_bars[runner_name] = progress_bar
                self._runners_progress_bars_previous_value[runner_name] = 0
                self._runners_progress_runner_names[runner_name] = progress_runner_name
                self._runners_progress_descriptions[runner_name] = progress_description
                display(HBox([progress_runner_name, progress_bar, progress_description]))

        return self

    def at(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["sunday"] = str(times)
        self._override["schedule"]["monday"] = str(times)
        self._override["schedule"]["tuesday"] = str(times)
        self._override["schedule"]["wednesday"] = str(times)
        self._override["schedule"]["thursday"] = str(times)
        self._override["schedule"]["friday"] = str(times)
        self._override["schedule"]["saturday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def sunday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["sunday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def monday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["monday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def tuesday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["tuesday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def wednesday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["wednesday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def thursday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["thursday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def friday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["friday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def saturday(self, times):
        if "schedule" not in self._override:
            self._override["schedule"] = dict()
        self._override["schedule"]["saturday"] = str(times)
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def timeout(self, seconds=const.DEFAULT_TIMEOUT_SETTING):
        self._override["timeout"] = int(float(seconds))
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def retries(self, count=const.DEFAULT_RETRIES_SETTING):
        self._override["retries"] = int(float(count))
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def bmp(self, seconds=const.DEFAULT_BPM_SETTING):
        self._override["bmp"] = int(float(seconds))
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def debug(self, enable=True):
        if enable:
            self._override["debug"] = DEBUG_SET_TO_TRUE
        else:
            self._override["debug"] = DEBUG_SET_TO_FALSE
        if self._awaitThread is not None:
            self._stop()._start()
        return self

    def __init__(self, track_name, *track_args, **track_kargs):
        self._trackName = track_name.strip().lower()
        self._trackArgs = track_args
        self._trackKArgs = track_kargs
        self._configPath = None
        self._notBootstrapped = True
        self._awaitThread = None
        self._terminateNow = False
        self._config = dict()
        self._configMtimes = dict()
        self._configurators = dict()
        self._runners = OrderedDict()
        self._runners_progress_bars = dict()
        self._runners_progress_bars_previous_value = dict()
        self._runners_progress_runner_names = dict()
        self._runners_progress_descriptions = dict()
        self._plugins = dict()
        self._pluginsConfigName = dict()
        self._pluginsConfigSection = dict()
        self._pluginParams = dict()
        self._changingFolders = ChangingFolders()
        self._progressQueue = None
        self._utcSetting = const.DEFAULT_UTC_SETTING
        self._bpmSetting = const.DEFAULT_BPM_SETTING
        self._timeoutSetting = const.DEFAULT_TIMEOUT_SETTING
        self._retriesSetting = const.DEFAULT_RETRIES_SETTING
        self._override = dict()
        self._runs = dict()
