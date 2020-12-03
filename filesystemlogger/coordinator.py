import logging
import multiprocessing as mp
import signal
import time
import uuid
import json
import re
from os import scandir, remove
from os.path import join
from datetime import datetime
from queue import Queue

from watchdog.events import FileSystemEventHandler


class EventCoordinator(FileSystemEventHandler):
    """
    The coordinator spawns a multiprocessing pool used by the scraper and starts the logger daemon thread. It is
    responsible for logging callbacks made by the scraper and inserting them into the queue which is constantly watched
    by the logger thread. It is also responsible for the graceful destruction of the logger thread and the worker pool.
    The reason for having a multiprocessor pool handling the detail scraping is so that events that take some time to
    finish do not block the entire code's execution and therefore run in parallel.

    The EventCoordinator class is used by the EventObserver class as its event handler. The EventObserver is simply a
    context that initiates the observation of a target directory. When the EventCoordinator receives events from the
    observer, it delegates the scraping to a scraper function; this scraper function is responsible for getting file
    information, formatting it and returns it to the coordinator via a callback function.

    When the EventCoordinator receives scraped events via the callback function of each of the workers, it inserts these
    events to a synchronous queue. The SQLLogger daemon, which always checks the queue, then takes the events, formats
    them for SQL table insertion, and inserts them to the target table.
    """

    def __init__(self, workers: int, scraper, logger, recovery, no_table_logging=False, no_recovery=False):
        super().__init__()
        self.workers = workers
        self.queue = Queue()

        # a pool of workers will perform metadata scraping in parallel
        self.pool = mp.Pool(workers, self.init_worker)

        # this is the scraper function
        self.scraper = scraper

        # initialize the logger daemon
        self.no_table_logging = no_table_logging
        self.logger = logger
        self.logger.queue = self.queue
        self.logger.daemon = True
        if not self.no_table_logging:
            self.logger.start()

        # recovery directory
        self.no_recovery = no_recovery
        self.recovery = recovery

        self.recover()

    def __repr__(self):
        return f"EventCoordinator(workers={self.workers}, scraper={self.scraper.__repr__()}, " \
               f"logger={self.logger.__repr__()})"

    def recover(self):
        """Handles recovery of queue items that were not properly logged in a table during a previous run. This occurs
        before the queue is populated with new items. Recovery is only performed if there is a valid recovery directory
        and if the no_recovery option is not set. Will only look for files matching a specified regex"""

        if isinstance(self.recovery, str) and not self.no_recovery:
            logging.debug(f'recovering from directory {self.recovery}')
            try:
                reg = re.compile('recovery_[a-z0-9]{32}')
                files = [file for file in scandir(self.recovery) if reg.match(file.name)]
                for file in files:
                    with open(file.path, 'r') as stream:
                        _savedqueue = json.load(stream)
                    for item in _savedqueue:
                        self.queue.put_nowait(item)
                    remove(file.path)
            except Exception as e:
                logging.exception(e, exc_info=True)
            logging.debug(f'recovered {self.queue.qsize()} items')

    def scraper_callback(self, cb):
        """This is what the scraper will return in normal operation. This is the object we will use to pass to the
        Logger class. Configure the logger here."""
        logging.debug(cb)
        self.queue.put_nowait(cb)

    @staticmethod
    def scraper_exception(cb):
        """This is the exception message thrown by the scraper if it encounters an error during the scraping of
        metainformation. This exception will be logged by the coordinator class into a file log containing exceptions"""
        logging.exception(cb)

    @staticmethod
    def init_worker():
        """Initialize all workers to ignore all SIGINT signals. Handle the signal elsewhere to gracefully exit the pool
        of workers via KeyboardInterrupt only after they have successfully handled their work. Pass this method as a
        parameter to <multiprocessing.Pool> to initialize states for each worker. Add any other startup scripts here"""
        # treat all signal.SIGINT (interrupt) as signal.SIG_IGN (ignore) in the scope of each worker
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def on_moved(self, event):
        # if there is a scraper provided, then delegate tasks to the scraper and log both its successful callback and
        # its error callback
        if self.scraper is not None:
            self.pool.apply_async(self.scraper, args=[event, time.time()], callback=self.scraper_callback,
                                  error_callback=self.scraper_exception)

    def on_created(self, event):
        # if there is a scraper provided, then delegate tasks to the scraper and log both its successful callback and
        # its error callback
        if self.scraper is not None:
            self.pool.apply_async(self.scraper, args=[event, time.time()], callback=self.scraper_callback,
                                  error_callback=self.scraper_exception)

    def on_deleted(self, event):
        # if there is a scraper provided, then delegate tasks to the scraper and log both its successful callback and
        # its error callback
        if self.scraper is not None:
            self.pool.apply_async(self.scraper, args=[event, time.time()], callback=self.scraper_callback,
                                  error_callback=self.scraper_exception)

    def on_modified(self, event):
        # TODO can we ensure that only the last message in a modified event is actually logged to not flood messages
        pass

    def terminate(self):
        """Call this to terminate the worker pool and the logger daemon gracefully"""
        logging.debug('terminating worker pool')
        self._terminate_workers()
        logging.debug('terminating logger thread')
        self._terminate_logger()

        if not self.queue.empty():
            logging.debug(f'saving {self.queue.qsize()} objects to a recovery file')
            _savequeue = list()
            while not self.queue.empty():
                _savequeue.append(self.queue.get_nowait())
            _filename = f"recovery_{uuid.uuid4().__str__().replace('-', '')}"
            _filepath = join(self.recovery, _filename)
            with open(_filepath, 'w') as f:
                json.dump(_savequeue, f)

    def _terminate_logger(self):
        """This destructor ensures that the logger daemon exits gracefully"""
        if not self.no_table_logging:
            __start = datetime.now()
            self.logger.stop()
            self.logger.join()
            __end = datetime.now()
            logging.debug(f"logger thread successfully stopped. {__end - __start} elapsed")

    def _terminate_workers(self):
        """This destructor ensures that the workers in the multiprocessing pool have finished all their tasks and are
        exited gracefully"""
        logging.debug('closing multiprocessing pool, please wait as all workers wrap up...')
        __start = datetime.now()
        self.pool.terminate()
        self.pool.close()
        self.pool.join()
        __end = datetime.now()
        logging.debug(f"pool successfully closed. {__end - __start} elapsed")
