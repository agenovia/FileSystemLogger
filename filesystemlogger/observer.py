"""
For version 2 of this utility, I want to address the following:
1. asynchronous multiprocessing callbacks
2. graceful handling of unexpected errors--these should either cause the entire program to stop or should be allowed to
    pass through gracefully


Here is the structure:

Coordinator Class (CC) - Consumer
    - this is a consumer class that all processes use to synchronize streams and transfer messages between each other;
        it is not only responsible for assigning tasks to worker classes but also implements subclasses for specific
        logging purposes
    - it takes an incoming stream from all processes and logs that stream (leverage the LoggingEventHandler for this)
        + the method of logging each stream is then entirely handled by this class (i.e. file logging, sql logging,
            a combination, etc.)
        + must have a method to handle as many arbitrary arguments as possible
            = should be easy with proper inheritance (always be mindful of MRO)
    - in addition to managing the logging, it also manages the spawning, coordination, error-handling, and destruction
        of workers

Observer Class (OC) - Producer
    - this is a producer class that is responsible for watching a directory for all changes and propagating messages
        to the CC
    - the observer class contains all the methods for scraping metadata from each file event seen on the root system
    - the observer class should be able to do the following:
        + gather metadata such as:
            = filename
            = file creation date
            = file last modification date
            = owner
            = size
            = type
            = time of event
        + determine directionality of files that transfer around the filesystem (not sure how feasible or useful)
            - what I mean by this is that some events such as a cut or a move really just looks like this to the system:
                    1: file is opened on X location
                    2: a file with the same name is created on Y location
                    3: the file size of the file being created on Y location increases
                    4: once the file on Y location is the same as the file on the X location, the writing stops
                    5: the file on X location is deleted
                = this means that the filesystem only sees a [File Created] event and a [File Deleted] event and doesn't
                    know or really care for that matter that the file was `moved` from X location to Y location
        + handle correct propagation of file rename events since the filesystem cannot distinguish between file renames
            without some inference
            = this is a solved issue, look to previous code for guidance
        + propagate these parameters to the LC queue


Error Correction (EC) - function decorator
    - to be attached to all workers to handle files disappearing before getting queried, in which case just log the
        timestamp of the events themselves and don't worry about scraping metadata--this ensures that the minimal
        relevant information is captured


+++ SQL Logging +++
The Logger utility will be provided by the logger module. Separating so I can consolidate all SQL-related actions

The main problem for SQL logging is that Microsoft SQL Server backend limits the number of inserts to 2100 parameters
wherein each column is a parameter, so a table with 21 columns and 101 rows will cause an exception whereas a table
with 21 columns and 100 rows will not

My current sql_insert code is actually already well-equipped for the task of chunking and inserting to a SQL Server
table, but improvements need to be made as far as timing and callbacks
"""
import datetime
import logging
import multiprocessing as mp
import signal
import time
from datetime import datetime

from queue import Queue
from watchdog.events import FileSystemEventHandler
from watchdog.observers import (Observer,
                                read_directory_changes,
                                )

"""
IMPORTANT

monkeypatching the queue_events method on the WindowsApiEmitter class of the read_directory_changes module. This is 
necessary because the original code does not handle the reacquisition of directory handles if an exception is raised as 
a consequence of the directory being unreachable. To enhance this behavior, I'm monkeypatching the method and adding a 
block of code to reacquire the handle when it is dropped
"""

# assign old method to a new method name
read_directory_changes.WindowsApiEmitter.old_queue_events = read_directory_changes.WindowsApiEmitter.queue_events


# this is the new method
def modified_queue_events(self, timeout):
    failure_time = None
    while True:
        try:
            # calling the old code, but this time I'm handling OSErrors
            self.old_queue_events(timeout)
            break  # if no errors are encountered during the queueing of events, exit loop
        except OSError:  # if initial acquisition fails, try to reacquire
            try:
                if failure_time is None:
                    failure_time = datetime.now()
                # reacquiring the directory handle object, this is necessary for continuing after an OSError is raised
                self.on_thread_start()  # this method raises another OSError if directory is unreachable
                time_elapsed, failure_time = (datetime.now() - failure_time), None
                logging.debug(f'Directory handle reacquired! Time elapsed : {time_elapsed}')
            except OSError:  # if reacquisition fails; log failure, sleep, and then retry (loop continues)
                logging.debug(f'Directory is unreachable, attempting to reacquire directory handle in {timeout} '
                              f'seconds')
                time.sleep(self.timeout)


# applying patch on original code
read_directory_changes.WindowsApiEmitter.queue_events = modified_queue_events
"""end of monkeypatch"""


class EventCoordinator(FileSystemEventHandler):
    """
    This is the event handler. It will handle certain methods for scraping additional information from events that occur

    When scraping events for metainformation, this class will spawn multiple processes on separate threads. This ensures
    the highest chance of capturing all the relevant information for high velocity file movements

    The coordinator will delegate all scraping tasks to an external Scraper class. The Scraper must accept at a minimum
    a watchdog.events.FileSystemEvent object as its first parameter, a successful callback function as its second
    parameter, and an exception callback function as its third parameter.

    The FileSystemEvent object is the object to be scraped; and the callback function is the SQL logger. It should
    therefore be important to make sure that whatever scraper object's return value is handled properly by the logger
    class since the Coordinator exists only to pass events around.

    The coordinator will populate the Queue to be used by the Logger Class. It populates the queue with tuples of the
    structure:
    (EventTime, EventType, SourcePath, SourceDir, SourceFile, DestinationPath, DestinationDir,
    DestinationFile, CreationTime, ModifiedTime, AccessTime, Size, IsDirectory)

    :param workers: This sets the number of workers for the coordinator class. More workers means more asynchronous
        work can be done on a queue of events. What this means is that if the value is set to '4' then the
        coordinator can delegate up to 4 workers who can receive commands if they are 'available'. Be careful in
        attempting to set this value too high as it might overload the computer's resources. Therefore, if the
        coordinator is taking too long to scrape one event, there is at least a non-zero chance that other workers
        are free and can take on the load, ensuring that high velocity file movements are at least logged even if
        the complete metainformation is not captured. The default value is set to 6.
    :param scraper: The scraper is a class that takes in a file event passed by the Coordinator and always returns a
        4-tuple containing:
            [0] the scraped event
                - if an exception is encountered, the event can still at least be meaningful if only by capturing the
                timestamp of the event, the event type, and the source and destination folders
            [1] an exception message
                - if no exceptions occur, then pass None, otherwise pass the exception object back to the coordinator
                for logging purposes
                - it is important that exceptions are captured and handled properly by the scraper in the event that it
                needs to pass it back to the Coordinator
            [2] timestamp of the event from the scraper's frame of reference
                - this is when the scraper finally attempted to scrape the object
                - informs the Coordinator on average time from the event being logged by the Observer to when the first
                scraper can actively work on it
                - use this for optimization and timing
            [3] a multiprocessing Process object returned by multiprocessing.current_process()
                - this is to inform the Coordinator which daemon has done the work
    :param logger: The Logger is a daemon whose primary purpose is to maintain and log a rolling queue of events passed
        by the Coordinator. When initialized for the first time, it will take in details such as which Server, Database,
        and Table to log to. It also must contain startup methods for how and when to log files to a local log (some of
        this should come by default as a debug log). The logger is also responsible for maintaining its own states and
        ensuring that the Coordinator does not shut down until the Logger has done all its work and has logged an
        interaction somewhere. If it is shut down and has to be restarted, it must be capable of picking up from the
        last time it failed (i.e. it must have some mechanism of restarting failed attempts at logging)
        - Some ideas:
            + The logger should have a method that checks for an active operation on the SQL server. This work can only
            be completed in three ways: (1) successful completion; (2) completion with exception; (3) forceful shutdown
            via daemon method. It must therefore NOT react to KeyboardInterrupt or any other such interrupts with a
            sudden exit!
            + The logger should have a forceful exit method that maintains the state of its own queue by writing it to
            disk and validating that only events that have not been logged in SQL yet are stored there
            + The logger should have a startup procedure for checking past failed attempts and attempts to log those
            at the soonest possible time
    """

    def __init__(self, workers: int, scraper, logger=None):
        super().__init__()
        self.workers = workers

        # a pool of workers will perform metadata scraping in parallel
        self.pool = mp.Pool(workers, self.init_worker)
        # just a thought, but we can force the freeing of unused resources by limiting the maxtasksperchild to 1
        # might significantly slow down scraping though as a new worker has to be spawned every time
        # self.pool = mp.Pool(workers, self.init_worker, maxtasksperchild=1)
        self.map = self.pool.map

        # pass a scraper class to use for all events of the structure
        # class MyScraper:
        #     def __init__(self, event):
        self.scraper = scraper

        # logger for events, logger must take in a tuple() as its first parameter
        # class Logger:
        #    def __init__(self, details: tuple):
        self.logger = logger

        self.queue = Queue()

    def __repr__(self):
        return f"EventCoordinator(workers={self.workers}, scraper={self.scraper.__repr__()}, " \
               f"logger={self.logger.__repr__()})"

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

    def terminate_operations(self):
        # TODO add the graceful destruction of the SQL logger here too, logger must be a daemon that implements these
        # TODO methods: (1) a method that wraps up all work remaining in the queue and inserts them to a SQL table or
        # TODO otherwise logs them then terminate; (2) a locking mechanism for the queue that the coordinator has to
        # TODO wait for before it can continue with destruction
        pass

    def terminate_workers(self):
        """This destructor ensures that the workers in the multiprocessing pool have finished all their tasks and are
        exited gracefully"""
        self.pool.terminate()
        self.pool.close()
        self.pool.join()


class EventObserver(Observer):
    def __init__(self, path: str, coordinator: EventCoordinator, timeout: int = 5, recursive: bool = False):
        """
        Context manager for watching directory changes

        :param path: This is the root path of the directory to watch. All subdirectory events will also be captured.
        :param timeout: The timeout in seconds to attempt to query a file event before giving up.
        :return:
        """
        super().__init__(timeout=timeout)
        self.path = path
        self.coordinator = coordinator

        # made this a private variable so it doesn't mess up the parent class' vars
        self._timeout = timeout

        # whether the observer is recursive
        self.recursive = recursive

    def __repr__(self):
        return f"EventObserver(path={self.path}, coordinator={self.coordinator.__repr__()}, timeout={self._timeout}"

    def __enter__(self):
        self.schedule(event_handler=self.coordinator, path=self.path, recursive=self.recursive)
        self.start()
        logging.info(f"Now watching the path '{self.path}'. Recursive: {self.recursive}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Context destructor. Handles destruction of multiprocessing pools in a controlled manner"""
        logging.debug('closing multiprocessing pool, please wait as all workers wrap up...')
        __start = datetime.now()
        # make sure the coordinator terminates all its workers before continuing with observer destruction
        self.coordinator.terminate_workers()
        self.stop()
        self.join()
        __end = datetime.now()
        logging.debug(f"pool successfully closed. {__end - __start} elapsed")
