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
import time
from datetime import datetime

from watchdog.observers import (Observer,
                                read_directory_changes,
                                )

from filesystemlogger.coordinator import EventCoordinator

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


class EventObserver(Observer):
    def __init__(self, path: str, coordinator: EventCoordinator, timeout: int = 5, recursive: bool = False):
        """
        Context manager for watching directory changes. The EventObserver simply watches a directory for changes and
        delegates all other tasks to the EventCoordinator. When the context is closed, the EventObserver tells the
        EventCoordinator to stop and exit gracefully.
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
        """Context destructor. Notifies the EventCoordinator to cease before terminating the EventObserver."""
        # tell the coordinator to terminate the logger and all workers before continuing with observer destruction
        self.coordinator.terminate()
        logging.debug('terminating observer')
        __start = datetime.now()
        self.stop()
        self.join()
        __end = datetime.now()
        logging.debug(f"observer successfully terminated. {__end - __start} elapsed")
