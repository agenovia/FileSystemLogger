import logging
import signal
import sys
import time
from argparse import ArgumentParser

from filesystemlogger.observer import EventCoordinator, EventObserver
from filesystemlogger.scraper import scrape


def keyboard_interrupt_handler(sig, frame):
    """Make sure keyboard interrupts are handled gracefully so they don't flood stdout"""
    logging.info(f"A KeyboardInterrupt (ID: {sig}) has been captured. Terminating pool of workers and closing the pool")
    # treat all KeyboardInterrupt signals as a graceful exit and return 0 after successfully destroying the pool
    # pool destruction is handled by the context and does not need to be called separately in this scope
    exit(0)


def main(*args, **kwargs):
    # initialize the coordinator with 6 workers and a scraper function
    coordinator = EventCoordinator(workers=int(kwargs['workers']), scraper=scrape)

    # initialize the observer using the path and passing the coordinator
    obs = EventObserver(path=kwargs['directory'],
                        coordinator=coordinator,
                        )

    # handle all interrupts in this scope via a call to keyboard_interrupt_handler
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    # start the context
    with obs:
        while True:
            # just keep looping until an interrupt is captured by the console
            # the observer and coordinator classes only need to be initialized and will operate on their respective
            # threads until destroyed
            time.sleep(1)


def parse_arguments(args):
    parser = ArgumentParser()
    parser.add_argument('-d', '--directory', required=True, help="This is the root directory to log changes.")
    parser.add_argument('-r', '--recursive', action='store_true', required=False,
                        help="Setting this to True will allow the Observer to watch changes for all subdirectories "
                             "under the root directory.")
    parser.add_argument('-w', '--workers', required=False,
                        help="The maximum allowed workers for scraping metainformation. Cannot be less than 1.")
    arguments = parser.parse_args(args)
    return arguments


if __name__ == '__main__':
    from utils.logit import start_logging

    start_logging()

    testing = True

    if testing:
        _cmd = ['--directory', r'\\scdatap1\InformationTechnology\Aaron Genovia\temp\NewFileWatcher',
                '--recursive', '--workers', '6']
        _args = parse_arguments(_cmd)
    else:
        _args = parse_arguments(sys.argv[1:])

    main(**{'directory': _args.directory, 'recursive': _args.recursive, 'workers': _args.workers})
