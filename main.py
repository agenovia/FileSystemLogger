import logging
import signal
import sys
import time
from argparse import ArgumentParser
import yaml

from filesystemlogger.observer import EventObserver
from filesystemlogger.coordinator import EventCoordinator
from filesystemlogger.scraper import scrape
from filesystemlogger.logger import SQLLogger


def keyboard_interrupt_handler(sig, frame):
    """Make sure keyboard interrupts are handled gracefully so they don't flood stdout"""
    logging.info(f"A KeyboardInterrupt (ID: {sig}) has been captured. Terminating pool of workers and closing the pool")
    # treat all KeyboardInterrupt signals as a graceful exit and return 0 after successfully destroying the pool
    # pool destruction is handled by the context and does not need to be called separately in this scope
    exit(0)


def yaml_parser(config):
    if config is not None:
        with open(config, 'r') as stream:
            return yaml.safe_load(stream)
    return None


def main(config):
    # handle all interrupts in this scope via a call to keyboard_interrupt_handler
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    # initialize the logger
    logger = SQLLogger(queue=None, **config['logger'])

    # initialize the coordinator with 6 workers and a scraper function
    coordinator = EventCoordinator(scraper=scrape, logger=logger, **config['scraper'])

    # initialize the observer using the path and passing the coordinator
    observer = EventObserver(coordinator=coordinator,
                             **config['observer']
                             )

    # start the context
    with observer:
        while True:
            # just keep looping until an interrupt is captured by the console
            # the observer and coordinator classes only need to be initialized and will operate on their respective
            # threads until destroyed
            time.sleep(1)


def parse_arguments(args):
    parser = ArgumentParser()
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument('--create_config', action='store_true', required=False,
                       help="Creates an empty configuration template file in the current directory.")
    mutex.add_argument('--configuration', required=False,
                       help="The configuration file to use.")
    arguments = parser.parse_args(args)
    return arguments


if __name__ == '__main__':
    from utils.logit import start_logging

    start_logging()

    testing = True

    if testing:
        _cmd = ['--config', r'C:\Users\aarong.SCC_NT\PycharmProjects\FileSystemLogger\config.yml']
        _args = parse_arguments(_cmd)
        _config = yaml_parser(_args.configuration)
    else:
        _args = parse_arguments(sys.argv[1:])
        _config = yaml_parser(_args.configuration)

    if _args.create_config:
        with open(r'./configuration_template.yml', 'w') as f:
            f.write('# make sure to not include the angled brackets <>\n')
            _template = {'observer': {'path': '<path_to_file>', 'recursive': '<true or false>'},
                         'scraper': {'workers': 6},
                         'logger': {'server': '<name of SQL server>',
                                    'database': '<name of SQL database>',
                                    'table': '<name of SQL table>',
                                    'schema': 'dbo', 'driver': 'SQL+Server'
                                    }
                         }
            yaml.dump(_template, f)

    if _config is not None:
        main(_config)
    else:
        parse_arguments(['--help'])
