"""
The SQL logger should be a daemon that periodically takes an existing queue and insert all new records into a table
"""

import logging
import pyodbc
import time
from contextlib import contextmanager
from threading import Thread, Event

import sqlalchemy as sa
from sqlalchemy.exc import *


class TableNotFound(Exception):
    def __init__(self, tbl):
        self.tbl = tbl

    def __str__(self):
        return f"{self.tbl} could not be found"


class SQLLogger(Thread):
    def __init__(self, server: str, database: str, table: str, schema: str, driver: str, tbl_obj=None):
        '''
        :param queue: This is the threading Queue to use for inserting new records
        :param server: This is the server for SQL to connect to
        :param database: This is the database that contains the target table
        :param table: This is the name of the target table
        :param schema: Target schema
        :param driver: ODBC driver to use
        '''
        super().__init__()
        self.name = 'LoggerThread'
        self.queue = None
        self.server = server
        self.database = database
        self.table = table
        self.driver = driver
        self.schema = schema
        self.engine = sa.create_engine(f'mssql+pyodbc://{self.server}/{self.database}?driver={self.driver}')

        # reusing objects so I don't have to do a lookup every time
        self.tbl_obj = tbl_obj

        # set this to False when we want the thread to exit
        self.stop_event = Event()

    def __str__(self):
        return f'[{self.server}].[{self.database}].[{self.schema}].[{self.table}]'

    def _tbl_obj(self, timeout=60):
        while True:
            if not self.stop_event.is_set():
                try:
                    logging.debug(f"inspecting metadata of database '{self.database}' on the server '{self.server}'")
                    if self.schema is None:
                        meta = sa.schema.MetaData()
                    else:
                        meta = sa.schema.MetaData(schema=self.schema)

                    meta.reflect(bind=self.engine)
                    tbl = None
                    logging.debug(f'searching for table {self.table}')
                    for table_name, table in meta.tables.items():
                        if table_name == f'{self.schema}.{self.table}':
                            logging.debug(f'table found: {table_name}')
                            tbl = table
                    if tbl is None:
                        raise TableNotFound(self.table)
                    else:
                        return tbl
                except (pyodbc.ProgrammingError, pyodbc.OperationalError, OperationalError) as e:
                    logging.exception(e, exc_info=True)
                    logging.debug(f'SQL connection could not be established. Retrying in {timeout} seconds')
                    time.sleep(timeout)
                except TableNotFound as e:
                    logging.exception(e, exc_info=True)
                    raise

    def _update_tbl_obj(self):
        self.tbl_obj = self._tbl_obj()

    @contextmanager
    def sql_connection(self):
        conn = None
        while conn is None:
            try:
                logging.debug('connecting to {}'.format(self.__str__()))
                conn = self.engine.connect()
            except (pyodbc.OperationalError, pyodbc.ProgrammingError, OperationalError) as e:
                logging.exception(e, exc_info=True)
                logging.warning(f'SQL connection could not be established. Retrying in 60 seconds')
                time.sleep(60)
        try:
            logging.debug('yielding connection')
            yield conn
        finally:
            conn.close()

    @staticmethod
    def prepare_inserts(details):
        _ev = details['event']
        _src = details['source']
        _dst = details['destination']
        _meta = details['meta']
        _ins = {'EventTime': _meta['observer_time'],
                'EventType': _ev['event_type'],
                'ObjectType': _ev['object_type'],
                'SourcePath': _src['fullpath'],
                'SourceDirectory': _src['directory'],
                'SourceFilename': _src['filename'],
                'SourceCreationTime': _src['created_time'],
                'SourceModifiedTime': _src['modified_time'],
                'DestinationPath': _dst['fullpath'],
                'DestinationDirectory': _dst['directory'],
                'DestinationFilename': _dst['filename'],
                'DestinationCreationTime': _dst['created_time'],
                'DestinationModifiedTime': _dst['modified_time'],
                }
        return _ins

    def run(self, timeout=60):
        self.tbl_obj = self._tbl_obj() if self.tbl_obj is None else self.tbl_obj
        failures = 0
        start = None
        with self.sql_connection() as con:
            while not self.stop_event.is_set():
                try:
                    start = time.time()
                    item = self.queue.get()
                    _ins = self.prepare_inserts(item)
                    logging.debug(_ins)
                    con.execute(self.tbl_obj.insert(_ins))
                    failures = 0
                except (pyodbc.ProgrammingError, pyodbc.OperationalError) as e:
                    logging.exception(e)
                    checkpoint = time.time()
                    failures += 1
                    elapsed_min = (checkpoint - start) / 60.0
                    logging.warning(f'Insertion has failed {failures} time(s). Time elapsed: {elapsed_min} minutes')
                    logging.warning(f'retrying in {timeout} seconds')
                    time.sleep(timeout)

    def stop(self):
        logging.debug(f'the logger has received a signal to terminate')
        logging.debug(f'stopping thread {self.name}')
        self.stop_event.set()


if __name__ == '__main__':
    pass
