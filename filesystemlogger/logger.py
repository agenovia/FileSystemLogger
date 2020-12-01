"""
The SQL logger should be a daemon that periodically takes an existing queue and insert all new records into a table
"""

import logging
import pyodbc
import time
from contextlib import contextmanager
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.exc import *


class TableNotFound(Exception):
    def __init__(self, tbl):
        self.tbl = tbl

    def __str__(self):
        return f"{self.tbl} could not be found"


class SQLInsert:
    def __init__(self, db_connection, inserts, tbl_obj=None):
        """
        db_connection = {server, database, driver, table, schema}
        """
        self.dbcon = db_connection
        self.server = db_connection['server']
        self.database = db_connection['database']
        self.driver = db_connection['driver']
        self.table = db_connection['table']
        self.schema = db_connection['schema']

        self.engine = sa.create_engine(f'mssql+pyodbc://{self.server}/{self.database}?driver={self.driver}')
        self.inserts = inserts

        # reusing objects so I don't have to do a lookup every time
        self.tbl_obj = self._tbl_obj() if tbl_obj is None else tbl_obj

    def __repr__(self):
        return f'SQLInsert({self.dbcon}, {self.inserts}, {self.tbl_obj})'

    @contextmanager
    def sql_connection(self):
        conn = None
        while conn is None:
            try:
                logging.debug('connecting to server: {}'.format(self.__repr__()))
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

    def _tbl_obj(self, timeout=60):
        while True:
            try:
                logging.debug('inspecting metadata of connected database')
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

    def insert(self, n=100, timeout=60):
        """Extracts dictionary objects from the queue and inserts them to the target table object

        `n` - number of records to insert at one time
        `timeout` - time to sleep
        `warn` - threshold for warning on consecutive retries
        """

        def chunks(l):
            """chunk the insert list so the SQL insert doesn't bomb out. 2100 max parameters allowed. Each column is a
            parameter, so if you're inserting 1000 records with 5 columns, that's 5000 parameters!"""
            for i in range(0, len(l), n):
                yield l[i:i + n]

        with self.sql_connection() as conn:
            start = datetime.now()
            failures = 0
            for chunk in chunks(self.inserts):
                while True:
                    try:
                        logging.debug(f'inserting {len(chunk)} records')
                        ins = self.tbl_obj.insert(chunk)
                        conn.execute(ins)
                        logging.debug(f'inserted {len(chunk)} records')
                        break
                    except (pyodbc.ProgrammingError, pyodbc.OperationalError) as e:
                        logging.exception(e)
                        checkpoint = datetime.now()
                        failures += 1
                        elapsed_min = (checkpoint - start).total_seconds() / 60.0
                        logging.warning(f'Insertion has failed {failures} time(s). Time elapsed: {elapsed_min} minutes')
                        logging.warning(f'chunk failed to insert')
                        logging.warning(f'retrying chunk in {timeout} seconds')
                        time.sleep(timeout)

        return self.tbl_obj


if __name__ == '__main__':
    pass
