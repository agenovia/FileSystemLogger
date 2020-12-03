# FileSystemLogger
FileSystemLogger for work. Used to capture events under a root directory. Born out of a need to track file changes under important directories.

Python 3.7.4

## Prerequisites
```
pip install watchdog sqlalchemy pyodbc
```

## TODO
1. Add a "modified events" handler
  - this handler needs to take care of tracking modified events and inserts only the last event into the queue so it doesn't flood the SQL table
  - as of now, modified events are not tracked
