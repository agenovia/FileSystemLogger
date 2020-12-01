from os.path import dirname, basename
from os import stat
from datetime import datetime
import time


class DetailScraper:
    """
    class watchdog.events.FileSystemEvent(src_path)[source]
    Bases: object

    Immutable type that represents a file system event that is triggered when a change occurs on the monitored file
        system.

    All FileSystemEvent objects are required to be immutable and hence can be used as keys in dictionaries or be
        added to sets.

    event_type = None
    The type of the event as a string.

    is_directory = False
    True if event was emitted for a directory; False otherwise.

    src_path[source]
    Source path of the file system object that triggered this event.


    Event Types:
    FileSystemMovedEvent = watchdog.events.FileSystemMovedEvent(src_path, dest_path)
    FileMovedEvent = watchdog.events.FileMovedEvent(src_path, dest_path)
    DirMovedEvent = watchdog.events.DirMovedEvent(src_path, dest_path)
    FileModifiedEvent = watchdog.events.FileModifiedEvent(src_path)
    DirModifiedEvent = watchdog.events.DirModifiedEvent(src_path)
    FileCreatedEvent = watchdog.events.FileCreatedEvent(src_path)
    DirCreatedEvent = watchdog.events.DirCreatedEvent(src_path)
    FileDeletedEvent = watchdog.events.FileDeletedEvent(src_path)
    DirDeletedEvent = watchdog.events.DirDeletedEvent(src_path)
    """
    EVENT_TYPES = {'FileMovedEvent': ('file', 'renamed'),
                   'DirMovedEvent': ('directory', 'renamed'),
                   'FileModifiedEvent': ('file', 'modified'),
                   'DirModifiedEvent': ('directory', 'modified'),
                   'FileCreatedEvent': ('file', 'created'),
                   'DirCreatedEvent': ('directory', 'created'),
                   'FileDeletedEvent': ('file', 'deleted'),
                   'DirDeletedEvent': ('directory', 'deleted'),
                   }
    EVENT_TYPES = {key: dict(zip(['object_type', 'event_type'], values)) for key, values in EVENT_TYPES.items()}

    def __init__(self, event, observer_init):
        self.event = event
        self.event_type = self.EVENT_TYPES[event.__class__.__name__]

        # metainformation for determining how soon after an event is observed is it scraped
        self.observer_init = observer_init
        self.scraper_init = time.time()

        # TODO IDEA: can we have a recursive loop for this class that handles recursively checking meta-information
        # TODO of a modified event_type. That way we can just make sure on the coordinator side to

    @property
    def src(self):
        _s = hasattr(self.event, 'src_path')
        _path = self.event.src_path if _s else None
        _dir = dirname(_path) if _s else None
        _file = basename(_path) if _s else None

        try:
            _stat = stat(_path) if _path else None
        except OSError:
            _stat = None

        _size = _stat.st_size if _stat else None
        _ctime = datetime.fromtimestamp(_stat.st_ctime).replace(microsecond=0).__str__() if _stat else None
        _mtime = datetime.fromtimestamp(_stat.st_mtime).replace(microsecond=0).__str__() if _stat else None

        return {'fullpath': _path, 'directory': _dir, 'filename': _file, 'created_time': _ctime,
                'modified_time': _mtime, 'size': _size}

    @property
    def dst(self):
        _d = hasattr(self.event, 'dest_path')
        _path = self.event.dest_path if _d else None
        _dir = dirname(_path) if _d else None
        _file = basename(_path) if _d else None

        try:
            _stat = stat(_path) if _path else None
        except OSError:
            _stat = None

        _size = _stat.st_size if _stat else None
        _ctime = datetime.fromtimestamp(_stat.st_ctime).replace(microsecond=0).__str__() if _stat else None
        _mtime = datetime.fromtimestamp(_stat.st_mtime).replace(microsecond=0).__str__() if _stat else None

        return {'fullpath': _path, 'directory': _dir, 'filename': _file, 'created_time': _ctime,
                'modified_time': _mtime, 'size': _size}

    @property
    def meta(self):
        _now = time.time()
        _metainfo = [datetime.fromtimestamp(self.observer_init).replace(microsecond=0).__str__(),
                     datetime.fromtimestamp(self.scraper_init).replace(microsecond=0).__str__(),
                     datetime.fromtimestamp(_now).replace(microsecond=0).__str__(),
                     round((_now - self.observer_init), 2)
                     ]
        _keys = ['observer_time', 'scraper_start', 'scraper_end', 'time_elapsed_since_observation']
        return dict(zip(_keys, _metainfo))

    def details(self):
        return self.event_type, self.src, self.dst, self.meta


def scrape(event, observer_init):
    _event = DetailScraper(event, observer_init)
    _details = _event.details()
    return {'event': _details[0], 'source': _details[1], 'destination': _details[2], 'meta': _details[3]}
