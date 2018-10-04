#!/usr/bin/env python3

"""
 Collects log entries in real time and provides extensions/callbacks to ship
 entries to any remote collector for log aggregation, metrics or operational
 use.

"""
import os
import time
import errno
import stat
import sys

class LogCollector(object):
    """Looks for changes in files of a given directory. Watches entries in real time
       and allows callbacks. """

    def __init__(self, watch_dir, callback, extensions=["log"], tail_lines=1, maxsize=1048576):
        """Arguments:
            (str) @watch_dir:
                the directory to watch
            (callable) @callback:
                a callback function that will be invoked everytime a new log entry in
                encountered.
            (list) @extensions:
                only watch files with these extensions, default to .log
            (int) @tail_lines:
                read last N lines from the log file(s)
            (int) @maxsize:
                    specifies the maximum number of bytes to read from a file on every
                    iteration, defaults to 1Mb
        """
        self.watch_dir = os.path.realpath(watch_dir)
        self.extensions = extensions
        self._callback = callback
        self.maxsize = maxsize
        self._files_to_watch = {}
        self._maxsize = maxsize

        assert os.path.isdir(self.watch_dir), self.watch_dir
        assert callable(self._callback), repr(callback)
        self.update_files()
        for id, file in self._files_to_watch.items():
            file.seek(os.path.getsize(file.name))
            if tail_lines:
                try:
                    lines = self.tail(file.name, tail_lines)
                except IOError as err:
                    if err.errno != errno.ENOENT:
                        raise
                    else:
                        if lines:
                            self._callback(file.name, lines)


    def __enter__(self):
        return self

    def __exit__(self,*args):
        self.close()

    def __del__(self):
        self.close()


    def loop(self,interval=0.1,blocking=True):
        """ Start a loop checking for any changes to files in files_to_watch list
        if blocking is False then just run once and exit the loop"""
        while True:
            self.update_files()
            for file in list(self._files_to_watch.items()):
                self.readlines(file)
                if not blocking:
                    return
            time.sleep(interval)

    def log(self, line):
        """Log a file when it is unwatched."""
        print(line)

    def listdir(self):
        """List directory and filter files to watch based on the
        matching extensions."""
        dirlist = os.listdir(self.watch_dir)
        if self.extensions:
            return [x for x in dirlist if os.path.splitext(x)[1][1:] in self.extensions]
        return dirlist
