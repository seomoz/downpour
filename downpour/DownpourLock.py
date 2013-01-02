# Advisory lock object that works on all threads in all processes, which is
# needed if (like attercob) one has multiple threads in multiple processes.
#
# To lock threads, use threading.RLock objects. To lock processes, use the
# flock(2) system call. Python furnishes no direct interface to Posix
# semaphores, so we can't use those. Using the threading lock as the
# outer-level lock helps ensure that one process's threads won't unfairly
# monopolize the overall lock to the detriment of other processes.
#
# This code inspired by an example at:
# http://blog.vmfarms.com/2011/03/cross-process-locking-and.html

import os
import fcntl
import threading

class DownpourLock:
    def __init__(self, filename):
        self.rlock = threading.RLock()
        # This will create the cruft file if it does not exist already
        self.handle = open(filename, 'w')

    def acquire(self):
        self.rlock.acquire()
        fcntl.flock(self.handle, fcntl.LOCK_EX)

    def release(self):
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        self.rlock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def __del__(self):
        self.handle.close()
