#! /usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Routines for managing the reppy cache in a way which is thread-safe,
# always honors proxies properly, and always uses downpour to fetch
# robots.txt files.
#
# XXX - As always, nothing is ever done to purge old, unneeded entries
# from the cache. It probably should be, but my (David B.) research
# indicates no evidence that cache growth is presently causing issues,
# and there are more pressing bugs to attend to.

# I m p o r t s

import reppy
import threading
import time
import urlparse
from downpour import RobotsRequest, logger

# V a r i a b l e s

# What to do when a domain's data is not cached or cached but expired.
NONE = 0  # Return a None object
DUMMY = 1  # Return a reppy.reppy object representing no restrictions
EXPIRED_OR_DUMMY = 2  # Like above, but if expired, return exipired object

# Domains which have pending robots.txt fetches.
_pending = { }

# For keeping things thread-safe.
_rlock = threading.RLock()

# The dummy object we hand back
_dummy = reppy.parse('', autorefresh=False, ttl=86400*365)

# C l a s s e s

# A thread-safe robots request object for use with the PoliteFetcher
class PoliteRobotsRequest(RobotsRequest):
    def __init__(self, url, *args, **kwargs):
        RobotsRequest.__init__(self, url, *args, **kwargs)
        # Initialize the state of this request.
        domain = urlparse.urlparse(url).netloc
        with _rlock:
            _pending[domain] = time.time()
        self._updated = False

    def _update(self, body):
        # First, make reppy do all the dirty work or parsing and caching it
        reppy.parse(body, url=self.url, ttl=self.ttl, autorefresh=False)
        # Then, clear the pending flag for this domain. If we get a KeyError
        # doing this, it is bad news because it means we have a race condition
        # somewhere.
        domain = urlparse.urlparse(self.url).netloc
        with _rlock:
            del _pending[domain]
        self._updated = True

    def onStatus(self, version, status, message):
        logger.warn('%s => Status %s' % (self.url, status))
        self.status = int(status)
        if self.status == 401 or self.status == 403:
            self._update("User-agent: *\nDisallow: /")
        elif self.status != 200:
            # This means we're going to act like there wasn't one
            logger.warn('No robots.txt => %s' % self.url)
            self._update('')

    def onSuccess(self, text, fetcher):
        self._update(text)

    def onError(self, *args, **kwargs):
        if not self._updated:
            logger.error('This should not happen (URL = %s).' % repr(self.url))
            self._update('')

# F u n c t i o n s

# retrieve: retrieve cached robots.txt data
# passed: url, policy for dealing with missing or expired data
# returns: a reppy.reppy object or None
def retrieve(url, policy):
    global NONE, DUMMY, EXPIRED_OR_DUMMY
    global _dummy
    ret = reppy.findRobot(url)
    if ret is None:
        return None if policy == NONE else _dummy
    if ret.expired:
        if policy == NONE:
            return None
        elif policy == DUMMY:
            return _dummy
        elif policy == EXPIRED_OR_DUMMY:
            return ret
        else:
            raise ValueError("Invalid policy: %d" % policy)
    return ret

# request: return a RobotsRequest object for a given domain
# passed: domain
# returns: a PoliteRobotsRequest object
def request(url, *args, **kwargs):
    global _rlock, _pending, NONE
    # Refuse to return a request for that which is already pending or
    # present and valid.
    domain = urlparse.urlparse(url).netloc
    with _rlock:
        oldreq = reppy.findRobot(url)
        unneeded = oldreq is not None and not oldreq.is_expired(peek=True)
        if domain in _pending or unneeded:
            return None
        else:
            return PoliteRobotsRequest(url, *args, **kwargs)
