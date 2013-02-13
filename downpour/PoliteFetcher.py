#! /usr/bin/env python
#
# Copyright (c) 2011 SEOmoz
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

'''Politely (per pay-level-domain) fetch urls'''

from downpour import BaseFetcher, RobotsRequest, logger, reactor
import Robots

import qr
import sys
import time
import reppy
import redis
import urlparse
import threading

class Counter(object):
    @staticmethod
    def put(r, request):
        key = 'flight:' + request._originalKey
        # Just put some dummy value in there. We're mostly interested
        # in the sorted-ness and the zremrangebyrank
        o = r.zadd(key, **{request.url: time.time() + (request.timeout * 2)})
        if r.ttl(key) < (request.timeout * 2):
            o = r.expire(key, request.timeout * 2)
            # logger.debug('Exprire %s %fs: %s' % (key, request.timeout * 2, repr(o)))

        o = r.zcard(key)
        # logger.debug('Put %s (%s) to expire at %fs; zcard = %d' % (request.url, request._originalKey, (time.time() + request.timeout * 2), o))
        return o

    @staticmethod
    def remove(r, request):
        key = 'flight:' + request._originalKey
        with r.pipeline() as p:
            o = p.zrem(key, request.url)
            o = p.zremrangebyscore(key, 0, time.time())
            o = p.zcard(key)
            o, removed, card = p.execute()

        # logger.debug('Remove %s (%s); Removed: %d; zcard = %d' % (request.url, request._originalKey, removed, card))
        return card

    @staticmethod
    def len(r, name):
        key = 'flight:' + name
        with r.pipeline() as p:
            o = p.zremrangebyscore(key, 0, time.time())
            o = p.zcard(key)

            removed, card = p.execute()

        # logger.debug('Len %s; Removed: %d; zcard = %d' % (key, removed, card))
        return card

# XXX - This is unacceptably chummy with the underlying implementation,
# but it is efficient. Always keep *something* in the underlying Redis
# set, possibly a placeholder, while a PLD is being worked on. Moreover,
# never overwrite non-placeholders already in the queue. Like all qr
# queues, no operation here is guaranteed to be atomic.
class PLDQueue(qr.PriorityQueue):
    # Yuck. Writing a float to Redis and reading it back sometimes leads
    # to lossage. Proclaim anything sufficiently huge as a placeholder.
    _PH = sys.float_info.max
    _PH_MIN = sys.float_info.max * 0.99 # very lenient!

    # Only push if not already there or is a placeholder.
    def push_unique(self, value, score):
        v = self.redis.zscore(self.key, self._pack(value))
        if v is None or v >= self._PH_MIN:
            self.push(value, score)

    # As above, but leave placeholders alone, too.
    def push_init(self, value, score):
        if self.redis.zscore(self.key, self._pack(value)) is None:
            self.push(value, score)

    # Just look, don't touch. Hide placeholders.
    def peek(self, withscores=False):
        v, s = qr.PriorityQueue.peek(self, withscores=True)
        if v is None or s >= self._PH_MIN:
            return (None, 0.0) if withscores else None
        return (v, s) if withscores else v

    # Pop, replacing with a placeholder. Never return a placeholder to
    # the caller (return None instead).
    def pop(self, withscores=False):
        v, s = self.peek(withscores=True)
        if v is None:
            return (None, 0.0) if withscores else None
        self.push(v, self._PH)
        return (v, s) if withscores else v

    # Nuke a placeholder. Take offense if a non-placeholder is there.
    def clear_ph(self, value):
        packed = self._pack(value)
        v = self.redis.zscore(self.key, packed)
        if v is None:
            return
        if v < self._PH_MIN:
            raise ValueError('Attempt to clear an active PLD.')
        self.redis.zrem(self.key, packed)
        
class PoliteRobotsRequest(RobotsRequest):
    # Not finished

class PoliteFetcher(BaseFetcher):
    # This is the maximum number of parallel requests we can make
    # to the same key
    maxParallelRequests = 5

    def __init__(self, poolSize=10, agent=None, stopWhenDone=False,
        delay=2, allowAll=False, use_lock=None, **kwargs):

        # First, call the parent constructor
        BaseFetcher.__init__(self, poolSize, agent, stopWhenDone)

        # Import DownpourLock only if use_lock specified, because it uses
        # *NIX-specific features. We use one lock for the pldQueue and one
        # for all the request queues collectively. The latter is a tad
        # overly restrictive, but is far easier than managing hundreds
        # of locks for hundreds of queues.
        if use_lock:
            import DownpourLock
            self.pld_lock = DownpourLock.DownpourLock("%s_pld.lock" % use_lock)
            self.req_lock = DownpourLock.DownpourLock("%s_req.lock" % use_lock)
        else:
            self.pld_lock  = threading.RLock()
            self.req_lock = threading.RLock()
        self.twi_lock = threading.RLock()  # Twisted reactor lock

        # Include a priority queue of plds
        self.pldQueue = PLDQueue('plds', **kwargs)
        # Make sure that there is an entry in the plds for
        # each domain waiting to be fetched. Also, include
        # the number of urls from each domain in the count
        # of remaining urls to be fetched.
        self.r = redis.Redis(**kwargs)
        # Redis has a pipeline feature that allows for bulk
        # requests, the result of which is a list of the
        # result of each individual request. Thus, only get
        # the length of each of the queues in the pipeline
        # as we're just going to set remaining to the sum
        # of the lengths of each of the domain queues.
        with self.r.pipeline() as p:
            for key in self.r.keys('domain:*'):
                with self.pld_lock:
                    self.pldQueue.push_init(key, 0)
                p.llen(key)
            self.remaining = sum(p.execute())
        # For whatever reason, pushing key names back into the
        # priority queue has been problematic. As such, we'll
        # set them aside as they fail, and then retry them at
        # some point. Like when the next request finishes.
        self.retries = []
        # Now make a queue for incoming requests
        self.requests = qr.Queue('request', **kwargs)
        self.delay = float(delay)
        # This is used when we have to impose a delay before
        # servicing the next available request.
        with self.twi_lock:
            self.timer = None
        # This is a way to ignore the allow/disallow directives
        # For example, if you're checking for allow in other places
        self.allowAll = allowAll
        self.userAgentString = reppy.getUserAgentString(self.agent)

    def __len__(self):
        ''''''
        return len(self.pldQueue) + len(self.requests)

    def idle(self):
        '''Returns whether or not this fetcher can handle more work'''
        # Look at when the next item can be fetched
        next, when = self.pldQueue.peek(withscores=True)
        # If there is no next item available, then we're idle
        if not next:
            return True
        # Alternatively, we'd only idle if the next request can not
        # yet be serviced.
        return when > time.time()

    def getKey(self, req):
        # This actually considers the whole domain name, including subdomains, uniquely
        # This aliasing is just in case we want to change that scheme later, easily
        return 'domain:%s' % urlparse.urlparse(req.url.strip()).hostname

    def allowed(self, url):
        '''Are we allowed to fetch this url/urls?'''
        # This should be deadwood, so log a warning if something calls it.
        logger.warn('PoliteFetcher.allowed called, using EXPIRED_OR_DUMMY mode.')
        r = Robots.retrieve(url, Robots.EXPIRED_OR_DUMMY)
        return self.allowAll or r.allowed(url, self.userAgentString)

    def crawlDelay(self, request):
        '''How long to wait before getting the next page from this domain?'''
        # No delay for requests that were serviced from cache
        if request.cached:
            logger.debug('Using delay of %fs' % 0.0)
            return 0
        # Return the crawl delay for this particular url if there is one
        r = Robots.retrieve(request.url, Robots.EXPIRED_OR_DUMMY)
        ret = (self.allowAll and self.delay) or r.crawlDelay(self.agent) or self.delay
        logger.debug('Using delay of %fs' % ret)
        return ret

    # Event callbacks
    def onDone(self, request):
        # Append this next one onto the pld queue.
        # NOTE:
        #   This should no longer be necessary, since we won't be
        #   queueing anything like this, but rather we queue next
        #   requests when we make one. Instead, we'll decrement the
        #   flight counts. The only exception is if it's a robots
        #   request, since subsequent requests will like depend on
        #   it.
        # self.pldQueue.push(request._originalKey, time.time() + self.crawlDelay(request))
        with self.pld_lock:
            if isinstance(request, RobotsRequest):
                self.pldQueue.push_unique(request._originalKey, time.time() + self.crawlDelay(request))
            # If this request would bring down our parallel requests
            # down from the maximum, then we should immediately requeue
            # the original key to reduce latency.
            if Counter.remove(self.r, request) == (self.maxParallelRequests - 1):
                self.pldQueue.push_unique(request._originalKey, time.time() + self.crawlDelay(request))

    # When we try to pop off an empty queue
    def onEmptyQueue(self, key):
        pass

    # How many are in flight from this particular key?
    def inFlight(self, key):
        return Counter.len(self.r, key)

    #################
    # Insertion to our queue
    #################
    def extend(self, requests):
        count = 0
        t = time.time()
        for r in requests:
            count += self.push(r) or 0
        return count

    def grow(self, upto=10000):
        count = 0
        t = time.time()
        with self.req_lock:
            r = self.requests.pop()
        while r and count < upto:
            count += self.push(r) or 0
            with self.req_lock:
                r = self.requests.pop()
        logger.debug('Grew by %i' % count)
        return BaseFetcher.grew(self, count)

    def trim(self, request, trim):
        # Then, trim that list
        with self.req_lock:
            qr.Queue(self.getKey(request)).trim(trim)

    # This is one of two places where we use pld_lock inside of a req_lock.
    def push(self, request):
        key = self.getKey(request)
        q = qr.Queue(key)
        with self.req_lock:
            if not len(q):
                with self.pld_lock:
                    self.pldQueue.push_init(key, time.time())
            q.push(request)
        self.remaining += 1
        return 1

    # Here we use twi_lock inside pld_lock, and pld_lock inside req_lock.
    # Don't use locks-in-locks in the reverse order anywhere, or downpour
    # might deadlock.
    def pop(self, polite=True):
        '''Get the next request'''
        while True:

            # First, we pop the next thing in pldQueue *if* it's not a
            # premature fetch (and a race condition is not detected).
            with self.pld_lock:
                # Get the next plds we might want to fetch from
                next, when = self.pldQueue.peek(withscores=True)
                if not next:
                    # logger.debug('Nothing in pldQueue.')
                    return None
                # If the next-fetchable is too soon, wait. If we're
                # already waiting, don't schedule a double callLater.
                now = time.time()
                if polite and when > now:
                    with self.twi_lock:
                        if not (self.timer and self.timer.active()):
                            logger.debug('Waiting %f seconds on %s' % (when - now, next))
                            self.timer = reactor.callLater(when - now, self.serveNext)
                    return None
                # If we get here, we don't need to wait. However, the
                # multithreaded nature of Twisted means that something
                # else might be waiting. Only clear timer if it's not
                # holding some other pending call.
                with self.twi_lock:
                    if not (self.timer and self.timer.active()):
                        self.timer = None
                # We know the time has passed (we peeked) so pop it.
                next = self.pldQueue.pop()

            # Get the queue pertaining to the PLD of interest and
            # acquire a request lock for it.
            q = qr.Queue(next)
            with self.req_lock:
                if len(q):
                    # If we've already saturated our parallel requests, then we'll
                    # wait some short amount of time before we make our next request.
                    # There is logic elsewhere so that if one of these requests
                    # completes before this small amount of time elapses, then it
                    # will be advanced accordingly.
                    if Counter.len(self.r, next) >= self.maxParallelRequests:
                        logger.debug('maxParallelRequests exceeded for %s' % next)
                        with self.pld_lock:
                            self.pldQueue.push_unique(next, time.time() + 20)
                        continue
                    # If the robots for this particular request is not fetched
                    # or it's expired, then we'll have to make a request for it
                    v = q.peek()
                    robot = Robots.retrieve(v.url, Robots.NONE)
                    if not self.allowAll and robot is None:
                        logger.debug('Making robots request for %s' % next)
                        domain = urlparse.urlparse(v.url).netloc
                        r = Robots.request('http://' + domain + '/robots.txt', proxy=v.proxy)
                        if r is None:
                            # Oops! Just lost a race.
                            logger.debug('Robots request for %s cancelled.' % next)
                            return None
                        r._originalKey = next
                        # Increment the number of requests we currently have in flight
                        Counter.put(self.r, r)
                        return r
                    else:
                        logger.debug('Popping next request from %s' % next)
                        v = q.pop()
                        # This was the source of a rather difficult-to-track bug
                        # wherein the pld queue would slowly drain, despite there
                        # being plenty of logical queues to draw from. The problem
                        # was introduced by calling urlparse.urljoin when invoking
                        # the request's onURL method. As a result, certain redirects
                        # were making changes to the url, saving it as an updated
                        # value, but we'd then try to pop off the queue for the new
                        # hostname, when in reality, we should pop off the queue
                        # for the original hostname.
                        v._originalKey = next
                        # Increment the number of requests we currently have in flight
                        Counter.put(self.r, v)
                        # At this point, we should also schedule the next request
                        # to this domain.
                        with self.pld_lock:
                            self.pldQueue.push_unique(next, time.time() + self.crawlDelay(v))
                        return v
                else:
                    try:
                        if Counter.len(self.r, next) == 0:
                            logger.debug('Calling onEmptyQueue for %s' % next)
                            self.onEmptyQueue(next)
                            try:
                                with self.pld_lock:
                                    self.pldQueue.clear_ph(next)
                            except ValueError:
                                logger.error('pldQueue.clear_ph failed for %s' % next)
                        else:
                            # Otherwise, we should try again in a little bit, and
                            # see if the last request has finished.
                            with self.pld_lock:
                                self.pldQueue.push_unique(next, time.time() + 20)
                            logger.debug('Requests still in flight for %s. Waiting' % next)
                    except Exception:
                        logger.exception('onEmptyQueue failed for %s' % next)
                    continue

        logger.debug('Returning None (should not happen).')
        return None

if __name__ == '__main__':
    import logging
    from downpour import BaseRequest

    # Turn on logging
    logger.setLevel(logging.DEBUG)

    q = qr.Queue('requests')
    with file('urls.txt') as f:
        for line in f:
            q.push(BaseRequest(line.strip()))

    p = PoliteFetcher(100)
    p.start()
