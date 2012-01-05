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

import qr
import time
import reppy
import redis
import urlparse
import threading

class PoliteFetcher(BaseFetcher):
	requestLock = threading.Lock()
	
	def __init__(self, poolSize=10, agent=None, stopWhenDone=False, 
		delay=2, allowAll=False, **kwargs):
		
		# Call the parent constructor
		BaseFetcher.__init__(self, poolSize, agent, stopWhenDone)
		# Include a priority queue of plds
		self.pldQueue = qr.PriorityQueue('plds', **kwargs)
		# Make sure that there is an entry in the plds for
		# each domain waiting to be fetched. Also, include
		# the number of urls from each domain in the count
		# of remaining urls to be fetched.
		r = redis.Redis(**kwargs)
		# Redis has a pipeline feature that allows for bulk
		# requests, the result of which is a list of the 
		# result of each individual request. Thus, only get
		# the length of each of the queues in the pipeline
		# as we're just going to set remaining to the sum
		# of the lengths of each of the domain queues.
		with r.pipeline() as p:
			for key in r.keys('domain:*'):
				self.pldQueue.push(key, 0)
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
		self.timer = None
		# This is a way to ignore the allow/disallow directives
		# For example, if you're checking for allow in other places
		self.allowAll = allowAll
		self.userAgentString = reppy.getUserAgentString(self.agent)
	
	def __len__(self):
		''''''
		return len(self.pldQueue) + len(self.requests)
	
	def getKey(self, req):
		# This actually considers the whole domain name, including subdomains, uniquely
		# This aliasing is just in case we want to change that scheme later, easily
		return 'domain:%s' % urlparse.urlparse(req.url.strip()).hostname
	
	def allowed(self, url):
		'''Are we allowed to fetch this url/urls?'''
		logger.warn('Allowed? %s' % url)
		return self.allowAll or reppy.allowed(url, self.agent, self.userAgentString)
	
	def crawlDelay(self, request):
		'''How long to wait before getting the next page from this domain?'''
		# No delay for requests that were serviced from cache
		if request.cached:
			return 0
		# Return the crawl delay for this particular url if there is one
		return (self.allowAll and self.delay) or reppy.crawlDelay(request.url, self.agent) or self.delay
		# return self.delay
	
	# Event callbacks
	def onDone(self, request):
		# Append this next one onto the pld queue.
		with self.requestLock:
			self.retries.append((request._originalKey, time.time() + self.crawlDelay(request)))
			tostart = len(self.retries)
			results = zip(self.retries, self.pldQueue.extend(self.retries))
			self.retries = [val for val, success in results if (success != 1)]
			logger.debug('Requeued %i / %i in the pldQueue' % (tostart - len(self.retries), tostart))
	
	# When we try to pop off an empty queue
	def onEmptyQueue(self, key):
		pass
	
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
		r = self.requests.pop()
		while r and count < upto:
			count += self.push(r) or 0
			r = self.requests.pop()
		logger.debug('Grew by %i' % count)
		return BaseFetcher.grew(self, count)
	
	def push(self, request):
		key = self.getKey(request)
		q = qr.Queue(key)
		if not len(q):
			self.pldQueue.push(key, time.time())
		q.push(request)
		self.remaining += 1
		return 1
	
	def pop(self):
		'''Get the next request'''
		now = time.time()
		while True:
			# Get the next plds we might want to fetch from
			next, when = self.pldQueue.peek(withscores=True)
			if not next:
				return None
			# If the next-fetchable is not soon enough, then wait
			if when > now:
				# If we weren't waiting, then wait
				if self.timer == None:
					logger.debug('Waiting %f seconds on %s' % (when - now, next))
					self.timer = reactor.callLater(when - now, self.serveNext)
				return None
			else:
				# Go ahead and pop this item
				last = next
				next = self.pldQueue.pop()
				# Unset the timer
				self.timer = None
				q = qr.Queue(next)
				
				if len(q):
					# If the robots for this particular request is not fetched
					# or it's expired, then we'll have to make a request for it
					v = q.peek()
					domain = urlparse.urlparse(v.url).netloc
					robot = reppy.findRobot('http://' + domain)
					if not self.allowAll and (not robot or robot.expired):
						logger.debug('Making robots request for %s' % next)
						r = RobotsRequest('http://' + domain + '/robots.txt')
						r._originalKey = next
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
						return v
				else:
					try:
						logger.debug('Calling onEmptyQueue for %s' % next)
						self.onEmptyQueue(next)
					except Exception:
						logger.exception('onEmptyQueue failed for %s' % next)
					continue
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