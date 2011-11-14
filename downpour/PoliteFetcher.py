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

from downpour import BaseFetcher, logger, reactor

import qr
import time
import reppy
import redis
import urlparse

class PoliteFetcher(BaseFetcher):
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
		return self.allowAll or reppy.allowed(url, self.agent, self.userAgentString)
	
	def crawlDelay(self, request):
		'''How long to wait before getting the next page from this domain?'''
		# Until I can find a way to make this all asynchronous, going to have to omit it
		#return reppy.crawlDelay(url, self.agent, self.userAgentString) or self.delay
		# No delay for requests that were serviced from cache
		if request.cached:
			return 0
		return self.delay
	
	# Event callbacks
	def onDone(self, request):
		# Use the robots.txt delay, defaulting to our own
		self.pldQueue.push(request._originalKey, time.time() + self.crawlDelay(request))
	
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
		return count
	
	def push(self, request):
		if self.allowed(request.url):
			key = self.getKey(request)
			q = qr.Queue(key)
			if not len(q):
				self.pldQueue.push(key, time.time())
			q.push(request)
			self.remaining += 1
			return 1
		else:
			logger.debug('Request %s blocked by robots.txt' % request.url)
			return 0
		
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
					logger.debug('Waiting %f seconds' % (when - now))
					self.timer = reactor.callLater(when - now, self.serveNext)
				return None
			else:
				# Go ahead and pop this item
				next = self.pldQueue.pop()
				# Unset the timer
				self.timer = None
				v = qr.Queue(next).pop()
				if not v:
					continue
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