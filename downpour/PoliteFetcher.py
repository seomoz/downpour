#! /usr/bin/env python

'''Politely (per pay-level-domain) fetch urls'''

from downpour import BaseFetcher, logger, reactor

import qr
import time						# For to implement politeness
import reppy
import urlparse					# To efficiently parse URLs

class PoliteFetcher(BaseFetcher):
	def __init__(self, poolSize=10, delay=2, allowAll=False, **kwargs):
		# Call the parent constructor
		BaseFetcher.__init__(self, poolSize)
		self.pldQueue = qr.Queue('plds')
		self.requests = qr.Queue('request', **kwargs)
		self.delay = delay
		self.timer = None
		# This is a way to ignore the allow/disallow directives
		# For example, if you're checking for allow in other places
		self.allowAll = allowAll
		self.userAgentString = reppy.getUserAgentString(self.agent)
	
	def __len__(self):
		return len(self.pldQueue) + len(self.requests)
		
	def getKey(self, req):
		# This actually considers the whole domain name, including subdomains, uniquely
		# This aliasing is just in case we want to change that scheme later, easily
		return 'domain:%s' % urlparse.urlparse(req.url.strip()).hostname
	
	def allowed(self, url):
		'''Are we allowed to fetch this url/urls?'''
		return self.allowAll or reppy.allowed(url, self.agent, self.userAgentString)
	
	def crawlDelay(self, url):
		'''How long to wait before getting the next page from this domain?'''
		# Until I can find a way to make this all asynchronous, going to have to omit it
		#return reppy.crawlDelay(url, self.agent, self.userAgentString) or self.delay
		return self.delay
	
	# Event callbacks
	def onDone(self, request):
		# Use the robots.txt delay, defaulting to our own
		key = self.getKey(request)
		delay = time.time() + self.crawlDelay(request.url)
		self.pldQueue.push((delay, key))
	
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
		return count
	
	def push(self, request):
		if self.allowed(request.url):
			key = self.getKey(request)
			q = qr.Queue(key)
			if not len(q):
				self.pldQueue.push((time.time(), key))
			q.push(request)
			self.remaining += 1
		else:
			logger.debug('Request %s blocked by robots.txt' % request.url)
			return 0
		
	def pop(self):
		'''Get the next request'''
		if len(self.pldQueue) < self.poolSize:
			self.grow()
		now = time.time()
		while True:
			# Get the next plds we might want to fetch from
			next = self.pldQueue.peek()
			if not next:
				return None
			# If the next-fetchable is not soon enough, then wait
			if next[0] > now:
				# If we weren't waiting, then wait
				if self.timer == None:
					logger.debug('Waiting %f seconds' % (next[0] - now))
					self.timer = reactor.callLater(next[0] - now, self.serveNext)
				return None
			else:
				# Go ahead and pop this item
				next = self.pldQueue.pop()
				# Unset the timer
				self.timer = None
				try:
					v = qr.Queue(next[1]).pop()
					if not v:
						continue
					return v
				except ValueError:
					# This should never happen
					logger.error('Tried to pop from non-existent pld: %s' % next[1])
					return None
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