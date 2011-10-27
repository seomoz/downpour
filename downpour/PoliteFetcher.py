#! /usr/bin/env python

'''Politely (per pay-level-domain) fetch urls'''

from downpour import BaseFetcher, logger, reactor

import qr
import time						# For to implement politeness
import urlparse					# To efficiently parse URLs

class PoliteFetcher(BaseFetcher):
	# The /default/ time we should wait before hitting the same domain
	wait     = 2.0
	# Infinity, because there's not another easy way to access it?
	infinity = float('Inf')
	
	def __init__(self, poolSize=10, **kwargs):
		# Call the parent constructor
		BaseFetcher.__init__(self, poolSize)
		self.pldQueue = qr.Queue('plds')
		self.requests = qr.Queue('request', **kwargs)
		self.timer = None
	
	def __len__(self):
		return len(self.pldQueue) + len(self.requests)
		
	def getKey(self, req):
		# This actually considers the whole domain name, including subdomains, uniquely
		# This aliasing is just in case we want to change that scheme later, easily
		return 'domain:%s' % urlparse.urlparse(req.url.strip()).hostname
	
	# Event callbacks
	def onDone(self, request):
		key = self.getKey(request)
		delay = time.time() + self.wait
		self.pldQueue.push((delay, key))
	
	#################
	# Insertion to our queue
	#################
	def extend(self, requests):
		count = 0
		t = time.time()
		for r in requests:
			count += 1
			key = self.getKey(r)
			q = qr.Queue(key)
			if not len(q):
				self.pldQueue.push((t, key))
			q.push(r)
		self.remaining += count
	
	def grow(self, upto=10000):
		count = 0
		t = time.time()
		r = self.requests.pop()
		while r and count < upto:
			count += 1
			key = self.getKey(r)
			q = qr.Queue(key)
			if not len(q):
				self.pldQueue.push((t, key))
			q.push(r)
			r = self.requests.pop()
		self.remaining += count
		
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