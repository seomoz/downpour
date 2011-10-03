#! /usr/bin/env python

'''Politely (per pay-level-domain) fetch urls'''

from downpour import BaseFetcher, logger, reactor

import qr
import time						# For to implement politeness
import urlparse					# To efficiently parse URLs

class PoliteFetcher(BaseFetcher):
	# How long should we wait before making a request to the same tld?
	wait     = 2.0
	# Infinity, because there's not another easy way to access it?
	infinity = float('Inf')
	
	def __init__(self, poolSize=10, **kwargs):
		# Call the parent constructor
		BaseFetcher.__init__(self, poolSize)
		self.plds = dict((q.key, q) for q in qr.Queue.all('domain:*'))
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
		if len(self.plds[key]):
			delay = time.time() + self.wait
			self.pldQueue.push((delay, key))
		else:
			del self.plds[key]
	
	#################
	# Insertion to our queue
	#################
	def extend(self, max):
		count = 0
		t = time.time()
		r = self.requests.pop()
		while r and count < max:
			count += 1
			key = self.getKey(r)
			try:
				logger.debug('Read request %s' % r.url)
				self.plds[key].push(r)
			except KeyError:
				logger.debug('Made queue for %s' % key)
				q = qr.Queue(key)
				q.push(r)
				self.plds[key] = q
				self.pldQueue.push((t, key))
			r = self.requests.pop()
		self.remaining += count
		
	def pop(self):
		'''Get the next request'''
		if len(self.pldQueue) < self.poolSize:
			self.extend(10000)
		now = time.time()
		while True:
			# Get the next plds we might want to fetch from
			next = self.pldQueue.peek()
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
					return self.plds[next[1]].pop()
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