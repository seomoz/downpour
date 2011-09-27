#! /usr/bin/env python

'''Politely (per pay-level-domain) fetch urls'''

from downpour import BaseFetcher, logger, reactor

import time						# For to implement politeness
import urlparse					# To efficiently parse URLs
from collections import deque	# Thread-safe queue

class PoliteFetcher(BaseFetcher):
	# How long should we wait before making a request to the same tld?
	wait     = 2.0
	# Infinity, because there's not another easy way to access it?
	infinity = float('Inf')
	
	def __init__(self, poolSize=10, urls=None):
		# Call the parent constructor
		super(PoliteFetcher,self).__init__(poolSize)
		self.plds = {}
		self.requests = deque()
		self.extend([] if urls == None else urls)
		self.timer = None
		
	def getKey(self, req):
		# This actually considers the whole domain name, including subdomains, uniquely
		# This aliasing is just in case we want to change that scheme later, easily
		return urlparse.urlparse(req.url.strip()).hostname
	
	# Event callbacks
	def onDone(self, request):
		#logger.debug('Done with %s' % request.url)
		key = self.getKey(request)
		if len(self.plds[key]):			
			delay = time.time() + self.wait
			self.requests.append((delay, key))
		else:
			del self.plds[key]
	
	#################
	# Insertion to our queue
	#################
	def extend(self, request):
		t = time.time()
		self.remaining += len(request)
		for r in request:
			key = self.getKey(r)
			try:
				self.plds[key].append(r)
			except KeyError:
				self.plds[key] = deque([r])
				self.requests.append((t, key))
		
	def pop(self):
		'''Get the next (url, success, error) tuple'''
		now = time.time()
		while True:
			# Get the next plds we might want to fetch from
			try:
				next = self.requests.popleft()
			except IndexError:
				return None
			# If the next-fetchable is not soon enough, then wait
			if next[0] > now:
				self.requests.appendleft(next)
				# If we weren't waiting, then wait
				if self.timer == None:
					logger.debug('Waiting %f seconds' % (next[0] - now))
					self.timer = reactor.callLater(next[0] - now, self.serveNext)
				return None
			else:
				# Unset the timer
				self.timer = None
				try:
					return self.plds[next[1]].popleft()
				except IndexError:
					# This should never happen
					logger.error('Popping from an empty pld: %s' % next[1])
					return None
				except ValueError:
					# Nor should this ever happen
					logger.error('Tried to pop from non-existent pld: %s' % next[1])
					return None
		return None
	
	def download(self, req):
		'''Queue a (url, success, error) tuple request'''
		self.remaining += 1
		key = self.getKey(req)
		try:
			self.plds[key].append(req)
		except KeyError:
			self.plds[key] = deque([req])
			self.requests.append((time.time()), key)
	
if __name__ == '__main__':
	from downpour import BaseRequest
	import logging
	logger.setLevel(logging.DEBUG)
	
	f = file('urls.txt')
	reqs = [BaseRequest(u) for u in f.read().strip().split('\n')]
	f.close()
	
	#urls = ['http://en.wikipedia.org/wiki/A', 'http://en.wikipedia.org/wiki/B', 'http://en.wikipedia.org/wiki/C', 'http://google.com']
	#reqs = [BaseRequest(u) for u in urls]
	p = PoliteFetcher(100, reqs)
	p.start()