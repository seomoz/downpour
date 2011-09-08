#! /usr/bin/env python

'''Politely (per pay-level-domain) fetch urls'''

import heapq					# We'll use a heap to efficiently implement the queue
import time						# For to implement politeness
import urlparse					# To efficiently parse URLs
import logging
import Fetcher as Fetcher		# We'll piggy-back off of the existing infrastructure

logger = logging.getLogger('downpour')

class PoliteFetcher(Fetcher.Fetcher):
	# How long should we wait before making a request to the same tld?
	wait     = 2
	# Infinity, because there's not another easy way to access it?
	infinity = float('Inf')
	
	def __init__(self, poolSize=10):
		# Call the parent constructor
		super(PoliteFetcher,self).__init__(poolSize)
		self.plds = {}
		self.queue = []
		
	def getKey(self, req):
		# This actually considers the whole domain name, including subdomains, uniquely
		# This aliasing is just in case we want to change that scheme later, easily
		return urlparse.urlparse(req.url).hostname
	
	#################
	# Event callbacks
	#################
	def onSuccess(self, c):
		key   = self.getKey(c.request)
		delay = int(time.time() + self.wait)
		self.plds[key]['time'] = delay
		heapq.heappush(self.queue, (delay, key))
	
	def onError(self, c):
		key   = self.getKey(c.request)
		delay = int(time.time() + self.wait)
		self.plds[key]['time'] = delay
		heapq.heappush(self.queue, (delay, key))

	#################
	# Insertion to our queue
	#################
	def extend(self, request):
		t = int(time.time())
		for r in request:
			key = self.getKey(r)
			try:
				self.plds[key]['queue'].append(r)
			except KeyError:
				self.plds[key] = {'queue':[r], 'time':t}
				heapq.heappush(self.queue, (t, key))
		self.serveNext()
		
	def pop(self):
		'''Get the next (url, success, error) tuple'''
		cutoff = int(time.time() + self.wait)
		while True:
			# Get the next plds we might want to fetch from
			try:
				next = heapq.heappop(self.queue)
			except IndexError:
				return None
			if next[0] >= cutoff:
				heapq.heappush(self.queue, next)
				return None			
			# Check if we can fetch from this pld or not
			q = self.plds[next[1]]
			if time.time() > q['time']:
				# Update it to mark that a url here is in flight
				q['time'] = self.infinity
				logger.debug('Returning %s' % next[1])
				try:
					return q['queue'].pop(0)
				except IndexError:
					return None
			else:
				# Put this pld back into the queue
				heapq.heappush(self.queue, (cutoff, next[1]))
				logger.debug('Asking %s to wait' % next[1])
		return None
	
	def push(self, req):
		'''Queue a (url, success, error) tuple request'''
		key = self.getKey(req)
		try:
			self.plds[key]['queue'].append(req)
		except KeyError:
			self.plds[key] = {'queue':[req], 'time':int(time.time())}
			heapq.heappush(self.queue, (int(time.time()), key))
		self.serveNext()
	
