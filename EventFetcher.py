#! /usr/bin/env python

import pycurl					# We need to talk to curl
import pyev						# Our event model
import signal					# For sigint, etc.
import socket					# We'll create a socket for curl so we can watch it
import time						# For some sleeping
import logging
from cStringIO import StringIO	# To fake file descriptors into strings

logger = logging.getLogger('downpour')

class Request(object):
	def __init__(self, url, fetcher):
		logger.debug('Request object for %s' % url)
		self.url     = url
		self.fetcher = fetcher
		self.loop    = fetcher.loop
		self.sock    = None
		self.watcher = None
	
	def __del__(self):
		try:
			self.watcher.stop()
			self.watcher = None
		except AttributeError:
			pass
	
	def reset(self, events):
		self.watcher.stop()
		self.watcher.set(self.sock, events)
		self.watcher.start()
	
	################
	# Override these
	################
	def onSuccess(self, c, contents):
		logging.debug('Successfully fetched %s' % self.url)
	
	def onError(self, c, contents):
		logging.debug('Error fetching %s' % self.url)
	
	################
	# The actual callbacks
	################
	def success(self, c, contents):
		self.onSuccess(c, contents)
	
	def error(self, c, errmsg):
		self.onError(c, errmsg)
		
	#################
	# Curl callbacks
	#################
	def socket(self, family, socktype, protocol):
		'''Undocumented in stupid pycurl documentation. Found in test scripts'''
		logging.debug('Watching socket for %s' % self.url)
		self.sock = socket.socket(family, socktype, protocol)
		self.watcher = pyev.Io(self.sock._sock, pyev.EV_READ | pyev.EV_WRITE, self.loop, self.io)
		self.watcher.start()
		return self.sock
	
	#################
	# libev callbacks
	#################
	def io(self, watcher, revents):
		self.reset(pyev.EV_READ | pyev.EV_WRITE)
		self.fetcher.check()

class Fetcher(object):
	def __init__(self, poolSize=10):
		self.multi = pycurl.CurlMulti()
		self.multi.handles = []
		self.poolSize = poolSize
		self.loop     = pyev.default_loop()
		self.queue    = []
		self.numInFlight = 0
		for i in range(self.poolSize):
			c = pycurl.Curl()
			c.setopt(pycurl.FOLLOWLOCATION, 1)
			c.setopt(pycurl.MAXREDIRS, 5)
			c.setopt(pycurl.CONNECTTIMEOUT, 30)
			c.setopt(pycurl.TIMEOUT, 300)
			c.setopt(pycurl.NOSIGNAL, 1)
			self.multi.handles.append(c)
		self.pool = self.multi.handles[:]
		SIGSTOP = (signal.SIGPIPE, signal.SIGINT, signal.SIGTERM)
		self.watchers = [pyev.Signal(sig, self.loop, self.signal) for sig in SIGSTOP]
		self.watchers.append(pyev.Idle(self.loop, self.idle))
	
	#################
	# Inheritance Interface
	#################
	def __len__(self):
		return self.numInFlight + len(self.queue)
	
	def extend(self, requests):
		self.queue.extend(requests)
		self.checkQueue()
	
	def pop(self):
		'''Get the next request'''
		return self.queue.pop(0)

	def push(self, r):
		'''Queue a request'''
		self.queue.append(r)
		self.checkQueue()
	
	#################
	# libev callbacks
	#################
	def idle(self, watcher, revents):
		self.multi.perform()
		time.sleep(0.1)
	
	def signal(self, watcher, revents):
		self.stop()
	
	#################
	# Service
	#################
	def start(self):
		for watcher in self.watchers:
			watcher.start()
		self.checkQueue()
		self.loop.start()
	
	def stop(self):
		self.loop.stop(pyev.EVBREAK_ALL)
		while self.watchers:
			self.watchers.pop().stop()
		self.loop.stop()
	
	#################
	# Iheritable
	#################
	def onSuccess(self, c):
		pass
	
	def onError(self, c):
		pass
	
	def onDone(self, c):
		pass
	
	#################
	# Internal callbacks
	#################
	def success(self, c):
		c.request.success(c, c.fp.getvalue())
		self.onSuccess(c)
		self.done(c)
		
	def error(self, c):
		c.request.error(c, c.errstr())
		self.onError(c)
		self.done(c)
		
	def done(self, c):
		self.onDone(c)
		c.fp.close()
		c.fp = None
		self.pool.append(c)
		self.numInFlight -= 1
		self.multi.remove_handle(c)
		self.checkQueue()
		
	def check(self):
		self.multi.perform()
		numQ, okList, errList = self.multi.info_read()
		for c in okList:
			# Handle successulf
			self.success(c)
		for c, errno, errmsg in errList:
			# Handle failed
			self.error(c)
	
	def checkQueue(self):
		while len(self.queue) and len(self.pool):
			#logger.info('Remaining in queue: %i' % len(self.queue))
			logger.debug('Handle available. Making request.')
			# If we have requests and handles to spare,
			req = self.pop()
			if req == None:
				break
			c = self.pool.pop()
			c.request = req
			c.fp = StringIO()
			c.setopt(pycurl.URL, c.request.url)
			# When you create a socket, we must register it!
			c.setopt(pycurl.OPENSOCKETFUNCTION, c.request.socket)
			c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
			self.numInFlight += 1
			self.multi.add_handle(c)
			self.multi.perform()

if __name__ == '__main__':
	urls = ['http://google.com', 'http://wikipedia.org', 'http://yahoo.com', 'http://amazon.com', 'http://dan.lecocq.us']
	f = Fetcher()
	f.extend(Request(url, f) for url in urls)
	f.start()
	