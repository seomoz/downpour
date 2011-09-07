#! /usr/bin/env python

import pycurl					# We need to talk to curl
import pyev						# Our event model
import signal					# For sigint, etc.
import socket					# We'll create a socket for curl so we can watch it
import time						# For some sleeping
import urlparse					# To efficiently parse URLs
import logging

from cStringIO import StringIO	# To fake file descriptors into strings

logger = logging.getLogger('downpour')
logger.addHandler(logging.NullHandler())

class Request(object):
	MAX_RETRIES = 5
	
	def __init__(self, url):
		logger.debug('Request object for %s' % url)
		self.url     = url
		self.fetcher = None
		self.loop    = None
		self.sock    = None
		self.watcher = None
		# Backoff params
		self.retries = 0
		self.scale   = 2
	
	def __del__(self):
		logger.debug('Request for %s destroyed.' % self.url)
		try:
			self.watcher.stop()
			self.watcher = None
		except AttributeError:
			pass
		
	################
	# Override these
	################
	def onSuccess(self, c, contents):
		logger.debug('Successfully fetched %s' % self.url)
	
	def onError(self, c, contents):
		logger.debug('Error fetching %s' % self.url)
	
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
		logger.debug('Watching socket for %s' % self.url)
		self.sock = socket.socket(family, socktype, protocol)
		self.watcher = pyev.Io(self.sock.fileno(), pyev.EV_READ | pyev.EV_WRITE, self.loop, self.io)
		self.watcher.start()
		return self.sock
	
	#################
	# libev callbacks
	#################
	def io(self, watcher, revents):
		#logger.debug('IO Event for %s' % self.url)
		#self.watcher.stop()
		self.fetcher.check()
		#self.watcher.set(self.sock, pyev.EV_READ | pyev.EV_WRITE)
		#self.watcher.start()

class Fetcher(object):
	def __init__(self, poolSize=10):
		self.multi = pycurl.CurlMulti()
		self.multi.handles = []
		self.multi.setopt(pycurl.M_SOCKETFUNCTION, self.socket)
		self.multi.setopt(pycurl.M_TIMERFUNCTION, self.rescheduleTimer)
		self.share = pycurl.CurlShare()
		self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
		self.poolSize = poolSize
		self.loop     = pyev.default_loop()
		self.queue    = []
		self.numInFlight = 0
		for i in range(self.poolSize):
			c = pycurl.Curl()
			c.timeout = None
			c.setopt(pycurl.FOLLOWLOCATION, 1)
			c.setopt(pycurl.MAXREDIRS, 5)
			c.setopt(pycurl.CONNECTTIMEOUT, 15)
			c.setopt(pycurl.TIMEOUT, 300)
			c.setopt(pycurl.NOSIGNAL, 1)
			c.setopt(pycurl.FRESH_CONNECT, 1)
			c.setopt(pycurl.FORBID_REUSE, 1)
			c.setopt(pycurl.SHARE, self.share)
			self.multi.handles.append(c)
		self.pool = self.multi.handles[:]
		SIGSTOP = (signal.SIGPIPE, signal.SIGINT, signal.SIGTERM)
		self.watchers = [pyev.Signal(sig, self.loop, self.signal) for sig in SIGSTOP]
		self.evTimer = pyev.Timer(0, 1.0, self.loop, self.timer)
		self.timeoutTimer = pyev.Timer(60.0, 60.0, self.loop, self.checkTimeouts)
		#self.watchers.append(pyev.Idle(self.loop, self.idle))
	
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
	
	def timer(self, watcher, revents):
		self.evTimer.stop()
		logger.debug('Timer fired!')
		self.multi.perform()
		self.evTimer.start()
	
	def checkTimeouts(self, watcher, revents):
		now = time.time()
		for c in self.multi.handles:
			if c.timeout and c.timeout < now:
				logger.debug('Application timeout')
				self.multi.socket_action(pycurl.SOCKET_TIMEOUT, 0)
				break
	
	#################
	# Service
	#################
	def start(self):
		logger.info('Starting...')
		self.checkQueue()
		for watcher in self.watchers:
			watcher.start()
		self.evTimer.start()
		self.timeoutTimer.start()
		self.loop.start()
	
	def stop(self):
		logger.info('Stopping')
		self.loop.stop(pyev.EVBREAK_ALL)
		while self.watchers:
			self.watchers.pop().stop()
		self.evTimer.stop()
		self.timeoutTimer.stop()
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
	def socket(self, a, b, c, d):
		logger.debug('Socket action')
	
	def success(self, c):
		c.request.success(c, c.fp.getvalue())
		self.onSuccess(c)
		self.done(c)
	
	#################
	# libcurl callbacks
	#################
	def rescheduleTimer(self, timeout):
		if timeout > 0:
			self.evTimer.stop()
			self.evTimer.set(timeout / 1000.0, 0)
			self.evTimer.start()
		else:
			self.evTimer.reset()
		
	def error(self, c, errno, errmsg):
		logger.debug('%s : (%i) %s' % (c.request.url, errno, errmsg))
		if c.request.retries <= c.request.MAX_RETRIES:
			t = c.request.scale ** c.request.retries
			logger.debug('Request for %s sleeping %i' % (c.request.url, t))
			pyev.sleep(t)
			c.fp.close()
			c.fp = StringIO()
			c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
			self.multi.remove_handle(c)
			self.multi.add_handle(c)
		else:
			c.request.error(c, c.errstr())
			self.onError(c)
			self.done(c)
		
	def done(self, c):
		logger.debug('Done with %s' % c.request.url)
		self.onDone(c)
		c.fp.close()
		c.fp = None
		c.timeout = 0
		self.pool.append(c)
		self.numInFlight -= 1
		self.multi.remove_handle(c)
		self.checkQueue()
		
	def check(self):
		if self.multi.perform():
			numQ, okList, errList = self.multi.info_read()
			for c in okList:
				# Handle successulf
				self.success(c)
			for c, errno, errmsg in errList:
				# Handle failed
				self.error(c, errno, errmsg)
		self.evTimer.reset()
	
	def checkQueue(self):
		while len(self.queue) and len(self.pool):
			logger.info('Remaining in queue: %i' % len(self.queue))
			logger.debug('Pool: %i' % len(self.pool))
			# If we have requests and handles to spare,
			req = self.pop()
			if req == None:
				break
			req.fetcher = self
			req.loop    = self.loop
			c = self.pool.pop()
			c.request = req
			c.fp = StringIO()
			c.setopt(pycurl.URL, c.request.url)
			c.setopt(pycurl.HTTPHEADER, ['Host: %s' % urlparse.urlparse(req.url).hostname])
			# When you create a socket, we must register it!
			c.setopt(pycurl.OPENSOCKETFUNCTION, c.request.socket)
			c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
			self.numInFlight += 1
			self.multi.add_handle(c)
			if c.timeout == None:
				logger.debug('Kickstarting...')
				self.multi.socket_action(pycurl.SOCKET_TIMEOUT, 0)
			c.timeout = time.time() + 15.0
			self.multi.perform()

if __name__ == '__main__':
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler   = logging.StreamHandler()
	handler.setLevel(logging.DEBUG)
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)
	f = file('urls.txt')
	urls = f.read().strip().split()
	f.close()
	f = Fetcher(20)
	f.extend(Request(url) for url in urls)
	f.start()
	