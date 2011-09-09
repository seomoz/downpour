#! /usr/bin/env python

'''Fetch a bunch of feeds in quick succession'''

# Based on the example at:
# http://pycurl.cvs.sourceforge.net/pycurl/pycurl/examples/retriever-multi.py?view=markup

import pyev
import pycurl					# We need to talk to curl
import signal
import socket
import logging					# Early integration of logging is good
import urlparse
from cStringIO import StringIO	# To fake file descriptors into strings

# Our logger
logger = logging.getLogger('downpour')
# Signals that are equivalent of stopping
SIGSTOP = (signal.SIGPIPE, signal.SIGINT, signal.SIGTERM)
# The loop we'll be using for everything
loop = pyev.default_loop()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler('downpour.log', 'w+')
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

class Request(object):
	retryMax   = 0
	retryBase  = 2
	retryScale = 1
	
	def __init__(self, url):
		self.url       = url
		self.sock      = None
		self.fetcher   = None
		self.ioWatcher = pyev.Io(0, pyev.EV_READ | pyev.EV_WRITE, loop, self.io)
		# For backing off for retrying:
		# scale * (base ** retries)
		self.retries   = 0
	
	def success(self, c, content):
		pass
	
	def error(self, c, errno, errmsg):
		pass
	
	def done(self):
		try:
			self.sock.close()
		except AttributeError as e:
			logger.error(repr(e))

	#################
	# curl callbacks
	#################
	def socket(self, family, socktype, protocol):
		'''Pycurl wants a socket, so make one, watch it and return it.'''
		logger.debug('Watching socket for %s' % self.url)
		if self.sock:
			self.sock.close()
		self.sock = socket.socket(family, socktype, protocol)
		self.ioWatcher.stop()
		self.ioWatcher.set(self.sock, pyev.EV_READ | pyev.EV_WRITE)
		self.ioWatcher.start()
		return self.sock

	#################
	# libev callbacks
	#################
	def io(self, watcher, revents):
		#logger.debug('IO Event')
		# Temporarily stop our watcher, call, then restart
		#self.ioWatcher.stop()
		self.fetcher.socketAction(self.sock.fileno())
		#self.ioWatcher.start()

class Fetcher(object):
	def __init__(self, poolSize = 10):
		# Go ahead and make a curl multi handle
		self.multi = pycurl.CurlMulti()
		self.multi.setopt(pycurl.M_TIMERFUNCTION, self.curlTimer)
		self.multi.setopt(pycurl.M_SOCKETFUNCTION, self.curlSocket)
		# Make a sharing option for DNS stuff
		self.share = pycurl.CurlShare()
		self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
		# A queue of our requests, and the number of requests in flight
		self.queue = []
		self.retryQueue = []
		self.num = 0
		# Now instantiate a pool of easy handles
		self.pool = []
		for i in range(poolSize):
			c = pycurl.Curl()
			# It will need a file to write to
			c.fp = None
			# Set some options
			c.setopt(pycurl.CONNECTTIMEOUT, 15)
			c.setopt(pycurl.FOLLOWLOCATION, 1)
			c.setopt(pycurl.SHARE, self.share)
			c.setopt(pycurl.FRESH_CONNECT, 1)
			c.setopt(pycurl.FORBID_REUSE, 1)
			c.setopt(pycurl.MAXREDIRS, 5)
			c.setopt(pycurl.TIMEOUT, 15)
			c.setopt(pycurl.NOSIGNAL, 1)
			# Now add it to the pool
			self.pool.append(c)
		self.multi.handles = self.pool[:]
		# Now listen for certain events
		self.signalWatchers = [pyev.Signal(sig, loop, self.signal) for sig in SIGSTOP]
		self.timerWatcher = pyev.Timer(1000.0, 0.0, loop, self.timer)
		#self.timeoutTimer = pyev.Timer(60.0, 60.0, loop, self.checkTimeouts)
	
	def __del__(self):
		'''Clean up the pool of curl handlers we allocated'''
		logger.info('Cleaning up')
		for c in self.pool:
			if c.fp is not None:
				c.fp.close()
				c.fp = None
			c.close()
		self.multi.close()

	#################
	# Inheritance Interface
	#################
	def __len__(self):
		return self.num + len(self.queue)

	def extend(self, requests):
		self.queue.extend(requests)
		self.serveNext()

	def pop(self):
		'''Get the next request'''
		return self.queue.pop(0)

	def push(self, r):
		'''Queue a request'''
		self.queue.append(r)
		self.serveNext()
	
	def onSuccess(self, c):
		pass
	
	def onError(self, c):
		pass
	
	def onDone(self, c):
		pass
	
	#################
	# Our interface
	#################
	def start(self):
		logger.info('Starting fetcher...')
		for w in self.signalWatchers:
			w.start()
		self.serveNext()
		while True:
			try:
				loop.start()
			except OSError as e:
				logger.error('OSError in loop: %s' % repr(e))
	
	def stop(self):
		logger.info('Stopping fetcher...')
		for w in self.signalWatchers:
			w.stop()
		loop.stop()

	#################
	# libev callbacks
	#################
	def signal(self, watcher, revents):
		logger.info('Signal caught')
		self.stop()
	
	def timer(self, watcher, revents):
		logger.info('Timer fired')
		self.perform()
	
	def retry(self, watcher, revents):
		try:
			c = self.retryQueue.pop()
			logger.info('Retrying %s' % c.request.url)
			self.serve(c, c.request)
		except ValueError:
			logger.warn('Tried popping off empty retryQueue')
	
	#################
	# handle complete
	#################
	def success(self, c):
		content = c.fp.getvalue()
		logger.debug('Success %s => %s...' % (c.request.url, content[0:100]))
		c.request.success(c, c.fp.getvalue())
		self.onSuccess(c)
		self.done(c)
	
	def error(self, c, errno, errmsg):
		if c.request.retries < c.request.retryMax:
			# Append it to the list to retry
			r = c.request
			t = r.retryScale * (r.retryBase ** r.retries)
			c.request.retries += 1
			logger.debug('Retrying %s in %is (%s)' % (r.url, t, errmsg))
			self.retryQueue.append(c)
			self.multi.remove_handle(c)
			c.timer = pyev.Timer(t, 0, loop, self.retry)
			c.timer.start()
		else:
			logger.debug('Error %s => (%i) %s' % (c.request.url, errno, errmsg))
			c.request.error(c, errno, errmsg)
			self.onError(c)
			self.done(c)
	
	def done(self, c):
		logger.debug('Done with %s' % c.request.url)
		self.onDone(c)
		c.fp.close()
		c.fp = None
		c.request.done()
		self.pool.append(c)
		self.multi.remove_handle(c)
		self.serveNext()
		
	#################
	# curl callbacks
	#################
	def curlTimer(self, timeout):
		t = timeout / 1000.0
		if t < self.timerWatcher.remaining():
			logger.debug('Resetting timer to fire in %fs' % t)
			self.timerWatcher.stop()
			self.timerWatcher.set(t, 1.0)
			self.timerWatcher.start()
	
	def curlSocket(self, sock, action, userp, socketp):
		pass
	
	def perform(self):
		try:
			return self.multi.perform()
		except socket.error as e:
			logger.error('Socket error: %s' % repr(e))
		except OSError as e:
			logger.error('OSError: %s' % repr(e))
		except Exception as e:
			logger.error(repr(e))
	
	def socketAction(self, sock):
		try:
			ret, num = self.multi.perform()
			if num < self.num:
				logger.info('%i < %i => one or more handles has completed' % (num, self.num))
				self.infoRead()
		except socket.error as e:
			logger.error('Socket error: %s' % repr(e))
		except OSError as e:
			logger.error('OSError: %s' % repr(e))
		except Exception as e:
			logger.error('%s' % repr(e))
			
	def infoRead(self):
		#logger.debug('Checking with curl for finished handlers')
		try:
			num, ok, err = self.multi.info_read()
		except socket.error as e:
			logger.error('Socket error: %s' % repr(e))
		except OSError as e:
			logger.error('OSError: %s' % repr(e))
		except Exception as e:
			logger.error('%s' % repr(e))
			return
		logger.debug('infoRead : %i <=> %i' % (num, self.num))
		self.num = num
		for c in ok:
			# Handle successulf
			self.success(c)
		for c, errno, errmsg in err:
			# Handle failed
			self.error(c, errno, errmsg)

	def serveNext(self):
		while len(self.queue) and len(self.pool):
			# While there are requests to service, and handles to service them
			logger.debug('Queue : %i\tPool: %i' % (len(self.queue), len(self.pool)))
			# Look for the next request
			r = self.pop()
			if r == None:
				# pop() can return None to signal there are no more
				break
			# Get a handle, and attach a request and fp to it
			c = self.pool.pop()
			self.serve(c, r)
		self.perform()
	
	def serve(self, c, r):
		# The request should know who the fetcher is
		r.fetcher = self
		c.request = r
		c.fp = StringIO()
		# Set some options
		c.setopt(pycurl.URL, c.request.url)
		c.setopt(pycurl.HTTPHEADER, ['Host: %s' % urlparse.urlparse(r.url).hostname])
		c.setopt(pycurl.OPENSOCKETFUNCTION, c.request.socket)
		c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
		# Indicate that we have one more in flight
		self.num += 1
		self.multi.add_handle(c)
		self.multi.socket_action(pycurl.SOCKET_TIMEOUT, 0)
		self.perform()		

if __name__ == '__main__':
	handler   = logging.StreamHandler()
	handler.setLevel(logging.DEBUG)
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)
	f = file('urls.txt')
	urls = f.read().strip().split()
	f.close()
	f = Fetcher(20)
	f.extend([Request(url) for url in urls])
	f.start()
