#! /usr/bin/env python

'''Fetch a bunch of feeds in quick succession'''

# Based on the example at:
# http://pycurl.cvs.sourceforge.net/pycurl/pycurl/examples/retriever-multi.py?view=markup

import pycurl					# We need to talk to curl
import logging					# Early integration of logging is good
from cStringIO import StringIO	# To fake file descriptors into strings

try:
	import signal
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
	logging.warn("Failed to import signal.")

class Fetcher(object):
	def __init__(self, poolSize = 10):
		'''Create a pool of curl handlers, and create an empty queue of requests'''
		self.poolSize = poolSize			# Number of connections to use
		self.queue = []						# The queue of (url, filehandler)
		self.multi = pycurl.CurlMulti()		# Our multicurler
		self.multi.handles = []				# Allocate a pool of curl handlers
		for i in range(self.poolSize):
			c = pycurl.Curl()
			c.fp = None
			c.setopt(pycurl.FOLLOWLOCATION, 1)
			c.setopt(pycurl.MAXREDIRS, 5)
			c.setopt(pycurl.CONNECTTIMEOUT, 30)
			c.setopt(pycurl.TIMEOUT, 300)
			c.setopt(pycurl.NOSIGNAL, 1)
			self.multi.handles.append(c)
		self.pool = self.multi.handles[:]	# Set a list of all the free curl handles
		self.numInFlight = 0
	
	def __del__(self):
		'''Clean up the pool of curl handlers we allocated'''
		logging.info('Cleaning up')
		for c in self.multi.handles:
			if c.fp is not None:
				c.fp.close()
				c.fp = None
			c.close()
		self.multi.close()
	
	def __len__(self):
		'''How many URLs are still unfetched'''
		return len(self.queue) + self.numInFlight
	
	def pop(self):
		'''Get the next (url, success, error) tuple'''
		return self.queue.pop(0)
	
	def push(self, url, success, error=None):
		'''Queue a (url, success, error) tuple request'''
		self.queue.append((url, success, error))
	
	def onsuccess(self, c):
		'''We successfully fetched a url. Provides the curl handle'''
		logging.info('Success: %s => %s' % (c.url, c.getinfo(pycurl.EFFECTIVE_URL)))
	
	def onerror(self, c, errno, errmsg):
		'''We failed to fetch a url. Provides the curl handle'''
		logging.error('Failed: %s => %i : %s' % (c.url, errno, errmsg))
	
	def run(self, daemonize=False):
		'''Run through the list we have to fetch. If daemonize, run indefinitely'''
		while len(self) or daemonize:
			# Add all as many requests as we have space for
			while len(self) and self.pool:
				c = self.pool.pop()
				try:
					# Try to get the next thing to fetch
					c.url, c.success, c.error = self.pop()
				except (IndexError, TypeError):
					# If there's nothing to be fetched, then 
					self.pool.append(c)
					logging.info('Nothing else we can fetch.')
					break
				c.fp = StringIO()
				c.setopt(pycurl.URL, c.url)
				c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
				self.multi.add_handle(c)
				self.numInFlight += 1
				logging.debug('Requesting ' + c.url)
			# Perform the multi_perform
			while 1:
				ret, numHandles = self.multi.perform()
				if ret != pycurl.E_CALL_MULTI_PERFORM:
					break
			# Now check for terminated handles
			while 1:
				numQ, okList, errList = self.multi.info_read()
				# Go through all the successful handles
				for c in okList:
					# Call the success callback handler
					c.success(c, c.fp.getvalue())
					c.fp.close()
					c.fp = None
					self.multi.remove_handle(c)
					self.numInFlight -= 1
					self.pool.append(c)
					self.onsuccess(c)
				# Go through all the failed handles
				for c, errno, errmsg in errList:
					# Call the error handler if it exists
					if c.error:
						c.error(c, errno, errmsg)
					c.fp.close()
					c.fp = None
					self.multi.remove_handle(c)
					self.numInFlight -= 1
					self.pool.append(c)
					self.onerror(c, errno, errmsg)
				# Update the count of the urls we've processed
				if numQ == 0:
					break
			# No more I/O pending. You can do some CPU stuff here if you'd like
			self.multi.select(1.0)

def success(c, contents):
	logging.info('%s contains %s' % (c.url, contents[0:100]))

def error(c, errno, errmsg):
	logging.error('ZOMG FAILED! for %s' % (c.url))

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	f = Fetcher()
	f.push('http://google.com', success, error)
	f.push('http://yahoo.com', success, error)
	f.push('http://lwkjerpiu20983094822.com', success, error)
	f.run()