#! /usr/bin/env python

# This tries to import the most efficient reactor 
# that's available on the system.
try:
	from twisted.internet import epollreactor
	epollreactor.install()
	print 'Using epoll reactor'
except ImportError:
	try:
		from twisted.internet import kqreactor
		kqreactor.install()
		print 'Using kqueue reactor'
	except ImportError:
		print 'Using select reactor'

import threading	
from twisted.web import client, error
from twisted.internet import reactor, ssl

# Logging
# We'll have a stream handler and file handler enabled by default, and 
# you can select the level of debugging to affect verbosity
import logging
logger = logging.getLogger('downpour')
# Stream handler
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(funcName)s => %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

# File handler to downpour.log
handler = logging.FileHandler('downpour.log')
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

class BaseRequest(object):
	timeout = 30
	redirectLimit = 10
	
	def __init__(self, url):
		self.url = url
		self.response = None
		self.failure  = None
	
	# Inheritable callbacks. You don't need to worry about
	# returning anything. Just go ahead and do what you need
	# to do with the input!
	def onSuccess(self, text):
		pass
	
	def onError(self, failure):
		pass
	
	def onDone(self, response):
		pass
	
	# Finished
	def done(self, response):
		try:
			self.response = response
			self.onDone(response)
		except Exception as e:
			try:
				logger.error(repr(e))
			except Exception:
				logger.error('Unloggable error.')
		finally:
			return self

	# Made contact
	def success(self, response):
		try:
			logger.info('Successfully fetched %s' % self.url)
			self.response = response
			self.onSuccess(response)
		except Exception as e:
			try:
				logger.error(repr(e))
			except Exception:
				logger.error('Unloggable error.')
		finally:
			return self

	# Failed to made contact
	def error(self, failure):
		try:
			logger.info('Failed %s => %s' % (self.url.strip(), failure.getErrorMessage()))
			self.failure = failure
			self.onError(failure)
		except Exception as e:
			try:
				logger.error(repr(e))
			except Exception:
				logger.error('Unloggable error.')
		finally:
			return self

class BaseFetcher(object):
	def __init__(self, poolSize, urls=None):
		self.sslContext = ssl.ClientContextFactory()
		self.requests = [] if urls == None else urls
		self.poolSize = poolSize
		self.numFlight = 0
		self.lock = threading.Lock()
	
	def download(self, r):
		self.requests.append(r)
		self.serveNext()
	
	# This is how subclasses communicate how many requests they have 
	# left to fulfill. 
	def __len__(self):
		return len(self.requests)
	
	# This is how we get the next request to service. Return None if there
	# is no next request to service. That doesn't have to mean that it's done
	def pop(self):
		try:
			return self.requests.pop()
		except IndexError:
			return None
	
	# These can be overridden to do various post-processing. For example, 
	# you might want to add more requests, etc.
	
	def onDone(self, response):
		pass
	
	def onSuccess(self, response):
		pass
	
	def onError(self, response):
		pass
	
	# These are internal callbacks
	def done(self, response):
		try:
			with self.lock:
				self.numFlight -= 1
				logger.debug('numFlight : %i | len : %i' % (self.numFlight, len(self)))
			self.onDone(response)
		except Exception as e:
			logger.error(repr(e))
		finally:
			self.serveNext()
	
	def success(self, response):
		try:
			self.onSuccess(response)
		except Exception as e:
			logger.error(repr(e))
		return response
	
	def error(self, response):
		try:
			self.onError(response)
		except Exception as e:
			logger.error(repr(e))
		return response
	
	# These are how you can start and stop the reactor. It's a convenience
	# so that you don't have to import reactor when you want to use this
	def start(self):
		self.serveNext()
		reactor.run()
	
	def stop(self):
		reactor.stop()
	
	# This probably shouldn't be overridden, as it contains the majority
	# of the logic about how to deploy requests and bind the callbacks.
	def serveNext(self):
		with self.lock:
			if len(self) == 0:
				self.stop()
			logger.debug('Fetching more things!')
			while (self.numFlight < self.poolSize) and len(self):
				r = self.pop()
				if r == None:
					break
				logger.debug('Requesting %s' % r.url)
				self.numFlight += 1
				d = client.getPage(r.url, agent='SEOmoz Twisted Cralwer', timeout=r.timeout, followRedirect=1, redirectLimit=r.redirectLimit)
				d.addCallback(r.success).addCallback(self.success)
				d.addErrback(r.error).addErrback(self.error)
				d.addBoth(r.done).addBoth(self.done)
