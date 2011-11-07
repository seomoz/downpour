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

import os
import re
import urlparse
import threading
from twisted.python import log
from twisted.web import client, error
from twisted.internet import reactor, ssl
from twisted.python.failure import Failure

# Logging
# We'll have a stream handler and file handler enabled by default, and 
# you can select the level of debugging to affect verbosity
import logging
logger = logging.getLogger('downpour')
# Stream handler
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(funcName)s@%(lineno)s => %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

# File handler to downpour.log
handler = logging.FileHandler('/mnt/log/downpour.log')
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Twisted has an observer for logging twisted's errors
observer = log.PythonLoggingObserver()
observer.start()

class BaseRequestServicer(client.HTTPClientFactory):
	def __init__(self, request, agent):
		self.request = request
		self.request.cached = True
		client.HTTPClientFactory.__init__(self, url=request.url, agent=agent, timeout=request.timeout, redirectLimit=request.redirectLimit)
	
	def setURL(self, url):
		try:
			self.request.onURL(url)
		except:
			logger.exception('%s onURL failed' % self.request.url)
		scheme, host, port, path = client._parse(url)
		self.proxy = os.environ.get('%s_proxy' % scheme)
		if self.proxy:
			scheme, host, port, path = client._parse(self.proxy)
			self.scheme = scheme
			self.host = host
			self.port = port
			self.path = url
			self.url = url
		else:
			client.HTTPClientFactory.setURL(self, url)
		logger.debug('URL: %s' % self.url)
	
	def gotHeaders(self, headers):
		try:
			self.request.onHeaders(headers)
			# This request is marked as cached iff every request was served out
			# of the cache specified, and it was a hit.
			cached = self.proxy and ('HIT from %s' % self.host) in ';'.join(headers.get('x-cache', ''))
			self.request.cached = self.request.cached and cached
		except:
			logger.exception('%s onHeaders failed' % self.request.url)
		client.HTTPClientFactory.gotHeaders(self, headers)
	
	def gotStatus(self, version, status, message):
		try:
			self.request.onStatus(version, status, message)
		except:
			logger.exception('%s onStatus failed' % self.request.url)
		client.HTTPClientFactory.gotStatus(self, version, status, message)

class BaseRequest(object):
	urlRE = re.compile(r'#.+$')
	timeout = 45
	redirectLimit = 10
	
	def __init__(self, url):
		self.url = self.urlRE.sub('', url)
	
	def __del__(self):
		logger.debug('Deleting request for %s' % self.url)
	
	# Inheritable callbacks. You don't need to worry about
	# returning anything. Just go ahead and do what you need
	# to do with the input!
	def onSuccess(self, text):
		print repr(self.cached)
		pass
	
	def onError(self, failure):
		pass
	
	def onDone(self, response):
		pass
	
	def onHeaders(self, headers):
		pass
	
	def onStatus(self, version, status, message):
		if status != '200':
			logger.error('%s Got status => (%s, %s, %s)' % (self.url, version, status, message))
		pass
	
	def onURL(self, url):
		if self.url != url:
			logger.error('%s set => %s' % (self.url, url))
		pass
	
	# Finished
	def done(request, response):
		try:
			request.onDone(response)
		except Exception as e:
			logger.exception('Request done handler failed')
		return request

	# Made contact
	def success(request, response):
		try:
			logger.info('Successfully fetched %s' % request.url)
			request.onSuccess(response)
		except Exception as e:
			logger.exception('Request success handler failed')
		return request

	# Failed to made contact
	def error(request, failure):
		try:
			try:
				failure.raiseException()
			except:
				logger.exception('Failed for %s' % request.url)
			request.onError(failure)
		except Exception as e:
			logger.exception('Request error handler failed')
		return Failure(request)

class BaseFetcher(object):
	def __init__(self, poolSize=10, urls=None, agent=None):
		self.sslContext = ssl.ClientContextFactory()
		self.requests = [] if urls == None else urls
		self.poolSize = poolSize
		self.numFlight = 0
		self.lock = threading.Lock()
		self.processed = 0
		self.remaining = 0
		self.agent = agent or 'rogerbot/1.0'
	
	def download(self, r):
		self.remaining += 1
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
	
	# This is how to fetch another request
	def push(self, request):
		self.requests.append(request)
		self.serveNext()
		return 1
	
	# This is how to fetch several more requests
	def extend(self, requests):
		self.requests.extend(requests)
		self.serveNext()
		return len(requests)
	
	# These can be overridden to do various post-processing. For example, 
	# you might want to add more requests, etc.
	
	def onDone(self, request):
		pass
	
	def onSuccess(self, request):
		pass
	
	def onError(self, request):
		pass
	
	# These are internal callbacks
	def done(self, request):
		try:
			with self.lock:
				self.numFlight -= 1
				self.processed += 1
				self.remaining -= 1
				logger.info('Processed : %i | Remaining : %i | In Flight : %i | len : %i' % (self.processed, self.remaining, self.numFlight, len(self)))
			self.onDone(request)
		except Exception as e:
			logger.exception('BaseFetcher:onDone failed.')
		finally:
			self.serveNext()
	
	def success(self, request):
		try:
			self.onSuccess(request)
		except Exception as e:
			logger.exception('BaseFetcher:onSuccess failed.')
	
	def error(self, failure):
		try:
			self.onError(failure.value)
		except Exception as e:
			logger.exception('BaseFetcher:onError failed.')
	
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
			while (self.numFlight < self.poolSize) and len(self):
				r = self.pop()
				if r == None:
					break
				logger.debug('Requesting %s' % r.url)
				self.numFlight += 1
				try:
					# This is the expansion of the short version getPage
					# and is taken from twisted's source
					scheme, host, port, path = client._parse(r.url)
					factory = BaseRequestServicer(r, self.agent)
					# If http_proxy or https_proxy, or whatever appropriate proxy
					# is set, then we should try to honor that. We do so simply 
					# by overriding the host/port we'll connect to. The client
					# factory, BaseRequestServicer takes care of the rest
					proxy = os.environ.get('%s_proxy' % scheme)
					if proxy:
						scheme, host, port, path = client._parse(proxy)
					if scheme == 'https':
						from twisted.internet import ssl
						contextFactory = ssl.ClientContextFactory()
						reactor.connectSSL(host, port, factory, contextFactory)
					else:
						reactor.connectTCP(host, port, factory)
					factory.deferred.addCallback(r.success).addCallback(self.success)
					factory.deferred.addErrback(r.error).addErrback(self.error).addErrback(log.err)
					factory.deferred.addBoth(r.done).addBoth(self.done)
				except:
					self.numFlight -= 1
					logger.exception('Unable to request %s' % r.url)

# Now do a few imports for convenience
from PoliteFetcher import PoliteFetcher