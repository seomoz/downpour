#! /usr/bin/env python
#
# Copyright (c) 2011 SEOmoz
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

__author__     = 'Dan Lecocq'
__copyright__  = '2011 SEOmoz'
__license__    = 'SEOmoz'
__version__    = '0.1.0'
__maintainer__ = 'Dan Lecocq'
__email__      = 'dan@seomoz.org'
__status__     = 'Development'

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
import cPickle as pickle
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

class UserPreemptionError(error.Error):
	'''The exception raised when a `Request.cancel` is called'''
	def __init__(self, code, reason):
		error.Error(self, code, reason)
		self.reason = reason
		self.code = code
	
	def __repr__(self):
		return '%s => %s' % (self.code, repr(self.reason))
	
	def __str__(self):
		return '%s => %s' % (self.code, str(self.reason))

class BaseRequestServicer(client.HTTPClientFactory):
	'''This class services requests, providing the request with
	additional callbacks beyond those typically provided. For 
	example, it's by way of this class that `onHeaders`, `onURL`,
	and `onStatus` are supported.'''
	def __init__(self, request, agent):
		'''Provide the request to service, and the user agent to identify with.'''
		self.request = request
		self.request.cached = True
		self.request.factory = self
		client.HTTPClientFactory.__init__(self, url=request.url, agent=agent, timeout=request.timeout, redirectLimit=request.redirectLimit, postdata=self.request.data)
	
	def setURL(self, url):
		'''Called when redirection occurs, with the new url.
		This method is aware of the `*_proxy` environment 
		variables, and so if present, it will override the 
		default action, but the redirected url will still appear
		as the argument to the request callback.'''
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
		'''Received headers, a dictionary of lists.'''
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
		'''Received the HTTP version, status and status message.'''
		try:
			self.request.onStatus(version, status, message)
		except:
			logger.exception('%s onStatus failed' % self.request.url)
		client.HTTPClientFactory.gotStatus(self, version, status, message)
	
	def buildProtocol(self, *args, **kwargs):
		'''In order to facilitate user preemption, we need to remember
		the protocol we made. So, save it and pass through.'''
		self.p = client.HTTPClientFactory.buildProtocol(self, *args, **kwargs)
		return self.p
	
	def cancel(self, reason):
		'''If the user needs to preempt the transfer. For example, if looking
		at the content headers, we decide we don't want to get the file.'''
		err = UserPreemptionError(self.status, reason)
		self.noPage(Failure(err))
		self.p.quietLoss = True
		self.p.transport.loseConnection()

class BaseRequest(object):
	urlRE = re.compile(r'#.+$')
	timeout = 45
	redirectLimit = 10
	
	def __init__(self, url, data=None):
		self.url = self.urlRE.sub('', url)
		self.data = None
	
	def __del__(self):
		# For a brief while, I was having problems with memory leaks, and so 
		# I was printing this out in order to help make sure that requests
		# were getting freed. It has been a long time since that ugly day, but
		# this will stay as a reminder. FWIW, Python's garbage collection is
		# based on reference counting, which cannot detect leaks in the form
		# of isolated cliques with no external references (circular reference)
		# logger.debug('Deleting request for %s' % self.url)
		pass
	
	def cancel(self, reason):
		'''If for any reason, you discover you don't want to fetch
		this particular resource, then you can cancel it'''
		self.factory.cancel(reason)
	
	def __getstate__(self):
		'''This is a section of code of which I am not terribly proud.
		Unfortunately, it's necessary for the time being.'''
		try:
			del self.factory
		except:
			pass
		return self.__dict__
	
	# Inheritable callbacks. You don't need to worry about
	# returning anything. Just go ahead and do what you need
	# to do with the input!
	def onSuccess(self, text, fetcher):
		pass
	
	def onError(self, failure, fetcher):
		pass
	
	def onDone(self, response, fetcher):
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
	def _done(self, response, fetcher):
		try:
			self.onDone(response, fetcher)
			try:
				del self.factory
			except AttributeError:
				pass
		except Exception as e:
			logger.exception('Request done handler failed')
		return self

	# Made contact
	def _success(self, response, fetcher):
		try:
			logger.info('Successfully fetched %s' % self.url)
			self.onSuccess(response, fetcher)
		except Exception as e:
			logger.exception('Request success handler failed')
		return self

	# Failed to made contact
	def _error(self, failure, fetcher):
		try:
			try:
				failure.raiseException()
			except:
				logger.exception('Failed for %s' % self.url)
			self.onError(failure, fetcher)
		except Exception as e:
			logger.exception('Request error handler failed')
		return Failure(self)

class BaseFetcher(object):
	def __init__(self, poolSize=10, agent=None, stopWhenDone=False):
		self.sslContext = ssl.ClientContextFactory()
		# The base fetcher keeps track of requests as a list
		self.requests = []
		# A limit on the number of requests that can be in flight
		# at the same time
		self.poolSize = poolSize
		# Keeping tabs on counts. The lock is necessary to avoid 
		# contentious access to numFlight, processed, remaining.
		# numFlight => the number of requests currently active
		# processed => the number of requests completed
		# remaining => how many requests are left
		self.lock = threading.Lock()
		self.numFlight = 0
		self.processed = 0
		self.remaining = 0
		# Use this user agent when making requests
		self.agent = agent or 'rogerbot/1.0'
		self.stopWhenDone = stopWhenDone
	
	# This is how subclasses communicate how many requests they have 
	# left to fulfill. 
	def __len__(self):
		return self.remaining
	
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
		self.remaining += 1
		return 1
	
	# This is how to fetch several more requests
	def extend(self, requests):
		self.requests.extend(requests)
		self.serveNext()
		self.remaining += len(requests)
		return len(requests)
	
	# This is a way for the fetcher to let you know that it is capable of
	# handling more requests than are currently enqueued. Returns how much
	# the queue grew by. The count is an estimate of how many new requests
	# would be appropriate. This call is responsible for correctly keeping
	# self.remaining updated. As such, it's recommended to internally make
	# calls to `extend` or `push` for that purpose
	def grow(self, count):
		return 0
	
	# These can be overridden to do various post-processing. For example, 
	# you might want to add more requests, etc.
	def onDone(self, request):
		pass
	
	def onSuccess(self, request):
		pass
	
	def onError(self, request):
		pass
	
	# These are how you can start and stop the reactor. It's a convenience
	# so that you don't have to import reactor when you want to use this
	def start(self):
		self.serveNext()
		reactor.run()

	def stop(self):
		reactor.stop()
	
	# These are internal callbacks, and should generally not be modified
	# in descendent classes. They manage the proper execution of a number
	# of requests at a single time, and changing them can result in deadlock,
	# premature termination, or memory leaks. CHANGE WITH CARE. In particular,
	# it's important that these functions do not throw errors, as every
	# step provided is important. Instead, use the convenience methods that
	# are excuted in association with these functions: `onSuccess`, `onDone`
	# and `onError`. Exceptions thrown in those functions do not affect the
	# performance of the internal logic in these methods.
	def _done(self, request):
		'''A request has completed'''
		try:
			with self.lock:
				self.numFlight -= 1
				self.processed += 1
				self.remaining -= 1
				logger.info('Processed : %i | Remaining : %i | In Flight : %i' % (self.processed, self.remaining, self.numFlight))
			self.onDone(request)
		except Exception as e:
			logger.exception('BaseFetcher:onDone failed.')
		finally:
			# If there are no more requests being serviced, and no requests
			# waiting to be serviced, the perhaps it is time to stop.
			if self.stopWhenDone and not self.numFlight and not self.remaining:
				self.stop()
				return
			self.serveNext()
	
	def _success(self, request):
		'''A request has completed successfully.'''
		try:
			self.onSuccess(request)
		except Exception as e:
			logger.exception('BaseFetcher:onSuccess failed.')
	
	def _error(self, failure):
		'''A request resulted in this failure'''
		try:
			self.onError(failure.value)
		except Exception as e:
			logger.exception('BaseFetcher:onError failed.')
	
	# This repeatedly services available requests while there are spots open
	# and there are requests to be serviced. If there are no queued requests,
	# then it will attempt to grow the queue with a call to `grow`, which 
	# must return by how much the queue grew.
	def serveNext(self):
		with self.lock:
			while self.numFlight < self.poolSize:
				r = self.pop()
				if r == None:
					# If nothing was fetchable, then try to grow the number
					# of requests to service.
					if not self.grow(self.poolSize):
						return
					else:
						logger.debug('Grew queue...')
						continue
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
					factory.deferred.addCallback(r._success, self).addCallback(self._success)
					factory.deferred.addErrback(r._error, self).addErrback(self._error).addErrback(log.err)
					factory.deferred.addBoth(r._done, self).addBoth(self._done)
				except:
					self.numFlight -= 1
					logger.exception('Unable to request %s' % r.url)

# Now do a few imports for convenience
from PoliteFetcher import PoliteFetcher