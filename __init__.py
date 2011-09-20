#! /usr/bin/env python

import logging

logger = logging.getLogger('downpour')

# This tries to import the most efficient reactor 
# that's available on the system.
try:
	import twisted.internet.epollreactor as reactor
	print 'Using epoll reactor'
except ImportError:
	try:
		import twisted.internet.kqreactor as reactor
		print 'Using kqueue reactor'
	except ImportError:
		from twisted.internet import reactor
		print 'Using select(2) reactor'

from twisted.web import client, error
from twisted.internet import ssl

class BaseRequest(client.HTTPClientFactory):
	def __init__(self, url, timeout=15, redirectLimit=10):
		client.HTTPClientFactory.__init__(self, url, agent='SEOmoz Twisted Crawler', timeout=timeout, followRedirect=1, redirectLimit=redirectLimit)
	
	# Inheritable callbacks. You don't need to worry about
	# returning anything. Just go ahead and do what you need
	# to do with the input!
	def onPage(self, text):
		pass
	
	def onHeaders(self, headers):
		pass
	
	# Finished
	def done(self, response):
		return response

	# Made contact
	def success(self, response):
		print 'Fetched %s' % self.url
		return response

	# Failed to made contact
	def error(self, failure):
		if failure:
			print 'FAILED %s' % self.url
			return failure
		else:
			return None
	
	# Internal callbacks. These interface with twisted, and
	# are, frankly, a little weird to work with. They *do*
	# have to return their inherited method's results
	def page(self, text):
		self.onPage(text)
		return client.HTTPClientFactory.page(self, text)
	
	def noPage(self, reason):
		return client.HTTPClientFactory.noPage(self, reason)
	
	def gotHeaders(self, headers):
		self.onHeaders(headers)
		return client.HTTPClientFactory.gotHeaders(self, headers)

class BaseFetcher(object):
	def __init__(self, poolSize, urls):
		self.sslContext = ssl.ClientContextFactory()
		self.requests = urls
		self.poolSize = poolSize
		self.numFlight = 0
		self.serveNext()
	
	def download(self, r):
		self.requests.append(r)
		self.serveNext()
	
	def done(self, response):
		print '%i left in flight' % self.numFlight
		self.numFlight -= 1
		self.serveNext()
		if self.numFlight == 0:
			reactor.stop()
		return response
	
	def success(self, response):
		#print "Success: %s" % repr(response)
		return response
	
	def error(self, failure):
		#print "Error: %s" % failure.getErrorMessage()
		return failure
	
	def start(self):
		reactor.run()
	
	def stop(self):
		reactor.stop()
	
	def serveNext(self):
		while (self.numFlight < self.poolSize) and len(self.requests):
			r = self.requests.pop()
			self.numFlight += 1
			scheme, host, port, path = client._parse(r.url)
			reactor.connectTCP(host, port, r)
			if scheme == 'https':
				reactor.connectSSL(host, port, r, self.sslContext)
			else:
				reactor.connectTCP(host, port, r)
			r.deferred.addCallback(self.success).addCallback(r.success)
			r.deferred.addErrback(self.error).addErrback(r.error)
			r.deferred.addBoth(self.done).addBoth(r.done)
