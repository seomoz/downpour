#! /usr/bin/env python

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

class Request(client.HTTPClientFactory):
	def __init__(self, url, timeout=15, redirectLimit=10):
		client.HTTPClientFactory.__init__(self, url, agent='SEOmoz Twisted Crawler', timeout=timeout, followRedirect=1, redirectLimit=redirectLimit)
	
	# 200 received
	def page(self, text):
		#print 'Text %s => %s' % (self.url, text)
		return client.HTTPClientFactory.page(self, text)
	
	# Non-200
	def noPage(self, reason):
		return client.HTTPClientFactory.noPage(self, reason)
	
	def gotHeaders(self, headers):
		return client.HTTPClientFactory.gotHeaders(self, headers)
	
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

class Downpour(object):
	def __init__(self, poolSize, urls):
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
	
	def serveNext(self):
		while (self.numFlight < self.poolSize) and len(self.requests):
			r = self.requests.pop()
			self.numFlight += 1
			scheme, host, port, path = client._parse(r.url)
			reactor.connectTCP(host, port, r)
			r.deferred.addCallback(self.success).addCallback(r.success)
			r.deferred.addErrback(self.error).addErrback(r.error)
			r.deferred.addBoth(self.done).addBoth(r.done)
		    # factory = HTTPProgressDownloader(url, outputfile, *args, **kwargs)
		    # if scheme == 'https':
		    #     from twisted.internet import ssl
		    #     if contextFactory == None :
		    #         contextFactory = ssl.ClientContextFactory()
		    #     reactor.connectSSL(host, port, factory, contextFactory)
		    # else:
		    #     reactor.connectTCP(host, port, factory)

if __name__ == '__main__':
	import sys
	f = file('urls.txt')
	#reqs = [Request(u) for u in ['http://google.com', 'http://yahoo.com', 'http://wikipedia.org', 'http://apple.com']]
	reqs = [Request(u) for u in f.read().strip().split('\n')]
	f.close()
	d = Downpour(20, reqs)
	reactor.run()
