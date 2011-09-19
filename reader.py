#! /usr/bin/env python

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

class Downpour(object):
	agent = Agent(reactor)
	
	def __init__(self, poolSize, urls):
		self.requests = urls
		self.poolSize = poolSize
		self.numFlight = 0
		self.serveNext()
	
	def append(self, r):
		self.requests.append(r)
		self.serveNext()
	
	def done(self, response):
		print '%i left in flight %s' % (self.numFlight, repr(response))
		self.numFlight -= 1
		self.serveNext()
		if self.numFlight == 0:
			reactor.stop()
	
	def success(self, response):
		return response
	
	def error(self, response):
		return response
	
	def serveNext(self):
		while (self.numFlight < self.poolSize) and len(self.requests):
			r = self.requests.pop()
			d = Downpour.agent.request(
				'GET',
				r.url,
				Headers({'User-Agent': ['SEOmoz Twisted Crawler']}),
				None)
			self.numFlight += 1
			d.addCallback(self.success).addCallback(r.success)
			d.addErrback(self.error).addErrback(r.error)
			d.addBoth(self.done)

class Request(object):
	def __init__(self, url):
		self.url = url
	
	def done(self, response):
		pass
	
	def success(self, response):
		print 'Fetched %s => %s' % (self.url, response)
	
	def error(self, response):
		print 'FAILED %s => %s' % (self.url, response)

if __name__ == '__main__':
	import sys
	f = file('urls.txt')
	#reqs = [Request(u) for u in ['http://google.com', 'http://yahoo.com', 'http://wikipedia.org', 'http://apple.com']]
	reqs = [Request(u) for u in f.read().strip().split('\n')]
	f.close()
	d = Downpour(20, reqs)
	reactor.run()
