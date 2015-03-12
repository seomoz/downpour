![Status: Deprecated](https://img.shields.io/badge/status-deprecated-green.svg?style=flat)
![Team: Big Data](https://img.shields.io/badge/team-big_data-green.svg?style=flat)
![Scope: Internal](https://img.shields.io/badge/scope-internal-lightgrey.svg?style=flat)
![Product: None (deprecated)](https://img.shields.io/badge/products-none_%28deprecated%29-blue.svg?style=flat)
![Open Source: Unknown](https://img.shields.io/badge/open_source-unknown-orange.svg?style=flat)
![Critical Code: No](https://img.shields.io/badge/critical-no-red.svg?style=flat)

Main purpose: Twisted-based crawler  
E-Mail: [dan@moz.com](mailto:dan@moz.com)  
MOZ Package Dependencies: reppy, qr  
Open Source Dependencies: twisted  

----------

Downpour
========

Downpour is a helpful bit of glue code to facilitate the service of a large number of HTTP requests
using the [Twisted](http://twistedmatrix.com/trac/) library. It encapsulates two notions: _requests_
and _policies_.

A request is an object that represents the endpoint you'd like to request, any data associated with
it, and the callbacks that should happen when successful, completed, or failed. A policy, on the 
other hand, represents the way in which requests are serviced and scheduled. By default, downpour
comes with two policies, a `BaseFetcher` and a `PoliteFetcher`, which tries to honor politeness at
the fully-qualified-domain-name (fqdn) level.

Using
=====

The following example should help illuminate how to use downpour:

	import downpour
	
	class Request(downpour.BaseRequest):
		def onURL(self, url):
			self.url = url
		
		def onSuccess(self, text):
			print 'Successfully fetched %s' % self.url
	
	fetcher = downpour.PoliteFetcher(delay=1, allowAll=True)
	
	with file('urls.txt') as f:
		fetcher.extend([Request(line.strip()) for line in f])
	
	fetcher.start()

Requests
========

To create your own requests, inherit from the `downpour.BaseRequest` class, and override the methods
(if you need to):

	import downpour
	
	class MyRequest(downpour.BaseRequest):
		'''Do something totally awesome!'''
		
		def onSuccess(self, text):
			'''I fetched the page and got text'''
		
		def onError(self, failure):
			'''I got a twisted failure object'''
		
		def onDone(self, response):
			'''I either got text, or a failure, but it's done in either case.'''
		
		def onHeaders(self, headers):
			'''I got some headers from the page I'm fetching. This can be called
			multiple times, as redirection happens transparently. Also, this is
			a dictionary of lists, as headers can appear multiple time. Also, keys
			are all lowercase.'''
			for key, value in headers.items():
				print '%s => %s' % (key, '; '.join(value))
		
		def onStatus(self, version, status, message):
			'''Got a status from the URL I'm currently fetching. Each is a string.'''
		
		def onURL(self, url):
			'''Redirection happened. This is the current url.'''

The request exposes access to the status, url (when redirection automatically occurs), and the headers
received. The base request class does very little with them itself, outside of what it must in order
to provide you access to callbacks. Of course, your callbacks shouldn't raise exceptions, but the 
`BaseRequest` class traps all of them.

The Requests class also examines the `http_proxy` environment variable. If set, requests will be 
routed through the specified proxy transparently.

Policies
========

You can create your own fetching policies to service requests, or you can use one provided with downpour.

BaseFetcher
-----------

This fetcher is dumb as dirt, except that it makes the promise that it won't lose track of a request, and
will make sure it calls your own fetcher's `onDone`, `onSuccess`, and `onError` callbacks. It provides no
synchronization, or politeness, or queueing of any kind.

PoliteFetcher
-------------

The polite fetcher will wait a default configurable delay before fetching from the same fqdn again. It 
makes heavy use of redis to manage its queues, and serializes requests out to redis. In order to use the
`PoliteFetcher`, you must:

- Run an instance of redis locally
- Your `Request` class must be `pickle` serializable

There are plans to incorporate robots.txt politeness directly into `PoliteFetcher`, but that's not yet been
done.

Writing Your Own
----------------

To write your own policy, inherit from `downpour.BaseFetcher`. The `__init__` function accepts a pool size,
a set of requests, and then a user-agent to use for requests. You must implement the following methods:

	import downpour
	
	class MyFetcher(downpour.BaseFetcher):
		'''Arguably the best fetcher ever'''
		
		def __len__(self):
			'''How many requests remain to be fetched?'''
		
		def pop(self):
			'''Get the next request to service, or None if there is none ready.'''
		
		def push(self, r):
			'''Same as download(self, r)'''
			# Serve the next request, if there is one ready
			self.serveNext()
		
		def extend(self, requests):
			'''Enqueue several requests at once'''
			# Serve the next request, if there is one ready
			self.serveNext()
		
		def onDone(self, request):
			'''If your fetching logic needs to know when a request finishes.'''
		
		def onSuccess(self, request):
			'''If your fetching logic needs to know when a request is successful.'''
		
		def onError(self, request):
			'''If your fetching logic needs to know when a request failed.'''
		
		def start(self):
			'''Start fetching. Call downpour.BaseFetcher.start(self)'''
		
		def stop(self):
			'''Stop fetching. Call downpour.BaseFetcher.stop(self)'''


