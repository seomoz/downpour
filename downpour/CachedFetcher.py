#! /usr/bin/env python

'''Fetch once, cache to disk, and all subsequent requests are served from disk'''

import os
import base64
import urlparse
import cPickle as pickle
from downpour import logger
from downpour import reactor
from downpour import BaseFetcher
from downpour import BaseRequest
from twisted.python.failure import Failure

# Make a filesystem path for this url
def makePath(url):
	parsed = urlparse.urlparse(url)
	# First, get the http / https / etc.
	path = parsed.scheme
	# And now the domain
	path = os.path.join(path, *parsed.hostname.split('.'))
	# And port if it exists, or the default port
	path = os.path.join(path, parsed.port or '80')
	# We need to strip off the first '/'
	path = os.path.join(path, *[p for p in parsed.path.split('/') if p])
	if parsed.query:
		path = os.path.join(path, base64.b64encode(parsed.query))
	return path

def getFile(path, mode='r'):
	# Ensure that there's a directory
	d, f = os.path.split(path)
	try:
		os.makedirs(d)
	except OSError:
		# Ignore if the directory exists
		pass
	return file(path, mode)

# Service this particular request
def service(request, base):
	# Alright, follow redirects indefinitely
	url = request.url
	while url:
		path = os.path.join(base, makePath(url))
		with getFile(path) as f:
			logger.debug('\tReading %s' % path)
			obj = pickle.load(f)
			# Invoke the status callback
			status = obj.get('status', None)
			if status:
				logger.debug('\tonStatus(%s)' % ', '.join(status))
				# This is a tuple, so we need to expand it
				request.onStatus(*status)
			# Now invoke the headers callback
			headers = obj.get('headers', None)
			if headers:
				logger.debug('\tonHeaders(%s)' % repr(headers))
				request.onHeaders(headers)
			# Now, we'll either invoke the onURL, or the onDone, etc. callbacks
			url = obj.get('url', None)
			if url:
				logger.debug('\tForwarded to %s' % url)
				# Just move on to the next followed url
				continue
			# The success callback
			success = obj.get('success', None)
			if success:
				logger.debug('\tonSuccess')
				request.onSuccess(success)
				request.onDone(request)
			# The failure callback
			failure = obj.get('error', None)
			if failure:
				logger.debug('\tonError')
				request.onError(failure)
				request.onDone(Failure(request))		

# Do we have a cached copy?
def exists(request, base):
	return os.path.exists(os.path.join(base, makePath(request.url)))

class CachedRequest(BaseRequest):
	def __init__(self, url, base, request):
		self.url     = url
		self.base    = base
		self.request = request
		self.headers = None
		self.status  = None
	
	# Inheritable callbacks. You don't need to worry about
	# returning anything. Just go ahead and do what you need
	# to do with the input!
	def onSuccess(self, text):
		path = os.path.join(self.base, makePath(self.url))
		with getFile(path, 'w+') as f:
			pickle.dump({
				'status': self.status,
				'headers': self.headers,
				'success': text
			}, f)
		self.request.onSuccess(text)
	
	def onError(self, failure):
		path = os.path.join(self.base, makePath(self.url))
		with getFile(path, 'w+') as f:
			pickle.dump({
				'status': self.status,
				'headers': self.headers,
				'error': failure
			}, f)
		self.request.onError(failure)
	
	def onDone(self, response):
		self.request.onDone(response)
	
	def onHeaders(self, headers):
		self.headers = headers
		self.request.onHeaders(headers)
	
	def onStatus(self, version, status, message):
		self.status = (version, status, message)
		self.request.onStatus(version, status, message)
	
	def onURL(self, url):
		path = os.path.join(self.base, makePath(self.url))
		# Now, we should write what information we have out
		with getFile(path, 'w+') as f:
			pickle.dump({
				'url': url,
				'status': self.status,
				'headers': self.headers,
			}, f)

class CachedFetcher(BaseFetcher):
	def __init__(self, fetcher, base='./'):
		# Only service one request at a time. It's ok, though -- we're not going to
		# be relying on this too heavily.
		BaseFetcher.__init__(self, 1)
		self.fetcher = fetcher
		# It's important to get the absolute path
		self.base = os.path.abspath(base)
	
	# For completeness of the interface
	def __len__(self):
		return len(self.fetcher)
		
	# Pass the buck
	def push(self, request):
		if not exists(request, self.base):
			logger.debug('%s is not cached.' % request.url)
			self.fetcher.push(CachedRequest(request.url, self.base, request))
		else:
			logger.debug('%s is cached.' % request.url)
			service(request, self.base)

	# Pass the buck
	def extend(self, requests):
		# This is a pretty clever piece of code found on StackOverflow. Props to Ants Aasma:
		# http://stackoverflow.com/q/949098/173556
		from itertools import tee
		# Make two generators for exists/request pairs
		g1, g2 = tee((exists(request, self.base), request) for request in requests)
		# Extend for those that haven't been cached
		self.fetcher.extend(CachedRequest(request.url, self.base, request) for exists, request in g1 if not exists)
		for exists, request in g2:
			if exists:
				service(request, self.base)
	
	def serveNext(self):
		# Just in case this gets called by accident
		pass
		
if __name__ == '__main__':
	import logging
	from downpour import BaseRequest
	
	# Turn on logging
	logger.setLevel(logging.DEBUG)
	
	# An object for fetching
	bf = BaseFetcher()
	# Cached fetcher
	cf = CachedFetcher(bf)
	
	with file('urls.txt') as f:
		for line in f:
			cf.push(BaseRequest(line.strip()))
	
	cf.start()