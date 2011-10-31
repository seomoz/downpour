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
from twisted.internet import defer
from twisted.python.failure import Failure

# Make a filesystem path for this url
def makePath(url):
	parsed = urlparse.urlparse(url)
	# First, get the http / https / etc.
	path = parsed.scheme
	# And now the domain
	path = os.path.join(path, *parsed.hostname.split('.'))
	# And port if it exists, or the default port
	path = os.path.join(path, str(parsed.port) or '80')
	# We need to strip off the first '/'
	path = os.path.join(path, *[p for p in parsed.path.split('/') if p])
	# There's not a really good way to do this in a way that preserves
	# the actual URL in the filename. Originally, I tried using base64
	# encoding, but that can quickly lead to filenames that are too long
	# for the filesystem.
	#
	# Other serialization techniques make it difficult to distinguish 
	# directories from files. For example, if you say that for all urls
	# that are just a directory, turn it into dir/index, that precludes
	# the possibility of dir/index/*. Basically, we can't have any file
	# names that could appear in the URL as a directory.
	#
	# So, instead, I'm just going to provide the python-provided hash
	# of the entire url as the string. It's sad that it's a one-way mapping,
	# but it appears to be the only way.
	return os.path.join(path, str(url.__hash__()))

def getPath(path):
	# Ensure that there's a directory
	d, f = os.path.split(path)
	try:
		os.makedirs(d)
	except OSError:
		# Ignore if the directory exists
		pass
	return path

def store(base, url, obj):
	# I think it's best that this doesn't throw any exceptions.
	# Even though the callbacks onSuccess, onError, etc. are protected in
	# their own try/except blocks, a failure here should not preclude the
	# execution of the base request's callback
	try:
		path = os.path.join(base, makePath(url))
		with file(getPath(path), 'w+') as f:
			pickle.dump(obj, f)
	except:
		logger.exception('Problem storing %s from %s' % (repr(obj), url))

def service(request, base):
	'''Attempt to service a particular request. Return the url to request
	or None if it was possible to service completely.'''
	try:
		url = request.url
		while url:
			# Follow redirects indefinitely. Build the path it would have in the cache
			path = os.path.join(base, makePath(url))
			# If the path doesn't exist, we can't service this request from
			# the cache, so we should return the current url we're working on
			if not os.path.exists(path):
				return url
			# Otherwise, read the file we have cached and attempt to service the request
			with file(getPath(path), 'r') as f:
				logger.debug('Reading from cache %s => %s' % (url, path))
				obj = pickle.load(f)
		
			# Invoke the status callback
			status = obj.get('status', None)
			if status:
				logger.debug('onStatus(%s)' % ', '.join(status))
				# This is a tuple, so we need to expand it
				request.onStatus(*status)
		
			# Now invoke the headers callback
			headers = obj.get('headers', None)
			if headers:
				logger.debug('onHeaders(%s)' % repr(headers))
				request.onHeaders(headers)
		
			# Now, we'll either invoke the onURL, or the onDone, etc. callbacks
			url = obj.get('url', None)
			if url:
				logger.debug('Forwarded to %s' % url)
				# Just move on to the next followed url
				continue
		
			# The success callback
			success = obj.get('success', None)
			if success:
				logger.debug('onSuccess')
				d = defer.Deferred()
				d.addCallback(request.onSuccess).addBoth(request.onDone)
				reactor.callLater(0, d.callback, success)
				# We were able to service this request, so return None
				return None
		
			# The failure callback
			failure = obj.get('error', None)
			if failure:
				logger.debug('onError')
				d = defer.Deferred()
				d.addCallback(request.onError).addBoth(request.onDone)
				reactor.callLater(0, d.callback, failure)
				# We were able to service this request, so return None
				return None
	except:
		logger.exception('Failed to run service %s' % request.url)
		return None
	return None

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
		store(self.base, self.url, {
			'status': self.status,
			'headers': self.headers,
			'success': text
		})
		self.request.onSuccess(text)
	
	def onError(self, failure):
		# First, try to store this obj
		store(self.base, self.url, {
			'status': self.status,
			'headers': self.headers,
			'error': failure
		})
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
		store(self.base, self.url, {
			'url': url,
			'status': self.status,
			'headers': self.headers,
		})
		self.url = url

class CachedFetcher(object):
	# The cached fetcher should implement the same /interface/, but should not
	# inherit base functionality from the BaseFetcher class. To do so would mean
	# potentially accidentally calling code centered around /actually/ fetching
	# asynchronously, when this implements synchronous caching, and otherwise
	# passes the buck to the provided fetcher
	def __init__(self, fetcher, base='./'):
		# Only service one request at a time. It's ok, though -- we're not going to
		# be relying on this too heavily.
		self.fetcher = fetcher
		# It's important to get the absolute path
		self.base = os.path.abspath(base)
	
	# For completeness of the interface
	def __len__(self):
		return len(self.fetcher)
		
	# Pass the buck
	def push(self, request):
		count = 0
		# Get the URL we'd like to service, or None if we serviced it
		url = service(request, self.base)
		if url:
			# If we did get a URL back
			logger.debug('%s is not completely cached.' % request.url)
			count += self.fetcher.push(CachedRequest(url, self.base, request))
		return count

	# Pass the buck
	def extend(self, requests):
		count = 0
		for r in requests:
			url = service(request, self.base)
			if url:
				# If we did get a URL back
				logger.debug('%s is not completely cached.' % request.url)
				count += self.fetcher.push(CachedRequest(url, self.base, request))
		return count
	
	def onDone(self, request):
		pass

	def onSuccess(self, request):
		pass

	def onError(self, request):
		pass
	
	# These are internal callbacks
	def done(self, request):
		logger.warn('CachedFetcher::done called')

	def success(self, request):
		logger.warn('CachedFetcher::success called')

	def error(self, failure):
		logger.warn('CachedFetcher::error called')
	
	def start(self):
		self.fetcher.start()
	
	def stop(self):
		self.fetcher.stop()
		
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