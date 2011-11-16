#! /usr/bin/env python

import logging
from downpour import logger
from downpour.test import run, host
from downpour.test import ExpectRequest
from downpour.test import ExamineRequest
from downpour import BaseFetcher, BaseRequest

logger.setLevel(logging.CRITICAL)

fetcher = BaseFetcher(stopWhenDone=True)

# Try a plain-and-simple request
fetcher.push(ExpectRequest('200 Test', 'asis/ok.asis',
	expectHeaders = {
	'content-type': ['text/html'],
	'content-length': ['11']
}, expectStatus = ('HTTP/1.1', '200', 'OK'),
 	expectURL = host + 'asis/ok.asis',
	expectSuccess = 'Hello world'))

# Try a redirect request, making sure we get
# every url we expect to get.
fetcher.push(ExpectRequest('301 Redirect Test', 'asis/301_to_ok.asis', expectURL = [
	host + 'asis/301_to_ok.asis',
	host + 'asis/ok.asis'
]))

fetcher.push(ExpectRequest('302 Redirect Test', 'asis/302_to_ok.asis', expectURL = [
	host + 'asis/302_to_ok.asis',
	host + 'asis/ok.asis'
]))

# Expect that we get a failure from various bad requests
bad = [
	('HTTP/1.1', '400', 'Bad Request'),
	('HTTP/1.1', '401', 'Unauthorized'),
	('HTTP/1.1', '402', 'Payment Required'),
	('HTTP/1.1', '403', 'Forbidden'),
	('HTTP/1.1', '404', 'Not Found'),
	('HTTP/1.1', '405', 'Method Not Allowed'),
	('HTTP/1.1', '406', 'Not Acceptable'),
	('HTTP/1.1', '407', 'Proxy Authentication Required'),
	('HTTP/1.1', '408', 'Request Timeout'),
	('HTTP/1.1', '409', 'Conflict'),
	('HTTP/1.1', '410', 'Gone'),
	('HTTP/1.1', '411', 'Length Required'),
	('HTTP/1.1', '412', 'Precondition Failed'),
	('HTTP/1.1', '413', 'Request Entity Too Large'),
	('HTTP/1.1', '414', 'Request-URI Too Long'),
	('HTTP/1.1', '415', 'Unsupported Media Type'),
	('HTTP/1.1', '416', 'Request Range Not Satisfiable'),
	('HTTP/1.1', '417', 'Expectation Failed'),
	('HTTP/1.1', '500', 'Internal Server Error'),
	('HTTP/1.1', '501', 'Not Implemented'),
	('HTTP/1.1', '502', 'Bad Gateway'),
	('HTTP/1.1', '503', 'Service Unavailable'),
	('HTTP/1.1', '504', 'Gateway Timeout'),
	('HTTP/1.1', '505', 'HTTP Version Not Supported')
]

for b in bad:
	fetcher.push(ExpectRequest('%s Failure Test' % b[1], 'asis/%s.asis' % b[1],
		expectHeaders = True,
		expectURL     = True,
		expectStatus  = b,
		expectSuccess = False,
		expectError   = True))

class FollowRequest(BaseRequest):
	def onURL(self, url):
		self.url = url

fetcher.push(ExamineRequest('200 Examine Request', FollowRequest('asis/ok.asis'),
	lambda request: request.url == (host + 'asis/ok.asis')))

run(fetcher)
