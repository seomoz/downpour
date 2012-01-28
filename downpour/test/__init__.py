#! /usr/bin/env python

'''Some tools to make testing easier when using downpour. This module
provides a server that knows a couple of cool tricks, including returning
the HTTP response you supply in POST, and returning HTTP responses stored
in files locally (ending with .asis), and everything else is returned as
static content.

In addition, this defines a class that enables you to conduct tests at each
of the callbacks provided by a downpour BaseRequest object.
'''

import os
import time
import urlparse
import unittest
from downpour import logger
from downpour import reactor
from downpour import BaseRequest
from downpour import BaseFetcher
from echoServer import EchoServer
from twisted.python.failure import Failure

# Make an echo server with downpour's reactor
s = EchoServer(reactor)

# The host you should send requests to
host = 'http://localhost:8080/'

failures  = []
successes = []

def run(fetcher, callback=None, *args, **kwargs):
    '''Start the fetcher.  After the fetcher finishes, execute callback.'''
    expected = len(fetcher)
    fetcher.stopWhenDone = True
    fetcher.start()
    for r in failures:
        print 'FAILED: %s => %s' % (r.name, r.url)
    for r in successes:
        print 'PASSED: %s => %s' % (r.name, r.url)
    print 'FAILED %i' % len(failures)
    print 'PASSED %i' % len(successes)
    if callback:
        callback(*args, **kwargs)
    if len(failures):
        print 'FAILED.'
        exit(1)
    elif len(successes) < expected:
        print 'FAILED. Not all expected tests ran.'
        exit(1)
    else:
        print 'PASSED'
        exit(0)

class UnittestRequest(BaseRequest):
    def __init__(self, name, *args, **kwargs):
        BaseRequest.__init__(self, *args, **kwargs)
        self.name      = name
        self.failures  = []
        self.successes = 0
    
    def assertTrue(self, expression, msg=None):
        try:
            assert(expression)
            self.successes += 1
        except AssertionError:
            self.failures.append(self)
    
    def assertEqual(self, a, b, msg=None):
        try:
            assert(a == b)
            self.successes += 1
        except AssertionError:
            print msg or 'Unexpected: %s != %s' % (repr(a), repr(b))
            self.failures.append(self)
    
    def assertNotEqual(self, a, b, msg=None):
        try:
            assert(a != b)
            self.successes += 1
        except AssertionError:
            print msg or 'Unexpected: %s == %s' % (repr(a), repr(b))
            self.failures.append(self)
    
    def onDone(self, *args, **kwargs):
        try:
            if not len(self.failures):
                successes.append(self)
            else:
                failures.append(self)
        except Exception as e:
            print repr(e)

class ExamineRequest(UnittestRequest):
    def __init__(self, name, request, examine):
        self.request = request
        self.examine = examine
        # If it was just a filename, go ahead and append it to
        # the EchoServer's host. Otherwise, use the url
        UnittestRequest.__init__(self, name, request.url, request.data,
                request.proxy)
    
    def onHeaders(self, *args, **kwargs):
        self.request.onHeaders(*args, **kwargs)
    
    def onStatus(self, *args, **kwargs):
        self.request.onStatus(*args, **kwargs)
    
    def onURL(self, *args, **kwargs):
        self.request.onURL(*args, **kwargs)
    
    def onSuccess(self, *args, **kwargs):
        self.request.onSuccess(*args, **kwargs)
    
    def onError(self, *args, **kwargs):
        self.request.onError(*args, **kwargs)
    
    def onDone(self, *args, **kwargs):
        logger.debug('onDone')
        self.request.onDone(*args, **kwargs)
        logger.debug('request onDone complete')
        # Alright, now that all the callbacks have been invoked,
        # we should invoke the examine command to ensure that 
        # this object is in the state we'd expect it to be in.
        self.assertTrue(self.examine(self.request))
        UnittestRequest.onDone(self, *args, **kwargs)

class ExpectRequest(UnittestRequest):
    '''This hits the echo server's as-is endpoint, which responds
    with the text provided in post.'''
    def __init__(self, name, url, data = None,
        expectHeaders = None,
        expectStatus  = None,
        expectURL     = None,
        expectSuccess = None,
        expectError   = None,
        expectDone    = None):
        
        UnittestRequest.__init__(self, name, url, data)
        self.expectHeaders = expectHeaders
        self.expectStatus  = expectStatus
        self.expectURL     = expectURL
        self.expectSuccess = expectSuccess
        self.expectError   = expectError
        self.expectDone    = expectDone
        
        self.redirectCount = 0
        self.checklist = {
            'onHeaders': 0,
            'onStatus' : 0,
            'onURL'    : 0,
            'onSuccess': 0,
            'onError'  : 0
        }
        
        self.failures  = []
    
    def onHeaders(self, headers):
        # Increment the counter of  how many times we've seen this method
        self.checklist['onHeaders'] += 1
        # Now try to conduct the appropriate test.
        if isinstance(self.expectHeaders, bool):
            self.assertTrue(self.expectHeaders, 'Expected headers')
        elif isinstance(self.expectHeaders, dict):
            for key in self.expectHeaders:
                self.assertEqual(self.expectHeaders[key], headers[key])
        elif self.expectHeaders:
            self.assertTrue(self.expectHeaders(self, headers))
    
    def onStatus(self, version, status, message):
        # Increment the counter of  how many times we've seen this method
        self.checklist['onStatus'] += 1
        # Now try to conduct the appropriate test.
        if isinstance(self.expectStatus, bool):
            self.assertTrue(self.expectStatus, 'Expected status')
        elif isinstance(self.expectStatus, tuple):
            self.assertEqual((version, status, message), self.expectStatus)
        elif self.expectStatus:
            self.assertTrue(self.expectStatus(self, version, status, message))
    
    def onURL(self, url):
        # Increment the counter of  how many times we've seen this method
        self.checklist['onURL'] += 1
        # Now try to conduct the appropriate test.
        if isinstance(self.expectURL, bool):
            self.assertTrue(self.expectURL, 'Expected URL')
        elif isinstance(self.expectURL, basestring):
            self.assertEqual(url, self.expectURL)
        elif isinstance(self.expectURL, list):
            self.assertEqual(url, self.expectURL[self.redirectCount])
        elif self.expectURL:
            self.assertTrue(self.expectURL(self, url))
        self.redirectCount += 1
    
    def onSuccess(self, text, fetcher):
        # Increment the counter of  how many times we've seen this method
        self.checklist['onSuccess'] += 1
        # Now try to conduct the appropriate test.
        if isinstance(self.expectSuccess, bool):
            self.assertTrue(self.expectSuccess, 'Expected success')
        elif isinstance(self.expectSuccess, basestring):
            self.assertEqual(text, self.expectSuccess)
        elif self.expectSuccess:
            self.assertTrue(self.expectSuccess(self, text, fetcher))
    
    def onError(self, failure, fetcher):
        # Increment the counter of  how many times we've seen this method
        self.checklist['onError'] += 1
        # Now try to conduct the appropriate test.
        if isinstance(self.expectError, bool):
            self.assertTrue(self.expectError, 'Expected error')
        elif isinstance(self.expectError, Failure):
            self.assertEqual(failure, self.expectError)
        elif self.expectError:
            self.assertTrue(self.expectError(self, failure, fetcher))
    
    def onDone(self, results, fetcher):
        # Make sure that exactly one of these has happened.
        self.assertEqual(self.checklist['onSuccess'] + self.checklist['onError'], 1, '%s => Neither success nor failure called.' % self.url)
        # Make sure that onURL was called at least once
        self.assertNotEqual(self.checklist['onURL'], 0, '%s => onURL not called' % self.url)
        # If we were successful, then make sure these happened:
        if (self.checklist['onSuccess']):
            self.assertNotEqual(self.checklist['onHeaders'], 0, '%s => onHeaders not called' % self.url)
            self.assertNotEqual(self.checklist['onStatus'] , 0, '%s => onStatus not called' % self.url)
        # Now, make sure that everything that the user expect to fire
        # actually /did/ fire
        for i in ['Headers', 'Status', 'Error', 'Success']:
            expect = getattr(self, 'expect%s' % i)
            if expect:
                self.assertNotEqual(self.checklist['on%s' % i], 0, '%s => on%s executed 0 times, expected more' % (self.url, i))
            elif isinstance(expect, bool):
                self.assertEqual(self.checklist['on%s' % i], 0, '%s => on%s executed, expected to not' % (self.url, i))
        # Now, if an expectDone attribute was provided, then 
        # we should execute it with the request object provided
        if self.expectDone:
            self.assertTrue(self.expectDone(self))
        UnittestRequest.onDone(self, results, fetcher)

if __name__ == '__main__':
    # This simply serves as an example, and isn't really meant for executing
    import logging
    from downpour import logger
    logger.setLevel(logging.DEBUG)
    
    fetcher.push(ExpectRequest('asis/ok.asis', None, {
        'content-type': ['text/html'],
        'content-length': ['11']
    }, ('HTTP/1.1', '200', 'OK'), 'http://localhost:8080/asis/ok.asis', 'Hello world'))
    
    fetcher.push(ExpectRequest('asis/301_to_ok.asis', expectURL = [
        'http://localhost:8080/asis/301_to_ok.asis',
        'http://localhost:8080/asis/ok.asis'
    ]))
    
    reactor.run()
