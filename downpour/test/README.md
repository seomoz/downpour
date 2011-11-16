downpour.test
=============

This module provides access to some tools that make it easier for you
to test your downpour-based code. For example, there is an `EchoServer`,
which allows you to specify exactly what HTTP response you'd like to 
receive on a per-request basis.

It also has an `ExpectRequest` class that allows you to expect certain
headers, statuses, text responses, failures, etc., or to execute your
own sanity checks with the result of those callbacks.

Echo Server
===========

The echo server can be initialized with a reactor, a base path, and a 
port, which default to the current working directory and 8080. It replies
with static content in the case of most files serving that directory,
with two notable exceptions:

1. The `http://localhost:8080/echo` endpoint will give the contents of your
	POST request as the entire HTTP response.
2. Any file ending in `.asis` is interpreted as an HTTP response, headers
	included.

These allow you to contrive your own headers, statuses and redirections
so that you can verify that you handle these cases correctly in your request
class. By way of an __example__, suppose that you have the following 
directory structure:

	- test_cases/
		- success/
			- 200.asis
		- redirect/
			- 301.asis
			- 302.asis
			- 302.html
		- bad/
			- 500.asis

If that's the case, then requests to `http://localhost:8080/test_cases/bad/500.asis`
will result in the content of `500.asis` being returned to the user as the
complete HTTP response. The headers in that file will be supplied to your
request's callbacks, as well as the status, and contents. A request to the
`http://localhost:8080/test_cases/redirect/302.html` will result in a 400 
status code, with the contents of `302.html`.

ExpectRequest / Examine Request
===============================

This is a class that accepts expectations of what will be received for 
various callbacks. With one exception, you can provide a bool, a method that
should return `True` or `False` when given the parameters given to the callback
or the value you'd expect the associated callback to be invoked with.

Suppose we want to make sure that we're completely sane when getting a reasonable
200 response:

	from downpour import BaseFetcher
	from downpour.test import ExpectRequest, host, run
	
	fetcher = BaseFetcher()
	
	fetcher.push(ExpectRequst('Test 200', 'test_cases/success/200.asis',
		expectURL     = host + 'test_cases/success/200.asis',
		expectStatus  = ('HTTP/1.1', '200', 'OK'),
		expectHeaders = {
			'content-length' : ['19'],
			'content-type'   : ['text/html']
		}, expectSuccess = 'This request is ok!'))
	
	# Use the downpour.test.run method to start tests
	run(fetcher)

Alternatively, you can provide a value of `True` for an expectation if you
expect the associated callback to be invoked, or `False` if you explicitly
expect a callback to not be invoked. Example:

	# Make sure the error handler is called,
	# and the success handler isn't.
	fetcher.push(ExpectRequest('Test 500 Error', 'test_cases/bad/500.asis',
		expectError = True,
		expectSuccess = False))
	
	# Make sure that it got headers
	fetcher.push(ExpectRequest('Test 200 Headers', 'test_cases/good/200.asis', 
		expectHeaders = True))

Alternatively, you might want to verify that a series of requests are piped
through a series of redirects. Suppose you've set up the `301.asis` to redirect
to the `200.asis` file:

	# Make sure that these urls are hit in order:
	fetcher.push(ExpectRequest('Test 301 Redirect', 'test_cases/redirect/301.asis',
		expectURL = [
			host + 'redirect/301.asis',
			host + 'good/200.asis'
		]))

Lastly, you can specify a method that is invoked as if it were the callback,
and it asserts that the return value is True.

	# Make sure that some kind of processing took place
	fetcher.push(ExamineRequest('Test Arbitrary', 'test_cases/good/200.asis',
		lambda request: request.url == (host + 'test_cases/good/200.asis')
	))
