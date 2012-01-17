#! /usr/bin/env python

import unittest
from downpour import Auth

def strToHeaders(s):
    # This converts the string version of headers to the kind of
    # dictionary that downpour would give back as part of response
    # headers.
    from collections import defaultdict
    headers = defaultdict(list)
    for h in s.strip().split('\n'):
        key, colon, value = h.partition(':')
        headers[key.strip().lower()].append(value.strip())
    return headers

class TestAuth(unittest.TestCase):
    def test_register_unregister(self):
        # Let's make sure that if we search for a non-existent 
        # user/pass pair, that we get None, None
        self.assertEqual(Auth.get('hello', 'world'), (None, None))
        
        # First, let's register a dummy user/pass, and then see if 
        # we can get it back from Auth
        Auth.register('foo', 'bar', 'user', 'pass')
        self.assertEqual(Auth.get('foo', 'bar'), ('user', 'pass'))
        
        # Now that we could get it back, let's unregister it and 
        # make sure that we get nothing back
        Auth.unregister('foo', 'bar')
        self.assertEqual(Auth.get('foo', 'bar'), (None, None))
        
        # Now let's try to register a default user/pass
        Auth.register('foo', None, 'user', 'pass')
        self.assertEqual(Auth.get('foo'       ), ('user', 'pass'))
        self.assertEqual(Auth.get('foo', None ), ('user', 'pass'))
        self.assertEqual(Auth.get('foo', 'bar'), ('user', 'pass'))
        # And now let's unregister it
        Auth.unregister('foo')
        self.assertEqual(Auth.get('foo'       ), (None, None))
        self.assertEqual(Auth.get('foo', None ), (None, None))
        self.assertEqual(Auth.get('foo', 'bar'), (None, None))
    
    def test_auth(self):
        # First, test the case where we have the particular realm registered
        headers = strToHeaders('''
            Authenticate: Basic realm="foo"
        ''')
        host = 'flipper.seomoz.org'
        Auth.register('flipper.seomoz.org', 'foo', 'Aladdin', 'open sesame')
        self.assertEqual(Auth.auth(host, None, headers), {'Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='})
        Auth.unregister(host, 'foo')
        
        # First, test the case where we don't have this realm registered
        headers = strToHeaders('''
            Authenticate: Basic realm="bar"
        ''')
        host = 'flipper.seomoz.org'
        self.assertEqual(Auth.auth(host, None, headers), {})
    
    def test_proxy_auth(self):
        # First, test the case where we have the particular realm registered
        headers = strToHeaders('''
            Proxy-Authenticate: Basic realm="foo"
        ''')
        host = 'flipper.seomoz.org'
        Auth.register('flipper.seomoz.org', 'foo', 'Aladdin', 'open sesame')
        self.assertEqual(Auth.auth(host, None, headers), {'Proxy-Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='})
        Auth.unregister(host, 'foo')
        
        # First, test the case where we don't have this realm registered
        headers = strToHeaders('''
            Proxy-Authenticate: Basic realm="bar"
        ''')
        host = 'flipper.seomoz.org'
        self.assertEqual(Auth.auth(host, None, headers), {})        
    
    def test_wild_proxy_auth(self):
        headers = strToHeaders('''
            Server: squid/2.7.STABLE6
            Date: Tue, 17 Jan 2012 20:41:42 GMT
            Content-Type: text/html
            Content-Length: 1303
            X-Squid-Error: ERR_CACHE_ACCESS_DENIED 0
            Proxy-Authenticate: Basic realm="Squid proxy-caching web server"
            X-Cache: MISS from flipper.seomoz.com
            Connection: close
        ''')
        host = 'flipper.seomoz.org'
        Auth.register(host, None, 'Aladdin', 'open sesame')
        self.assertEqual(Auth.auth(host, None, headers), {'Proxy-Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='})
        Auth.unregister(host)

if __name__ == '__main__':
    unittest.main()