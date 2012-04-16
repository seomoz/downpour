#!/usr/bin/env python

import os
import re
from twisted.web import server, resource, static, http

headerMatch = re.compile(r'([^:]+):([^\r]+)$')

def cleanHeaders(content):
    headers  = []
    body     = []
    encoding = None
    content  = content.split('\r\n')
    # If there were no headers according to this, then...
    if len(content) == 1:
        content = content[0].split('\n')
        headers.append(content[0])
        done = False
        for line in content[1:]:
            if not line:
                done = True
            elif not done:
                headers.append(line)
                name, colon, value = line.partition(': ')
                if name == 'Content-Type':
                    t, sep, charset = value.partition('; charset=')
                    if charset:
                        encoding = charset
            else:
                body.append(line)
        if encoding:
            body = '\n'.join(body).decode('utf-8').encode(encoding)
            for i in range(len(headers)):
                if headers[i].partition(':')[0] == 'Content-Length':
                    headers[i] = 'Content-Length: %i' % (len(body))
        else:
            body = '\n'.join(body)
        
        response = '\r\n'.join(headers) + '\r\n\r\n' + body
        # print response.replace('\r', '\\r')
        return response
    else:
        return '\r\n'.join(content)

class CleanASIS(static.ASISProcessor):
    def render(self, request):
        request.startedWriting = 1
        try:
            with file(self.path) as f:
                return cleanHeaders(f.read())
        except Exception as e:
            return resource.NoResource(repr(e)).render(request)

class Echo(resource.Resource):
    def __init__(self, *args, **kwargs):
        resource.Resource.__init__(self)
    
    def render(self, request):
        request.startedWriting = 1
        return cleanHeaders(request.content.read())
    
    def getChild(self, *args, **kwargs):
        return self
    
    def getChildWithDefault(self, *args, **kwargs):
        return self
    
    def getChildForRequest(self, *args, **kwargs):
        return self

class EchoServer(server.Site):
    def __init__(self, reactor, path='.', port=8080):
        # Make a root resource, and add children to it for
        # each of the types of requests we plan to service
        root = static.File(os.path.abspath(path))
        root.indexNames = ['index.html', 'index.txt', 'index.asis']
        root.processors = {
            '.asis' : CleanASIS
        }
        root.putChild('echo', Echo())
        # Initialize and listen
        server.Site.__init__(self, root)
        reactor.listenTCP(port, self)

if __name__ == '__main__':
    from twisted.internet import reactor
    s = EchoServer(reactor)
    reactor.run()
