# Request class

class Request(object):
    def __init__(self, url, meta=None, method='GET'):
        self.url    = url
        self.method = method
        self.meta   = meta

    def __hash__(self):
        return hash(self.url)

    def __cmp__(self, other):
        return cmp(hash(self), hash(other))

    def __repr__(self):
        return '<Request: %s (%s)>' % (
            self.url,
            'done: %d bytes' % len(self.response.data) if hasattr(self, 'response') else 'pending',
        )