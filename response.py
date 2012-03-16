# Response class

class Response(object):
    def __init__(self, resp):
        self.data        = resp.content
        self.status_code = resp.status_code
        self._response   = resp