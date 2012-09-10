import asyncproc as async
import urllib2 as url

class H2O:
    def url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __init__(self, port):
        self.port = port;
        self.addr = 'localhost'
        self.proc = async.Process(["java", "-ea", "-jar", "build/h2o.jar", "--port=%d"%port])

        r = url.Request(self.url('Cloud.json'))
        while True:
            try:
                url.urlopen(r).read()
                break
            except url.HTTPError:
                self.proc.terminate()
                raise
            except url.URLError, e:
                if e.reason[0] == 61:
                    continue
                self.proc.terminate()
                raise

    def read(self):
        self.proc.read()
