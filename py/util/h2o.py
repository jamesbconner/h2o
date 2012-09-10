from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2
import time, os, json
import asyncproc

register_openers()

class H2O:
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __get(self, loc):
        req = urllib2.Request(self.__url(loc))
        return urllib2.urlopen(req).read()

    def __read(self, req):
        return json.loads(urllib2.urlopen(req).read())

    def get_cloud(self):
        req = urllib2.Request(self.__url('Cloud.json'))
        return self.__read(req)

    def put_file(self, f):
        datagen, headers = multipart_encode({"File": open(f, 'rb')})
        req = urllib2.Request(self.__url('PutFile.json'), datagen, headers)
        return self.__read(req)

    def stabilize(self, msg, timeout, func):
        start = time.clock()
        while time.clock() - start < timeout:
            if func(self):
                break
        else:
            raise Exception('Timeout waiting for condition: ' + msg)

    def __is_alive(self, s2):
        assert self == s2
        try:
            self.get_cloud()
            return True
        except urllib2.HTTPError:
            raise
        except urllib2.URLError, e:
            if e.reason[0] == 61:
                return False
            raise

    def __init__(self, port):
        self.port = port;
        self.addr = 'localhost'
        self.proc = asyncproc.Process(["java", "-ea", "-jar", "../build/h2o.jar", "--port=%d"%port])

        try:
            self.stabilize('h2o started', 2, self.__is_alive)
        except:
            self.proc.terminate()
            raise

        if self.proc.wait(os.WNOHANG) is not None:
            raise Exception('Failed to launch with exit code: ' + self.proc.wait())

    def read(self):
        self.proc.read()

    def terminate(self):
        self.proc.terminate()
