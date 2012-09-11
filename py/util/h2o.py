import asyncproc as async
import urllib2 as url
import time, os, json

class H2O:
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __get(self, loc):
        req = url.Request(self.__url(loc))
        return url.urlopen(req).read()

    def get_cloud(self):
        return json.loads(self.__get('Cloud.json'))

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
        except url.HTTPError:
            raise
        except url.URLError, e:
            if e.reason[0] == 61:
                return False
            raise

    def __init__(self, port):
        self.port = port;
        self.addr = 'localhost'
        self.proc = async.Process(["java", "-ea", "-jar", "../build/h2o.jar", "--port=%d"%port])

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
