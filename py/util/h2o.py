import requests
import time, os, json
import asyncproc

class H2O:
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __check(self, r):
        if not r:
            import inspect
            raise Exception('Error in %s: %s' % (inspect.stack()[1][3], str(r)))
        return r.json

    def get_cloud(self):
        return self.__check(requests.get(self.__url('Cloud.json')))

    def put_file(self, f):
        return self.__check(requests.post(self.__url('PutFile.json'), 
            files={"File": open(f, 'rb')}))

    def parse(self, key):
        return self.__check(requests.get(self.__url('Parse.json'),
            params={"Key": key}))

    def random_forest(self, key):
        return self.__check(requests.get(self.__url('RF.json'),
            params={"Key": key}))

    def random_forest_view(self, key):
        return self.__check(requests.get(self.__url('RFView.json'),
            params={"Key": key}))

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
        except requests.ConnectionError, e:
            if e.args[0].errno == 61 or e.args[0].errno == 111:
                return False
            raise

    def __init__(self, addr, port):
        self.port = port;
        self.addr = addr
        self.proc = asyncproc.Process(["java", "-ea", "-jar", "../build/h2o.jar",
                "--port=%d"%self.port,
                '--ip=%s'%self.addr,
                '--nosigar',
        ])

        try:
            self.stabilize('h2o started', 2, self.__is_alive)
        except:
            self.proc.terminate()
            raise

        if self.wait() is not None:
            raise Exception('Failed to launch with exit code: ' + self.proc.wait())

    def read(self):
        return self.proc.read()
    
    def wait(self):
        return self.proc.wait(os.WNOHANG)

    def terminate(self):
        return self.proc.terminate()
