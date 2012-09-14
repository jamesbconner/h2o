import requests
import time, os, json, signal
import asyncproc
import psutil

class H2O:
    def __url(self, loc):
        return 'http://%s:%d/%s' % (self.addr, self.port, loc)

    def __check(self, r):
        asyncproc.log_command('Sent ' + r.url)
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

    def random_forest(self, key, ntrees):
        return self.__check(requests.get(self.__url('RF.json'),
            params={
                "ntrees": ntrees,
                "Key": key
                }))

    def random_forest_view(self, key):
        return self.__check(requests.get(self.__url('RFView.json'),
            params={"Key": key}))

    def stabilize(self, msg, timeout, func):
        start = time.time()
        while time.time() - start < timeout:
            if func(self):
                break
            time.sleep(0.1)
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

    def __init__(self, addr, port, spawn=True):
        self.port = port;
        self.addr = addr
        if not spawn:
            self.stabilize('h2o started', 2, self.__is_alive)
        else:
            self.proc = asyncproc.Process(["java", "-ea", "-jar", "../build/h2o.jar",
                    "--port=%d"%self.port,
                    '--ip=%s'%self.addr,
                    '--nosigar',
            ])
            self.psutil = psutil.Process(self.proc.pid())
            try:
                self.stabilize('h2o started', 2, self.__is_alive)
            except:
                self.psutil.kill()
                raise

            while self.wait() is None:
                if self.read().find('HTTP listening') != -1: break
            if self.wait() is not None:
                raise Exception('Failed to launch with exit code: %d' % self.proc.wait())

    def read(self):
        return self.proc.read()

    def stack_dump(self):
        self.proc.kill(signal.SIGQUIT)
    
    def wait(self):
        if self.psutil.is_running(): return None
        return self.psutil.wait()

    def terminate(self):
        return self.psutil.kill()
