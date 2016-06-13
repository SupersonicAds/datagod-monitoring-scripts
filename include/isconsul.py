import json
import urllib2

class isconsul:
    lastcode = 0
    lasterror = ""

    def __init__(self, config):
        self.config = config.config
        self.consul_url = "http://{0}:{1}".format(self.config["consul"]["host"], self.config["consul"]["port"])

    def call(self, resource, method='get'):
        self.lastcode = 0
        self.lasterror = ""
        data = "[]"
        if method == 'get':
            try:
                data = urllib2.urlopen("{0}{1}".format(self.consul_url, resource), timeout=int(self.config["consul"]["timeout"]))
            except urllib2.HTTPError, e:
                self.lastcode = e.code
                self.lasterror = e.msg
                return False
        result = json.load(data)
        return result
