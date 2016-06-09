#!/usr/bin/python

###############
#
# @name: Consul cluster monitoring in datadog (using statsd)
# @version: 2016/06/05
# @author: andris jegorov
# @email: andris@ironsrc.com
# @description: Aerospike monitoring in datadog script
# @licence: MIT
# @copyright (c) 2016 IronSource Mobile
#
###############

import os, exceptions, sys
sys.path.append(os.path.abspath('../include'))

try:
    from isconfig import isconfig
except ImportError:
    print '{E} python module "isconfig" is not present in system.'
    raise

try:
    from isdatadog import isdatadog
except ImportError:
    print '{E} python module "isdatadog" is not present in system.'
    raise

# try:
#     import consul
# except ImportError:
#     print '{E} python module "python-consul" is not present in system. Please install using "pip install python-consul".'
#     raise
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

class cMonitoring:
    consul = None

    def __init__(self):
        self.config = isconfig(os.path.splitext(os.path.basename(__file__))[0])
        self.consul = isconsul(self.config)
        self.datadog = isdatadog(self.config)

    def execute(self):
        # debug : BEGIN
        # result = []
        # debug : END
        config = self.config.config['checks']
        # check master in cluster
        try:
            leader = self.consul.call('/v1/status/leader?stale')
            if leader != False:
                self.datadog.gauge('consule.cluter.up', 'on')
            else:
                self.datadog.gauge('consule.cluter.up', 'off')
                self.datadog.event('consule.cluter.up', 'Got HTTP error [{0}]:{1}'.format(self.consul.lastcode, self.consul.lasterror), tags=['critical'])
                sys.exit()
        except urllib2.URLError, e:
            self.datadog.gauge('consule.cluter.up', 'off')
            self.datadog.event('consule.cluter.up', 'Got URL error: {0}'.format(e.reason), tags=['critical'])
            sys.exit()
        # check peers count in cluster
        peers = self.consul.call('/v1/status/peers?stale')
        # check peers count crit, warn
        self.datadog.gauge('consule.status.peers', len(peers))
        if len(peers) <= config['peers']['critical']:
            self.datadog.event('consule.status.peers', 'Found critical peers count {0}.'.format(len(peers)), tags=['critical'])
        elif len(peers) <= config['peers']['warining']:
            self.datadog.event('consule.status.peers', 'Found warning peers count {0}.'.format(len(peers)), tags=['warning'])
        # check peers change diff
        if self.datadog.isevent('consule.status.peers', len(peers)):
            self.datadog.event('consule.status.peers', 'Peer count changes, new count {0}.'.format(len(peers)), tags=['warning'])
        # check if leader present
        if leader == None or leader == '' or leader == False:
            self.datadog.event('consule.status.leader', 'Leader not available.', tags=['critical'])
        # check if leader in peer list
        if leader not in peers:
            self.datadog.event('consule.status.leader', 'Leader "{0}" is not present in peers list "{1}".'.format(leader, peers), tags=['warning'])
        # check if leader changed
        if self.datadog.isevent('consule.status.leader', leader):
            self.datadog.event('consule.status.leader', 'Leader "{0}" changed.'.format(leader), tags=['warning'])
        # give datacenters list
        datacenters = self.consul.call('/v1/catalog/datacenters?stale')
        for dc in datacenters:
            # check services status in cluster
            if config['services']['check']:
                tag = ','.join(config['services']['tags']) if config['services']['tags'] != None else ""
                services = self.consul.call('/v1/catalog/services?stale&dc={0}'.format(dc))
                # result.append(services)
                for service in services:
                    data = self.consul.call('/v1/health/service/{0}?stale&dc={1}&tag={2}'.format(service, dc, tag))
                    # result.append(data)
                    if len(data) > 0:
                        # result.append(data)
                        for check in data[0]['Checks']:
                            if check['Status'] != 'passing':
                                self.datadog.event('consule.service.{0}.{1}.{2}'.format(check['Node'], service, check['CheckID']), 'On node {0}, service "{1}" check {2} on {3} state: {4}. '.format(check['Node'], service, check['Name'], check['Status'], check['Output']), tags=['critical'])

            # check nodes status in cluster
            if config['nodes']['check']:
                nodes = self.consul.call('/v1/catalog/nodes?stale')
                for node in nodes:
                    data = self.consul.call('/v1/health/node/{0}?stale'.format(node["Node"]))[0]
                    if data['Status'] != 'passing':
                        self.datadog.event('consule.node.{0}'.format(node["Node"]), 'Node "{0}" status is {1}. '.format(node["Node"], data['Status']), tags=['critical'])
        # infrastructure spetsiific checks
        # check environment templates
        for env in config['services']['tags']:
            envkeys = self.consul.call('/v1/kv/consul-template/{0}/?stale'.format(env))
            if envkeys == False or envkeys == None or envkeys == '':
                self.datadog.event('consule.kv.{0}.consul-template'.format(env), 'Environment {0} does not exists in consul KV.'.format(env), tags=['critical'])

        # debug : BEGIN
        # for data in result:
        #     print data
        # debug : END

    def finalize(self):
        pass

monitoring = cMonitoring()
try:
    monitoring.execute()

except IOError as error:
    print 'IO Error {0} [{1}]'.format(error.errno, error.strerror)
    raise
except ValueError:
    print 'Value error'
    raise
except exceptions.SystemExit:
    sys.exit()
except:
    print 'Unexpected error: ', sys.exc_info()[0]
    raise
finally:
    monitoring.finalize()
