#!/usr/bin/python

###############
#
# @name: Aerospike monitoring in datadog (using statsd)
# @version: 2016/06/05
# @author: andris jegorov
# @email: andris@ironsrc.com
# @description: Aerospike monitoring in datadog script
# @licence: MIT
# @copyright (c) 2016 IronSource Mobile
#
###############

import os.path
import sys
import exceptions
sys.path.append(os.path.abspath('../include'))

try:
    import aerospike
    from aerospike.exception import AerospikeError
except ImportError:
    print '{E} python module "aerospike" is not present in system. Please install using "pip install aerospike".'
    raise

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

class asAeroSpike:
    clustername = None
    service = None

    def __init__(self, config):
        self.config = config.config
        self.configfile = config.configfile
        self.datadog = isdatadog(config)
        try:
            # connect to aerospike cluster
            self.asclient = aerospike.client({'hosts': list((k, v) for (k, v) in self.config['aerospike']['hosts'])}).connect()
            self.isInit = 'true'
            self.clustername = self.metric('node', single=True)[0]
            self.service = self.metric('service', single=True)[0]
            self.datadog.clustername = self.clustername
        except AerospikeError:
            self.isInit = 'false'
            sys.exit()
        finally:
            self.send("aerospike.clustername.up", 'clustername', 'up', self.isInit, True)

    def process(self, onlynode=False):
        ## instance ( node ) statistics
        self.metric('bins', ns=True, onlynode=True)
        #asmetrics('get-config', onlynode=True)
        namespaces = self.metric('namespaces', ns=True, single=True, onlynode=True)
        if namespaces != None:
            for ns in namespaces:
                self.metric('namespace/{0}'.format(ns), onlynode=True)
        self.metric('sets', sets=True, onlynode=True)
        sindex = self.metric('sindex', sets=True, nodatadog=True, onlynode=True)
        if sindex != None:
            for sidx in sindex:
                self.metric('sindex/{0}/{1}'.format(sidx['ns'], sidx['indexname']), onlynode=True)
        self.metric('statistics', onlynode=True)
        self.metric('latency:', latency=True, onlynode=True)

    def metric(self, command, ns=False, single=False, sets=False, nodatadog=False, onlynode=False, latency=False):
        cmd = command.replace('/', '.').replace(':', '')
        asresp = self.asclient.info_node(command, (self.service.split(':')[0], self.service.split(':')[1])) if onlynode else self.asclient.info(command)
        if onlynode:
            asresp = {self.clustername: (None, asresp.split('\t')[1].strip()+'\n')}
        if self.clustername == None:
            nsdata = asresp.values()[0][1].replace("\n", '').split(';')
        else:
            nsdata = asresp[self.clustername][1].replace("\n", '').split(';')

        if single:
            return nsdata
        nskey = ''
        for val in nsdata:
            nsval = {}
            if val in ("\n", ''):
                continue
            if ns:
                (nskey, nstval) = val.split(':')
                nstval = nstval.split(',')
            elif sets:
                nskey = ''
                nstval = val.split(':')
            elif latency:
                nstval = val.split(',')
                nstkey = nstval[0].split(':', 1)[0]
                if nstkey in ('reads', 'writes_master', 'proxy', 'udf', 'query'):
                    nskey = nstkey
                    continue
            else:
                nskey = ''
                nstval = [val]
            if latency:
                self.send("aerospike.{0}.{1}.time".format(cmd, nskey), command=command, key=nskey, value=nstval[0], onlynode=onlynode) # time
                self.send("aerospike.{0}.{1}.opssec".format(cmd, nskey), command=command, key=nskey, value=nstval[1], onlynode=onlynode) # ops/sec
                self.send("aerospike.{0}.{1}.more1ms".format(cmd, nskey), command=command, key=nskey, value=nstval[2], onlynode=onlynode) # >1ms
                self.send("aerospike.{0}.{1}.more8ms".format(cmd, nskey), command=command, key=nskey, value=nstval[3], onlynode=onlynode) # >8ms
                self.send("aerospike.{0}.{1}.more64ms".format(cmd, nskey), command=command, key=nskey, value=nstval[4], onlynode=onlynode) # >64ms
                continue
            else:
                for nsdataval in nstval:
                    ddict = nsdataval.split('=', 2)
                    if len(ddict) < 2:
                        (dkey, dval) = (ddict[0], ddict[0])
                    else:
                        (dkey, dval) = ddict
                        if ns:
                            self.send("aerospike.{0}.{1}.{2}".format(cmd, nskey, dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                        elif sets:
                            if command == 'sets':
                                if dkey in ('ns_name', 'set_name'):
                                    nsval[dkey] = dval
                                elif not nodatadog:
                                    self.send("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns_name'], nsval['set_name'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                            elif command == 'sindex':
                                if dkey in ('ns', 'set', 'indexname'):
                                    nsval[dkey] = dval
                                elif not nodatadog:
                                    self.send("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns'], nsval['set'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                        elif not nodatadog:
                            self.send("aerospike.{0}.{1}".format(cmd, dkey), command=command, key=dkey, value=dval, onlynode=onlynode)

    def send(self, metric, command, key, value, onlynode=False, nodehost=False, tags=False):
        datatype = 'string'
        cnf = self.config['checks']
        mode = cnf['defaults']['number']

        if value.isdigit(): # digital
            datatype = 'number'
        elif value.lower() in ('true', 'on', 'enable', 'enabled'): # boolean : true
            if cnf['defaults']['boolean']['enabled']:
                value = 'true'
                datatype = 'bool'
            else:
                value = 1
                datatype = 'number'
        elif value.lower() in ('false', 'off', 'disable', 'disabled'): # boolean : false
            if cnf['defaults']['boolean']['enabled']:
                value = 'false'
                datatype = 'bool'
            else:
                value = 0
                datatype = 'number'
        else:
            try:
                float(value)
                datatype = 'number'
            except ValueError:
                mode = 'event'
        if datatype == 'number':
            cmd = command.split(':')[0].split('/')[0]
            if cmd in cnf['histogram']['command'] or key in cnf['histogram']['key']:
                mode = 'histogram'
            elif cmd in cnf['gauge']['command'] or key in cnf['gauge']['key']:
                mode = 'gauge'
            elif cmd in cnf['set']['command'] or key in cnf['set']['key']:
                mode = 'set'
            elif cmd in cnf['counter']['command'] or key in cnf['counter']['key']:
                mode = 'counter'

        if tags == False:
            tags = ["nodename:{0}".format(self.datadog.hostname), "cluster:{0}".format(self.clustername), "group:{0}".format(command), "set:{0}".format("node" if onlynode else "cluster")]

        if datatype == 'number':
            self.datadog.number(metric=metric, value=value, tags=tags, mode=mode)
        elif cnf['defaults']['events']['enabled']: # strings boolean and unknown types, as events, if enabled
            cmd = command.split(':')[0].split('/')[0]
            if cnf['defaults']['events']['filtered'] == False or cmd in cnf['event']['command'] or key in cnf['event']['key']:
                if self.datadog.isevent(key=metric, value=value, cluster=self.clustername, service=self.datadog.hostname):
                    self.datadog.event(metric=metric, value=value, tags=tags)

    def finalize(self):
        if self.asclient != None:
            self.asclient.close()
        self.datadog.finalize()

class asMonitoring:
    aerospike = None

    def __init__(self):
        self.config = isconfig(os.path.splitext(os.path.basename(__file__))[0])

    def execute(self):
        self.aerospike = asAeroSpike(self.config)
        config = self.config.config
        if config['checks']['defaults']['cluster']['check']:
            if config['checks']['defaults']['cluster']['single']:
                # check only from arbitrator instance
                # @ToDo will be implemented later
                self.aerospike.process(onlynode=False)
            else:
                # check from all instances from cluster
                self.aerospike.process(onlynode=False)
        if config['checks']['defaults']['instance']['check']:
            if config['checks']['defaults']['instance']['all']:
                # detect and in loop check all instances from cluster
                # @ToDo will be implemented later
                self.aerospike.process(onlynode=True)
            else:
                # check only current instance
                self.aerospike.process(onlynode=True)

    def finalize(self):
        if self.aerospike != None:
            self.aerospike.finalize()

monitoring = asMonitoring()
try:
    monitoring.execute()

except IOError as error:
    print 'IO Error {0} [{1}]'.format(error.errno, error.strerror)
    raise
except ValueError:
    print 'Value error'
    raise
except AerospikeError as error:
    print 'Aerospike error: {0} [{1}]'.format(error.msg, error.code)
    raise
except exceptions.SystemExit:
    sys.exit()
except:
    print 'Unexpected error: ', sys.exc_info()[0]
    raise
finally:
    monitoring.finalize()
