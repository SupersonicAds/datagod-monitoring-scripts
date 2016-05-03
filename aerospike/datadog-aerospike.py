#!/usr/bin/python

###############
#
# @name: Aerospike monitoring in datadog (using statsd)
# @version: 2016/05/03
# @author: andris jegorov
# @email: andris@ironsrc.com
# @description: Aerospike monitoring in datadog script
# @licence: MIT
# @copyright (c) 2016 IronSource Mobile
#
###############

try:
    from datadog import initialize, statsd
except ImportError:
    print '{E} python module "datadog" is not present in system. Please install using "pip install datadog".'
    raise
try:
    import aerospike
    from aerospike.exception import AerospikeError
except ImportError:
    print '{E} python module "aerospike" is not present in system. Please install using "pip install aerospike".'
    raise
try:
    import yaml
except ImportError:
    print '{E} python module "yaml" is not present in system. Please install using "pip install yaml".'
    raise
import os.path
import sys
import exceptions

class asConfig:
    config = {}

    def __init__(self):
        self.hostname = os.uname()[1]
        self.configfile = os.path.splitext(os.path.basename(__file__))[0]
        # read and parse configuration
        with open(self.configfile+'.yaml', mode='r') as stream:
            try:
                self.config = yaml.load(stream)
                stream.close()
            except yaml.YAMLError as err:
                print '[E] config problem: {0}'.format(err)
                raise

class asEvents:
    def __init__(self, config):
        self.events = {}
        self.config = config.config
        self.configfile = config.configfile
        if self.config['datadog']['defaults']['events']['enabled']:
            if not os.path.isfile(self.configfile+'.events'):
                try:
                    open(self.configfile+'.events', 'a').close()
                except IOError as e:
                    print '[E] events file create problem: {0}'.format(e)
                    raise

            with open(self.configfile+'.events', mode='r') as stream:
                try:
                    self.events = yaml.safe_load(stream)
                except (IOError, yaml.YAMLError) as e:
                    pass
                stream.close()

    def isevent(self, key, value, cluster=False, service=False):
        #print {key, value, cluster, service, self.events}
        isevent = False
        if self.events == None:
            self.events = {}
        if not cluster in dict(self.events):
            self.events[cluster] = {}
        if not service in self.events[cluster]:
            self.events[cluster][service] = {}
        if not key in self.events[cluster][service]:
            self.events[cluster][service][key] = value
        elif self.events[cluster][service][key] != value:
            isevent = True
            self.events[cluster][service][key] = value
        return isevent

    def finalize(self):
        if self.config['datadog']['defaults']['events']['enabled']:
            if os.path.isfile(self.configfile+'.events'):
                try:
                    with open(self.configfile+'.events', mode='w') as stream:
                        yaml.safe_dump(self.events, stream=stream)
                        stream.close()
                except IOError as e:
                    print '[E] events file create problem: {0}'.format(e)
                    raise

class asDataDog:
    clustername = None

    def __init__(self, config):
        self.config = config.config
        self.configfile = config.configfile
        self.hostname = config.hostname
        initialize(self.config['datadog']['host'], self.config['datadog']['port'])
        self.events = asEvents(config)

    def send(self, sensor, command, key, value, onlynode=False, nodehost=False):
        datatype = 'string'
        cnf = self.config['datadog']
        mode = cnf['defaults']['number']

        if value.isdigit(): # digital
            datatype = 'number'
        elif value.lower() in ('true', 'on', 'enable', 'enabled'): # boolean : true
            if cnf['defaults']['boolean']:
                value = 'true'
                datatype = 'bool'
            else:
                value = 1
                datatype = 'number'
        elif value.lower() in ('false', 'off', 'disable', 'disabled'): # boolean : false
            if cnf['defaults']['boolean']:
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

        tags = ["nodename:{0}".format(self.hostname), "cluster:{0}".format(self.clustername), "group:{0}".format(command), "set:{0}".format("node" if onlynode else "cluster")]
        ###print tags
        if self.config['debug']:
            print "{0}={2} type is {1} :: {3}".format(sensor, datatype, value, mode)
        if datatype == 'number':
            if mode == 'histogram':
                statsd.histogram(metric=sensor, value=value, tags=tags)
            elif mode == 'gauge':
                statsd.gauge(metric=sensor, value=value, tags=tags)
            elif mode == 'set':
                statsd.set(metric=sensor, value=value, tags=tags)
            elif mode == 'counter':
                statsd.increment(metric=sensor, value=value, tags=tags)
        elif cnf['defaults']['events']['enabled']: # strings boolean and unknown types, as events, if enabled
            cmd = command.split(':')[0].split('/')[0]
            if cnf['defaults']['events']['filtered'] == False or cmd in cnf['event']['command'] or key in cnf['event']['key']:
                import socket
                if self.events.isevent(key=sensor, value=value, cluster=self.clustername, service=self.hostname):
                    statsd.event(title=sensor, text=value, tags=tags, hostname=socket.gethostname())

    def finalize(self):
        self.events.finalize()

class asAeroSpike:
    clustername = None
    service = None

    def __init__(self, config):
        self.config = config.config
        self.configfile = config.configfile
        self.datadog = asDataDog(config)
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
            self.datadog.send("aerospike.clustername.up", 'clustername', 'up', self.isInit, True)

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
                self.datadog.send("aerospike.{0}.{1}.time".format(cmd, nskey), command=command, key=nskey, value=nstval[0], onlynode=onlynode) # time
                self.datadog.send("aerospike.{0}.{1}.opssec".format(cmd, nskey), command=command, key=nskey, value=nstval[1], onlynode=onlynode) # ops/sec
                self.datadog.send("aerospike.{0}.{1}.more1ms".format(cmd, nskey), command=command, key=nskey, value=nstval[2], onlynode=onlynode) # >1ms
                self.datadog.send("aerospike.{0}.{1}.more8ms".format(cmd, nskey), command=command, key=nskey, value=nstval[3], onlynode=onlynode) # >8ms
                self.datadog.send("aerospike.{0}.{1}.more64ms".format(cmd, nskey), command=command, key=nskey, value=nstval[4], onlynode=onlynode) # >64ms
                continue
            else:
                for nsdataval in nstval:
                    ddict = nsdataval.split('=', 2)
                    if len(ddict) < 2:
                        (dkey, dval) = (ddict[0], ddict[0])
                    else:
                        (dkey, dval) = ddict
                        if ns:
                            self.datadog.send("aerospike.{0}.{1}.{2}".format(cmd, nskey, dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                        elif sets:
                            if command == 'sets':
                                if dkey in ('ns_name', 'set_name'):
                                    nsval[dkey] = dval
                                elif not nodatadog:
                                    self.datadog.send("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns_name'], nsval['set_name'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                            elif command == 'sindex':
                                if dkey in ('ns', 'set', 'indexname'):
                                    nsval[dkey] = dval
                                elif not nodatadog:
                                    self.datadog.send("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns'], nsval['set'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode)
                        elif not nodatadog:
                            self.datadog.send("aerospike.{0}.{1}".format(cmd, dkey), command=command, key=dkey, value=dval, onlynode=onlynode)

    def finalize(self):
        if self.asclient != None:
            self.asclient.close()
        self.datadog.finalize()

class asMonitoring:
    aerospike = None

    def __init__(self):
        self.config = asConfig()

    def execute(self):
        self.aerospike = asAeroSpike(self.config)
        config = self.config.config
        if config['datadog']['defaults']['cluster']['check']:
            if config['datadog']['defaults']['cluster']['single']:
                # check only from arbitrator instance
                # @ToDo will be implemented later
                self.aerospike.process(onlynode=False)
            else:
                # check from all instances from cluster
                self.aerospike.process(onlynode=False)
        if config['datadog']['defaults']['instance']['check']:
            if config['datadog']['defaults']['instance']['all']:
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
