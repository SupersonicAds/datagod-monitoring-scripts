#!/usr/bin/python

###############
#
# @name: Aerospike monitoring in datadog (using statsd)
# @version: 2016/04/26
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

configfile = os.path.splitext(os.path.basename(__file__))[0]

# read and parse configuration
with open(configfile+'.yaml', mode='r') as stream:
    try:
        config = yaml.load(stream)
        stream.close()
    except yaml.YAMLError as err:
        print '[E] config problem: {0}'.format(err)
        raise
events = {}
if config['datadog']['defaults']['events']['enabled']:
    if not os.path.isfile(configfile+'.events'):
        try:
            open(configfile+'.events', 'a').close()
        except IOError as e:
            print '[E] events file create problem: {0}'.format(e)
            raise

    with open(configfile+'.events', mode='r') as stream:
        try:
            events = yaml.safe_load(stream)
            if events == None:
                events = {}
        except IOError as e:
            events = {}
        except yaml.YAMLError:
            events = {}
        stream.close()

nodename = os.uname()[1]
clustername = None

def asevent(key, value, cluster=False, service=False):
    print {key, value, cluster, service, events}
    isevent = False
    if not cluster in events:
        events[cluster] = {}
    if not service in events[cluster]:
        events[cluster][service] = {}
    if not key in events[cluster][service]:
        events[cluster][service][key] = value
    elif events[cluster][service][key] != value:
        isevent = True
        events[cluster][service][key] = value
    return isevent

def asdatadog(sensor, command, key, value, onlynode=False, nodehost=False):
    datatype = 'string'
    cnf = config['datadog']
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

    tags = ["nodename:{0}".format(nodename), "cluster:{0}".format(clustername), "group:{0}".format(command), "set:{0}".format("node" if onlynode else "cluster")]
    ###print tags
    if config['debug']:
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
            if asevent(key=sensor, value=value, cluster=clustername, service=nodehost):
                statsd.event(title=sensor, text=value, tags=tags, hostname=socket.gethostname())

def asmetrics(command, ns=False, single=False, sets=False, nodatadog=False, onlynode=False, latency=False, nodehost='127.0.0.1:3000'):
    cmd = command.replace('/', '.').replace(':', '')
    retdata = []
    asresp = asclient.info_node(command, (nodehost.split(':')[0], nodehost.split(':')[1])) if onlynode else asclient.info(command)
    if onlynode:
        asresp = {clustername: (None, asresp.split('\t')[1].strip()+'\n')}
    if clustername == None:
        nsdata = asresp.values()[0][1].replace("\n", '').split(';')
    else:
        nsdata = asresp[clustername][1].replace("\n", '').split(';')

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
            asdatadog("aerospike.{0}.{1}.time".format(cmd, nskey), command=command, key=nskey, value=nstval[0], onlynode=onlynode, nodehost=nodehost) # time
            asdatadog("aerospike.{0}.{1}.opssec".format(cmd, nskey), command=command, key=nskey, value=nstval[1], onlynode=onlynode, nodehost=nodehost) # ops/sec
            asdatadog("aerospike.{0}.{1}.more1ms".format(cmd, nskey), command=command, key=nskey, value=nstval[2], onlynode=onlynode, nodehost=nodehost) # >1ms
            asdatadog("aerospike.{0}.{1}.more8ms".format(cmd, nskey), command=command, key=nskey, value=nstval[3], onlynode=onlynode, nodehost=nodehost) # >8ms
            asdatadog("aerospike.{0}.{1}.more64ms".format(cmd, nskey), command=command, key=nskey, value=nstval[4], onlynode=onlynode, nodehost=nodehost) # >64ms
            continue
        else:
            for nsdataval in nstval:
		ddict = nsdataval.split('=', 2)
		if len(ddict)<2:
			( dkey, dval ) = ( ddict[0], ddict[0] )
		else:
			(dkey, dval) = ddict
                if ns:
                    asdatadog("aerospike.{0}.{1}.{2}".format(cmd, nskey, dkey), command=command, key=dkey, value=dval, onlynode=onlynode, nodehost=nodehost)
                elif sets:
                    if command == 'sets':
                        if dkey in ('ns_name', 'set_name'):
                            nsval[dkey] = dval
                        elif not nodatadog:
                            asdatadog("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns_name'], nsval['set_name'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode, nodehost=nodehost)
                    elif command == 'sindex':
                        if dkey in ('ns', 'set', 'indexname'):
                            nsval[dkey] = dval
                        elif not nodatadog:
                            asdatadog("aerospike.{0}.{1}.{2}.{3}".format(cmd, nsval['ns'], nsval['set'], dkey), command=command, key=dkey, value=dval, onlynode=onlynode, nodehost=nodehost)
                elif not nodatadog:
                    asdatadog("aerospike.{0}.{1}".format(cmd, dkey), command=command, key=dkey, value=dval, onlynode=onlynode, nodehost=nodehost)
        retdata.append(nsval)
    return retdata

def asgetdata(clustername=False, onlynode=False, service=False):
    ## instance ( node ) statistics
    asmetrics('bins', ns=True, onlynode=True, nodehost=service)
    #asmetrics('get-config', onlynode=True, nodehost=service)
    namespaces = asmetrics('namespaces', ns=True, single=True, onlynode=True, nodehost=service)
    for ns in namespaces:
        asmetrics('namespace/{0}'.format(ns), onlynode=True, nodehost=service)
    asmetrics('sets', sets=True, onlynode=True, nodehost=service)
    sindex = asmetrics('sindex', sets=True, nodatadog=True, onlynode=True, nodehost=service)
    for sidx in sindex:
        asmetrics('sindex/{0}/{1}'.format(sidx['ns'], sidx['indexname']), onlynode=True, nodehost=service)
    asmetrics('statistics', onlynode=True, nodehost=service)
    asmetrics('latency:', latency=True, onlynode=True, nodehost=service)

try:
    # connect to datadog statsd
    initialize(config['datadog']['host'], config['datadog']['port'])
    try:
        # connect to aerospike cluster
        asclient = aerospike.client({'hosts': list((k, v) for (k, v) in config['aerospike']['hosts'])}).connect()
    except AerospikeError:
        asdatadog("aerospike.clustername.up", 'clustername', 'up', 'false', True)
        sys.exit()
    # read current cluster name
    clustername = asmetrics('node', single=True)[0]

    asdatadog("aerospike.clustername.up", 'clustername', 'up', 'true', True)
    service = asmetrics('service', single=True)[0]

    if config['datadog']['defaults']['cluster']['check']:
        if config['datadog']['defaults']['cluster']['single']:
            # check only from arbitrator instance
            asgetdata(clustername=clustername, onlynode=False, service=service)
        else:
            # check from all instances from cluster
            asgetdata(clustername=clustername, onlynode=False, service=service)
    if config['datadog']['defaults']['instance']['check']:
        if config['datadog']['defaults']['instance']['all']:
            # detect and in loop check all instances from cluster
            # @ToDo will be implemented later
            asgetdata(clustername=clustername, onlynode=True, service=service)
        else:
            # check only current instance
            asgetdata(clustername=clustername, onlynode=True, service=service)

    asclient.close()

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
    if config['datadog']['defaults']['events']['enabled']:
        print events
        if not os.path.isfile(configfile+'.events'):
            try:
                with open(configfile+'.events', mode='w') as stream:
                    yaml.safe_dump(events, stream=stream)
                    stream.close()
            except IOError as e:
                print '[E] events file create problem: {0}'.format(e)
                raise
