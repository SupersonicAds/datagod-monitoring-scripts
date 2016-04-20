###############
#
# @name: Aerospike monitoring in datadog
# @version: 2016/04/20
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

node = None

# read and parse configuration
with open( os.path.splitext( os.path.basename(__file__) )[0]+'.yaml', 'r' ) as stream:
    try:
        config = yaml.load(stream)
    except yaml.YAMLError as err:
        print '{E} config problem, '+err
        raise
isDebug = config['debug']

def asdatadog(sensor, command, key, value, onlynode=False):
    datatype = 'string'
    cnf = config['datadog']
    mode = cnf['defaults']['number']

    if value.isdigit(): # digital
        datatype = 'number'
    elif value.lower() in ('true','on','enable','enabled'): # boolean : true
        if cnf['defaults']['boolean']:
            value = 'true'
            datatype = 'bool'
        else:
            value = 1
            datatype = 'number'
    elif value.lower() in ('false','off','disable','disabled'): # boolean : false
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
        except:
            mode = 'event'
            pass
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
    

    tags = ["host:{0}".format(node), "group:{0}".format(command), "set:{0}".format("node" if onlynode else "cluster")]
    if isDebug: print "{0}={2} type is {1} :: {3}".format(sensor, datatype, value, mode)
    ##print tags
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
        ##print '+ [{0}] :: {1}'.format(cmd, key)
        if cnf['defaults']['events']['filtered'] == False or cmd in cnf['event']['command'] or key in cnf['event']['key']:
            import socket
            statsd.event(title=sensor, text=value, tags=tags, hostname=socket.gethostname())
        pass

def asmetrics(command, ns=False, single=False, sets=False, nodatadog=False, onlynode=False, latency=False, nodehost='127.0.0.1:3000'):
    cmd = command.replace('/','.').replace(':','')
    retdata = []
    asresp   = asclient.info_node( command, ( nodehost.split(':')[0], nodehost.split(':')[1] ) ) if onlynode else asclient.info(command)
    if onlynode:
        asresp = {node: ( None, asresp.split('\t')[1].strip()+'\n' )}
    ##print asresp
    ##print type( asresp )
    if node == None:
        nsdata = asresp.values()[0][1].replace("\n",'').split(';')
    else:
        nsdata = asresp[node][1].replace("\n",'').split(';')
    ### DEBUG ONLY STATIC DATA : BEGIN
    if isDebug:
        if command == 'sets':
            nsdata = "ns_name=test:set_name=test-set:n_objects=679707:n-bytes-memory=0:set-enable-xdr=use-default:disable-eviction=false:set-delete=false;ns_name=test:set_name=another-set:n_objects=3245:n-bytes-memory=0:set-enable-xdr=use-default:disable-eviction=false:set-delete=false;ns_name=bar:set_name=bar-set:n_objects=12377:n-bytes-memory=78656543:set-enable-xdr=use-default:disable-eviction=false:set-delete=false;".split(';')
        elif command == 'sindex':
            nsdata = "ns=lifetimeAggregations:set=campaignOutcomeLifetimeCounters:indexname=campaignLifetimeRV:num_bins=1:bins=CL_RV:type=TEXT:indextype=NONE:path=CL_RV:sync_state=synced:state=RW;ns=lifetimeAggregations:set=campaignOutcomeLifetimeCounters:indexname=campaignLifetimeIS:num_bins=1:bins=CL_IS:type=TEXT:indextype=NONE:path=CL_IS:sync_state=synced:state=RW;ns=lifetimeAggregations:set=campaignOutcomeLifetimeCounters:indexname=campaignLifetimeOW:num_bins=1:bins=CL_OW:type=TEXT:indextype=NONE:path=CL_OW:sync_state=synced:state=RW;".split(';')
        elif command == 'sindex/lifetimeAggregations/campaignLifetimeRV':
            nsdata = "keys=0;objects=0;ibtr_memory_used=18688;nbtr_memory_used=0;si_accounted_memory=18688;load_pct=100;loadtime=0;stat_write_reqs=0;stat_write_success=0;stat_write_errs=0;stat_delete_reqs=0;stat_delete_success=0;stat_delete_errs=0;stat_gc_recs=0;stat_gc_time=0;query_reqs=0;query_avg_rec_count=0;query_avg_record_size=0;query_agg=0;query_agg_avg_rec_count=0;query_agg_avg_record_size=0;query_lookups=0;query_lookup_avg_rec_count=0;query_lookup_avg_record_size=0;gc-period=1000;gc-max-units=1000;data-max-memory=18446744073709551615;tracing=0;histogram=false;ignore-not-sync=true".split(';')
        elif command == 'sindex/lifetimeAggregations/campaignLifetimeIS':
            nsdata = "keys=0;objects=0;ibtr_memory_used=18688;nbtr_memory_used=0;si_accounted_memory=18688;load_pct=100;loadtime=0;stat_write_reqs=0;stat_write_success=0;stat_write_errs=0;stat_delete_reqs=0;stat_delete_success=0;stat_delete_errs=0;stat_gc_recs=0;stat_gc_time=0;query_reqs=0;query_avg_rec_count=0;query_avg_record_size=0;query_agg=0;query_agg_avg_rec_count=0;query_agg_avg_record_size=0;query_lookups=0;query_lookup_avg_rec_count=0;query_lookup_avg_record_size=0;gc-period=1000;gc-max-units=1000;data-max-memory=18446744073709551615;tracing=0;histogram=false;ignore-not-sync=true".split(';')
        elif command == 'sindex/lifetimeAggregations/campaignLifetimeOW':
            nsdata = "keys=0;objects=0;ibtr_memory_used=18688;nbtr_memory_used=0;si_accounted_memory=18688;load_pct=100;loadtime=0;stat_write_reqs=0;stat_write_success=0;stat_write_errs=0;stat_delete_reqs=0;stat_delete_success=0;stat_delete_errs=0;stat_gc_recs=0;stat_gc_time=0;query_reqs=0;query_avg_rec_count=0;query_avg_record_size=0;query_agg=0;query_agg_avg_rec_count=0;query_agg_avg_record_size=0;query_lookups=0;query_lookup_avg_rec_count=0;query_lookup_avg_record_size=0;gc-period=1000;gc-max-units=1000;data-max-memory=18446744073709551615;tracing=0;histogram=false;ignore-not-sync=true".split(';')
    ### DEBUG ONLY STATIC DATA : END
    
    ##print nsdata
    if single:
        return nsdata
    nskey = ''
    for val in nsdata:
        nsval = {}
        if val in ("\n", ''):
            continue
        ##print val
        if ns:
            (nskey,nstval) = val.split(':')
            nstval = nstval.split(',')
        elif sets:
            nskey = ''
            nstval = val.split(':')
        elif latency:
            nstval = val.split(',')
            nstkey = nstval[0].split(':',1)[0]
            if nstkey in ('reads','writes_master','proxy','udf','query'):
                nskey = nstkey
                continue
        else:
            nskey = ''
            nstval = [val]
        ##print { nskey: nstval }
        if latency:
            asdatadog("aerospike.{0}.{1}.time".format(cmd,nskey), command, nskey, nstval[0], onlynode ) # time
            asdatadog("aerospike.{0}.{1}.opssec".format(cmd,nskey), command, nskey, nstval[1], onlynode ) # ops/sec
            asdatadog("aerospike.{0}.{1}.more1ms".format(cmd,nskey), command, nskey, nstval[2], onlynode ) # >1ms
            asdatadog("aerospike.{0}.{1}.more8ms".format(cmd,nskey), command, nskey, nstval[3], onlynode ) # >8ms
            asdatadog("aerospike.{0}.{1}.more64ms".format(cmd,nskey), command, nskey, nstval[4], onlynode ) # >64ms
            continue
        else:
            for nsdataval in nstval:
                (dkey,dval) = nsdataval.split('=')
                if ns:
                    asdatadog("aerospike.{0}.{1}.{2}".format(cmd,nskey,dkey), command, dkey, dval, onlynode )
                elif sets:
                    if command == 'sets':
                        if dkey in ('ns_name','set_name'):
                            nsval[dkey] = dval
                        elif not nodatadog:
                            asdatadog("aerospike.{0}.{1}.{2}.{3}".format(cmd,nsval['ns_name'],nsval['set_name'],dkey), command, dkey, dval, onlynode )
                    elif command == 'sindex':
                        if dkey in ('ns','set','indexname'):
                            nsval[dkey] = dval
                        elif not nodatadog:
                            asdatadog("aerospike.{0}.{1}.{2}.{3}".format(cmd,nsval['ns'],nsval['set'],dkey), command, dkey, dval, onlynode )
                elif not nodatadog:
                    asdatadog("aerospike.{0}.{1}".format(cmd,dkey), command, dkey, dval, onlynode )
        retdata.append(nsval)
        ##print { nskey: nsval }
    
    return retdata

try:
    # connect to datadog statsd
    initialize(config['datadog']['host'], config['datadog']['port'])

    try:
        # connect to aerospike cluster 
        asclient = aerospike.client({'hosts': list( (k,v) for (k,v) in config['aerospike']['hosts'] ) }).connect()
    except:
        asdatadog("aerospike.node.up", 'node', 'up', 'false', True )
        sys.exit()

    # read current node name
    node = asmetrics('node',single=True)[0]
    asdatadog("aerospike.node.up", 'node', 'up', 'true', True )

    ## global ( cluster ) statistics
    #asmetrics('bins', ns=True)
    #asmetrics('get-config')
    #namespaces = asmetrics('namespaces',ns=True,single=True)
    #for ns in namespaces:
    #    asmetrics('namespace/{0}'.format(ns))
    #asmetrics('sets', sets=True)
    #sindex = asmetrics('sindex', sets=True, nodatadog=True)
    #for sidx in sindex:
    #    asmetrics('sindex/{0}/{1}'.format(sidx['ns'],sidx['indexname']))
    #asmetrics('statistics')
    #asmetrics('latency:',latency=True)
    
    ## instance ( node ) statistics
    service = asmetrics('service',single=True)[0]
    asmetrics('bins', ns=True, onlynode=True, nodehost=service)
    #asmetrics('get-config', onlynode=True, nodehost=service)
    namespaces = asmetrics('namespaces',ns=True,single=True, onlynode=True, nodehost=service)
    for ns in namespaces:
        asmetrics('namespace/{0}'.format(ns), onlynode=True, nodehost=service)
    asmetrics('sets', sets=True, onlynode=True, nodehost=service)
    sindex = asmetrics('sindex', sets=True, nodatadog=True, onlynode=True, nodehost=service)
    for sidx in sindex:
        asmetrics('sindex/{0}/{1}'.format(sidx['ns'],sidx['indexname']), onlynode=True, nodehost=service)
    asmetrics('statistics', onlynode=True, nodehost=service)
    asmetrics('latency:',latency=True, onlynode=True, nodehost=service)
    
    asclient.close()
    
except IOError as e:
    print '{E} IO Error {0} [{1}]'.format(e.errno, e.strerror)
    raise
except ValueError:
    print '{E} Value error'
    raise
except AerospikeError as ae:
    print '{E} Aerospike error: {0} [{1}]'.format(ae.msg, ae.code)
    raise
except exceptions.SystemExit:
    sys.exit()
except:
    print '{E} Unexpected error: ', sys.exc_info()[0]
    raise


