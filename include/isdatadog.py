#!/usr/bin/python

###############
#
# @name: Datadog Class
# @version: 2016/06/05
# @author: andris jegorov
# @email: andris@ironsrc.com
# @description: Datadog classes isdatadog, isevents
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
    import yaml
except ImportError:
    print '{E} python module "yaml" is not present in system. Please install using "pip install yaml".'
    raise

import os.path

class isevents:
    def __init__(self, config):
        self.events = {}
        self.configfile = config.configfile
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
        if os.path.isfile(self.configfile+'.events'):
            try:
                with open(self.configfile+'.events', mode='w') as stream:
                    yaml.safe_dump(self.events, stream=stream)
                    stream.close()
            except IOError as e:
                print '[E] events file create problem: {0}'.format(e)
                raise

class isdatadog:
    events = None

    def __init__(self, config):
        self.config = config.config['checks']['defaults']
        self.configfile = config.configfile
        self.hostname = config.hostname
        self.debug = config.config['debug']
        initialize(config.config['datadog']['host'], config.config['datadog']['port'])
        if self.config['events']['enabled']:
            self.events = isevents(config)

    def number(self, metric, value, tags=[], mode=False):
        if mode == False:
            mode = self.config['number']
        if mode == 'histogram':
            self.histogram(metric=metric, value=value, tags=tags)
        elif mode == 'gauge':
            self.gauge(metric=metric, value=value, tags=tags)
        elif mode == 'set':
            self.set(metric=metric, value=value, tags=tags)
        elif mode == 'counter':
            self.counter(metric=metric, value=value, tags=tags)

    def histogram(self, metric, value, tags=[]):
        statsd.histogram(metric=metric, value=value, tags=tags)
        if self.debug:
            print "{0} = {1} :: type={3} :: tags={2}".format(metric, value, tags, 'histogram')

    def gauge(self, metric, value, tags=[]):
        statsd.gauge(metric=metric, value=value, tags=tags)
        if self.debug:
            print "{0} = {1} :: type={3} :: tags={2}".format(metric, value, tags, 'gauge')

    def set(self, metric, value, tags=[]):
        statsd.set(metric=metric, value=value, tags=tags)
        if self.debug:
            print "{0} = {1} :: type={3} :: tags={2}".format(metric, value, tags, 'set')

    def counter(self, metric, value, tags=[]):
        statsd.increment(metric=metric, value=value, tags=tags)
        if self.debug:
            print "{0} = {1} :: type={3} :: tags={2}".format(metric, value, tags, 'counter')

    def event(self, metric, value, tags=[]):
        import socket
        statsd.event(title=metric, text=value, tags=tags, hostname=socket.gethostname())
        if self.debug:
            print "{0} = {1} :: type={3} :: tags={2}".format(metric, value, tags, 'event')

    def isevent(self, metric, value, cluster=False, service=False):
        if self.config['events']['enabled']:
            return self.events.isevent(metric, value, cluster, service)
        else:
            return False

    def finalize(self):
        if self.config['events']['enabled']:
            self.events.finalize()
