#!/usr/bin/python

###############
#
# @name: Configuration read Class
# @version: 2016/06/05
# @author: andris jegorov
# @email: andris@ironsrc.com
# @description: Configuration from yaml read class
# @licence: MIT
# @copyright (c) 2016 IronSource Mobile
#
###############

try:
    import yaml
except ImportError:
    print '{E} python module "yaml" is not present in system. Please install using "pip install yaml".'
    raise

import os.path

class isconfig:
    config = {}

    def __init__(self, configfile):
        self.hostname = os.uname()[1]
        self.configfile = configfile
        # read and parse configuration
        with open(self.configfile+'.yaml', mode='r') as stream:
            try:
                self.config = yaml.load(stream)
                stream.close()
            except yaml.YAMLError as err:
                print '[E] config problem: {0}'.format(err)
                raise
