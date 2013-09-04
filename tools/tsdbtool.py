#! /usr/bin/env python
"""tsdbtool - a python library for talking to OpenTSDB TSD
    
"""

# base python imports
import sys
import os
import logging
import random
import time
import json
import StringIO
from copy import deepcopy




# 3rd party libraries and tools
import requests # see http://www.python-requests.org/en/latest/

product_name = "py-tsdbtool"
version = '0.2.0'

custom_header = {'User-Agent': '%s/%s' % (product_name, version)}


def my_ctime(t, timetype='local', format='%Y-%m-%d %H:%M:%S'):
    if timetype == 'local':
        tstruct = time.localtime(t)
        tz = time.strftime("%Z", tstruct)
    else:
        tstruct = time.gmtime(t)
        tz = "UTC"
    return time.strftime("%s %s" % (format, tz), tstruct)



class _LogObject(logging.Logger):
    """TODO
"""
    
logger = _LogObject(product_name)


class _TSDHost(object):
    """Class that defines a single TSD host, its last known status, and when it
was last used to satisfy a query.
"""

    def __init__(self, host, options={}):

        """Set up a connection to a TSD host.
    where:
        host - a string containing a host name or IP address, it which case
               the host is interpreted to be the address or name of a TSD
               using HTTP listening on port 80, or a host:port combination
               which is interpreted to be the host and port of a TSD using
               HTTP listening on the specified port, or a URL prefix of the
               form 'proto://host-name-or-IP:port' where proto is the protocol
               to use (generally http or https) host-name-or-IP is the host
               or IP to use, and port is the port to use. The :port 
               specification may be left off and the default for the protocol
               in use will be used.

        options - a dictionary of options. Valid options and their meanings
                  are defined in the _HostOptions class.

"""
        self._host = host
        self._created = 0
        self._last_use = 0
        self._last_success = 0
        self._is_up = False
        self._connection = None # for keep alive connections
        self._fix_schema()
        self._options = HostOptions.validate_options(options)
        if not self._options['defer']:
            self._connect()
        

    def __repr__(self):
        rstr = "<_TSDHost instance, host '%s', "
        if self._connection == None:
            rstr += "not connected>"
        else:
            rstr += "connected, "
            if self._is_up:
                rstr += "up, "
            else:
                rstr += "down, "
        return rstr

    def _fix_schema(self):
        """Hostname and hostname:ports are allowed here but not in requests
URLS. Schema has to be explicit for that, so we look for a schema and if
there is none, assume http.
""" 
        protocheck = self._host.split("://")
        if len(protocheck) == 1:
            self._host = "http://" + self._host
            pass



    def _get(self, url):
        self._last_use = time.time()
        success = False
        response = None
        try:
            response = self._connection.get(url,
                                            timeout = self._options['timeout'],
                                           )
        except requests.exceptions.RequestException:
            logger.error("Get failed") # TODO
            self._is_up = False
        if response:
            if response.status_code > 399:
                logger.error("Get failed") # TODO
                self._is_up = False
                response = None
            else:
                self._is_up = True
                self._last_success = self._last_use
        return response


    def _connect(self):
        """Connects to the host defined in the instance, creating a session
to define it.
"""
        self._connection = requests.Session()
        self._connection.headers.update(custom_headers)
        self._connection.proxies = self._options['proxies']
        self._created = time.time()
        response = self._get('%s/' % (self._host))
        return response
        

    def get(self, query):
        if not self._connection:
            self._connect()
        qresult = self._get('%s/%s' % (self._host, query))
        return qresult

    def is_available(self, force=False):
        """Check if connection is available. Set force = True to force
a reconnect try even if we're in the retry interval.
"""
        if self._is_up: return True # easy case
        # we think we're down. See if we're right
        
        #if we're still within the retry interval, we're down.

        now = time.time()
        if (now - self._last_use) < self._options['retry_interval']:
            return False

        # enough time has elapsed. Let's try again
        self._connect()
        return self._is_up


class _HostOptions(object):
    """Definition of valid host options. 
"""

# options allowed are defined with the option name as a key, and an array
# containing a datatype, default value and a string that describes to option.
    _valid_options = {

"defer":
    (bool, False,
    """A boolean that tells whether the host should be connected when the
host is intialized (False) or wait to connect until a request is actually made
(True). 
"""),

"proxies":
    (dict, None,
    """A dictionary of protocols and proxy servers in the form the _TSDHost
object expects it. See help(TSDBPool) for more details on the proxies
dictionary behavior.
"""),

"retry"
    (int, 0,
    """An integer that tells the query pool mechanism how many times to retry
a query before giving up. Each retry will potentially be against the same
or a different host.
"""),

"retry-interval":
    (float, 300.0,
    """A number, in seconds, to make a connection eligible to be retried if
there was a problem the last time it was contacted. 
"""),

"weight":
    (int, 0,
    """Host usage priority for connection selection strategies. Larger numbers
imply higher priority."
"""),

"description":
    (str, '',
    """A string containing any sort of documentation regarding the host.
"""),

"timeout":
    (float, 15.0,
    """A value that determines how long to wait for a TCP connection, in seconds.
"""),
"balance":
    (str, "random",
    """A string that identifies the balance strategy used in picking hosts to
query. Current allowed values are 'rr' for round robin (hosts are tried in
order) or random (a random host is picked for each query). This value has no
real meaning at the host level and only has an effect at the pool level.
""")


}

    def is_valid_option(self, opt):
        if self._valid_options.has_key(opt): 
            return True
        return False

    def help(self, opt):
        if not self._is_valid_option(opt):
            return "'%s' not a valid host option." % opt
        return self.valid_options[opt][2]

    def list_valid_options(self):
        return self._valid_options.keys()

    def validate_options(self, opts):
        """Validates the options passed in as the opts argument and returns
an options dict that includes defaults for all values that were not supplied.
"""
        validated = {}
        for option in self._valid_options:
            details = self._valid_options[option]
            if opts.has_key(option):
                try:
                    # we always allow a None for a config value
                    if opts[option] == None:
                        validated[option] = None
                    else:
                        cast = details[0]
                        validated[option] = cast(opts[option])
                except ValueError:
                    raise ValueError("value '%s' for option '%s' not valid." % (opts[opt], opt))
            else:
                validated[option] = details[1] # supply default
        return validated


HostOptions = _HostOptions() 
        


class TSDBPool(object):
    """Class which defines which TSDs to speak to when using the library
"""
    def __init__(self, hosts, balance='random', retry_interval=300, 
                 retry = 0, proxies=None):
    def __init__(self, hosts, global_options={}):
        """TSDBPool initializer:
    c = TSDBPool(hosts, global_options)

    Where:
    
        hosts - a dictionary that contain as keys host names, ports, IP 
                addresses or URL prefixes that identify the TSD(s) to talk to. 
                Examples: "192.168.1.1", "tsdhost.example.com:8080", 
                "https://securetsd.example.com". Will only use HTTPS if the 
                https URL prefix is used. Default ports are 80 for unsecure and
                443 for secure. The value of the dictionary pair will contain 
                options for the given host, which is also a dictionary.

        global_options - A dictionary containing global defaults for library
                         settings. Valid values are described in HostOptions

"""

        self._host_pointer = None
        self._hostlist = []
        self._hostconns = {}
        self._options = {'balance':'random',
                         'retry_interval':300,
                         'retry': 0, 
                         'proxies' : None,
                        }
        self._options.update(global_options)
        for host in hosts:
            
            if self._hostconns.has_key(host):
                logger.warning("Duplicate host %s ignored. Skipping." % host)
                continue
            host_options = deepcopy(self._options)
            host_options.update(hosts[host])
            self._hostconns[host] = _TSDHost(host, host_options)
            self._hostlist.append(host) 

        pass

    def _find_next_round_robin(self, hosts):
        """Pick the next TSD in the round robin sequence.
"""
        if self._host_pointer == None:
            ret_host = hosts[0]
            self._host_pointer = ret_host
        else:
            try:
                my_new_host = ((hosts.index(self._pointer) + 1) % 
                              len(hosts))
            except ValueError:
                # The last host we used in the round robin is no longer
                # up, pick a new one at random
                my_new_host = random.randrange(0,len(hosts))
            ret_host = hosts[my_new_host]
            self._host_pointer = ret_host
        return ret_host

    def _find_next_random(self, hosts):
        """Pick a random host from the list sent"""
        return random.choice(hosts)

    def _find_next_host(self):
        """Find the next host to contact using the balancing strategy defined.
Returns the host connection for the host.

"""

        up_hosts = [h for h in self._hostlist if 
                    self._hostconns[h].is_available()]
        if not len(up_hosts):
            # if everything is down, force a repoll of all hosts regardless
            # of the retry intervals
            up_hosts = [h for h in self._hostlist if 
                        self._hostconns[h].is_available(force=True)]
            if len(up_hosts) == 0: # really down
                logger.error("All hosts down, cannot find one to connect to.")
                return None
        if len(up_hosts) == 1: # with only one up host, there is no strategy.
            self._host_pointer = self._hostconns[up_hosts[0]]
            return self._hostconns[up_hosts[0]]
        if self._balance == 'rr':
            ret_host = self._find_next_round_robin(up_hosts)
        elif self._balance == 'random':
            ret_host = self._find_next_random(up_hosts)
        else:
            logger.error("Undefined source selection strategy '%s'." % self._balance)

        return self._hostconns[ret_host]
        
    def query_pool(self, query, retry=None):
        """Run a query against TSDB. This method will retry the operation 
against another host according to the balancing strategy up to the number of 
times specified in the retry config or the retry argument, if supplied. 
Returns None if the query fails."""

        if retry = None:
            retry = 
        try:
            tries = int(retry + 1)
        except ValueError:
            logger.error("query_pool invalid argument for retry: %s" % retry)
            return None
        if tries < 1:
            logger.error("query_pool invalid argument for retry: %s" % retry)
            return None
        response = None
        last_node = None
        while t in range(tries):
            node = self._find_next_host()
            response = node.query(query)
            if response:
                break
        return response


class TSDBPoolConfig(object):
    """A class for reading a config file describing where to find the TSDs
this library should query."""



    def __init__(self, config_file="./py-tsdbtool.config"):

        cfh = file(config_file)
        self._read_config_file(cfh)
        cfh.close()
        self._parse_config()


    def _read_config_file(self, fh):
        """Read a configuration file describing the TSDs we want to contact.
The config file is a JSON file that contains a 'hosts' object and optionally
a 'global' object, defined as specified in the TSDBPool class. The file is
not strictly JSON as comment lines that start with # or // are also accepted
(and ignored). 
"""
        self._config = StringIO.StringIO()
        for b in fh:
            line = b.strip()
            if line.startswith("#") or line.startswith("//"):
                continue
            self._config.write(b)


    def _parse_config(self):
        self._config.seek(0)
        config = json.load(self._config)
        if config.has_key('global'):
            global_conf = config['global']
        else:
            global_conf = {}
        if not config.has_key('hosts') or len(config['hosts']) == 0:
            raise ValueError('tsdbtool configuration requires at least one host.')
        
        self._TSDPool = TSDBPool(config['hosts'], global_conf)
         
    def query(self, query):
        """Query againse the pool of TSDs in this configuration.
"""
        return self._TSDPool.query_pool(query)

    def get_pool(self):
        """Returns the TSD server pool associated with this configuration.
"""
        return self._TSDPool

        

