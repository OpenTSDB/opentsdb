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

DEBUG=True


# 3rd party libraries and tools
import requests # see http://www.python-requests.org/en/latest/

product_name = "py-tsdbtool"
version = '0.2.0'

custom_headers = {'User-Agent': '%s/%s' % (product_name, version)}

# headers needed for a given content protocol
protocol_header_map = {
                'wwwform': {'Content-Type':'x-www-form-urlencoded',
                           },
                'json':    {'Content-Type':'application/json',
                            'Accept':'text/plain,application/json',
                           },
                None:      {},
               }


def get_protocol_headers(protocol):
    """Returns the HTTP headers needed for the supplied protocol.
"""
    if not protocol_header_map.has_key(protocol):
        raise ValueError("Protocol '%s' is unsupported." % protocol)
    return protocol_header_map[protocol]

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
import logging    
logger = logging


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
        rstr = "<_TSDHost instance,\n\thost '%s', "
        if self._connection == None:
            rstr += "not connected>"
        else:
            rstr += "connected, "
            if self._is_up:
                rstr += "up, "
            else:
                rstr += "down, "
        rstr += "\n\tcreated: %s \n\tlast use: %s \n\tlast success: %s>\n"
        rstr %= (self._host, my_ctime(self._created), 
                 my_ctime(self._last_use), my_ctime(self._last_success))
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


    def _post(self, url, data, protocol=None):
        """Internal HTTP POST method.
    
    url - URL to post to.

    data - The data to post. Can be a dictionary or string.

    protocol - protocol used to communicate to remote server which may change

"""
        self._last_use = time.time()
        response = None
        headers = get_protocol_headers(protocol)
        if DEBUG: print "DEBUG: POST %s data:\n'%s'\nheaders: %s" % (url, data, headers)
        try:
            response = self._connection.post(url, data,
                                             timeout = self._options['timeout'],
                                             headers=headers,
                                            )
        except requests.exceptions.RequestException as e:
            logger.error("Post failed: %s" % repr(e)) 
            self._is_up = False 
            raise e
        else:
            self._is_up = True
            self._last_success = self._last_use
        return response

    def _get(self, url, protocol=None):
        self._last_use = time.time()
        response = None
        headers = get_protocol_headers(protocol)
        if DEBUG: print "DEBUG: GET %s" % url
        try:
            response = self._connection.get(url,
                                            timeout = self._options['timeout'],
                                            headers=headers,
                                           )
        except requests.exceptions.RequestException as e:
            logger.error("Get failed: %s" % repr(e)) 
            self._is_up = False
            raise e
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

    def post(self, query, data, protocol=None):
        if not self._connection:
            self._connect()
        response = self._post('%s/%s' % (self._host, query), data, 
                              protocol=protocol)
        return response        

    def get(self, query, protocol=None):
        if not self._connection:
            self._connect()
        response = self._get('%s/%s' % (self._host, query), protocol)
        return response

    def is_available(self, force=False):
        """Check if connection is available. Set force = True to force
a reconnect try even if we're in the retry interval.
"""
        if self._is_up: return True # easy case
        # we think we're down. See if we're right
        
        #if we're still within the retry interval, we're down.

        now = time.time()
        if not force and ((now - self._last_use) < 
                          self._options['retry-interval']):
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

"retry":
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
    (float, 300.0,
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
                my_new_host = ((hosts.index(self._host_pointer) + 1) % 
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

    def _get_up_hosts(self):
        """Returns the subset of hosts configured that are currently in an
up state. Will try to contact down hosts that have exceeded their retry 
interval and will try to contact all hosts if all configured hosts are down,
even if their retry intervals have not been met.
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
        return up_hosts

    def _find_next_host(self):
        """Find the next host to contact using the balancing strategy defined.
Returns the host connection for the host.

"""
        up_hosts = self._get_up_hosts()
        if len(up_hosts) == 0:
            return None
        if len(up_hosts) == 1: # with only one up host, there is no strategy.
            self._host_pointer = self._hostconns[up_hosts[0]]
            return self._hostconns[up_hosts[0]]
        balance = self._options['balance']
        if balance == 'rr':
            ret_host = self._find_next_round_robin(up_hosts)
        elif balance == 'random':
            ret_host = self._find_next_random(up_hosts)
        else:
            logger.error("Undefined balance strategy '%s'. Using random." % balance)
            ret_host = self._find_next_random(up_hosts)
        return self._hostconns[ret_host]


    def _run_http_verb(self, verb, query, data=None, protocol=None,
                       retry=None):
        verb = verb.upper()
        if verb not in ('GET','POST'):
            raise ValueError("Unsupported HTTP verb '%s'" % verb)
        if retry == None:
            retry = self._options['retry']
        try:
            tries = int(retry + 1)
        except (ValueError, TypeError):
            raise ValueError("invalid argument for retry: %s" % retry)
        response = None
        for t in range(tries):
            node = self._find_next_host()
            if not node:
                continue
            if verb == 'GET':
                response = node.get(query, protocol=protocol)
            elif verb == 'POST':
                response = node.post(query, data=data, protocol=protocol)
            else:
                raise ValueError("Didn't we check %s already?" % verb)
        return response

        
    def get_from_pool(self, query, protocol=None, retry=None):
        """Run a get against TSDB. This method will retry the operation 
against another host according to the balancing strategy up to the number of 
times specified in the retry config or the retry argument, if supplied. 
Returns None if the query fails."""
        return self._run_http_verb('GET', query, protocol)


    def post_to_pool(self, query, data=None, protocol=None, retry=None):
        """Run a post against TSDB. This method will retry the operation 
against another host according to the balancing strategy up to the number of 
times specified in the retry config or the retry argument, if supplied. 
Returns None if the query fails."""
        return self._run_http_verb('POST', query, data, protocol)

    def __repr__(self):
        rstr = "<TSDBPool instance, hosts in pool: %s, selection strategy %s>" % (self._hostlist, self._options['balance'])
        return rstr



class TSDBPoolConfig(object):
    """A class for reading a config file describing where to find the TSDs
this library should query."""



    def __init__(self, configfile="./py-tsdbtool.config", init=False):
        """TSDBPoolConfig init:
    configfile - Name the configuration file to use to set up the TSD query
                 pool.

    init - set to true if you want to set up the query pool on initialization,
           The default is false which means to defer query pool set up until
           either the first query is requested, or if the get_pool() method
           is invoked.
"""

        cfh = file(configfile)
        self._read_config_file(cfh)
        cfh.close()
        self._parse_config()
        if init:
            self.return_pool()
        else:
            self._TSDPool = None
        


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
        self._globalconf = global_conf
        self._hostconf = config['hosts'] 
         
    def get(self, query, protocol=None):
        """GET against the pool of TSDs in this configuration.
"""
        if not self._TSDPool:
            self.return_pool()
        return self._TSDPool.get_from_pool(query, protocol=protocol)


    def post(self, query, data=None, protocol=None):
        if not self._TSDPool:
            self.return_pool()
        return self._TSDPool.post_to_pool(query, data, protocol)

    def head(self, query, data=None, protocol=None):
        raise NotImplementedError("HEAD verb not implemented.")

    def put(self, query, data=None, protocol=None):
        raise NotImplementedError("PUT verb not implemented.")

    def delete(self, query, data=None, protocol=None):
        raise NotImplementedError("DELETE verb not implemented.")

    def trace(self, query, data=None, protocol=None):
        raise NotImplementedError("TRACE verb not implemented.")

    def connect(self, query, data=None, protocol=None):
        raise NotImplementedError("CONNECT verb not implemented.")

    def return_pool(self):
        """Returns the TSD server pool associated with this configuration.
"""
        if not self._TSDPool:
            self._TSDPool = TSDBPool(self._hostconf, self._globalconf)
        return self._TSDPool


class TSD_protocol(object):
    """TSD_protocol abstracts the TSD query mechanics away from the programmer,
turning it into a fairly easy to use method-based interface.
""" 
    def __init__(self, config):
        """TSD_protocol constructor.

Required arguments:

    config - either the name of a configuration file, or an instance of the
             TSDBPoolConfig class that has been previously configured.

Returns an instance of the TSD_protocol.
"""
        if type(config) == str:
            self._config = TSDBPoolConfig(config)
        else:
            self._config = config
        self._valid_aggregators = None

    def _validate_aggregator(self, aggregator):
        """Check if the aggregator is known to the TSD pool.

Required arguments:

    aggregator - The aggregator to validate

Does not return a value, but raises a ValueError if the aggregator is not
valid, or if there was a problem retrieving valid aggregators.
"""

        if not self._valid_aggregators:
            agresponse = self.api_aggregators()
            if agresponse.status_code != 200:
                raise ValueError("There was a problem with the request.",
                                 agresponse.status_code,
                                 agresponse
                                )
            self._valid_aggregators = self.api_aggregators().json()
        if aggregator not in self._valid_aggregators:
            raise ValueError("Aggregator must be one of %s" %
                             self._valid_aggregators)

    def api_aggregators(self):
        """Implements the OpenTSDB v2 API call to /api/aggregators

    No arguments. Returns a Request response that includes a list of all 
supported aggregators.
"""
        return self._config.get("api/aggregators")

    def api_query(self, start, subqueries, end=None, noannotations=False,
              globalannotations=False, msresolution=False, showtsuids=False):

        """Implements the OpenTSDB v2 API call to /api/query.

Required arguments:

    start - any valid time that OpenTSDB understands.

    subqueries - an array of subqueries. You can use the make_subquery()
                 method in this class to generate suitable entries for this
                 array.

Optional arguments:

    end - any valid end time that OpenTSDB understands. Should be later than
          start time or OpenTSDB will send an error response.

    noannotations - If set to true, annotations will be suppressed.

    globalannotations - return any global annotations with the query if true.

    msresolution - If true, data points will be output to millisecond
                   resolution. Otherwise, the default resolution is 1 second.

    showtsuids - If true, the TSU IDs of the timeseries will also be returned.

The above descriptions are only to jog your memory of the purposes of the
arguments. For more detailed information, consult the OpenTSDB docs.

Returns a Requests response instance that contains the results of the query.
    
"""
        if len(subqueries) == 0:
            raise ValueError("subqueries must contain at least one entry.")
        request_object = {'start': start,
                          'queries': subqueries,
                         }
        if end: request_object['end'] = end
        if noannotations: request_object['noAnnotations'] = True
        if globalannotations: request_object['globalAnnotations'] = True
        if msresolution: request_object['msResolution'] = True
        if showtsuids: request_object['showTSUIDs'] = True
        request_json = json.dumps(request_object, indent=4)
        return self._config.post("api/query", 
                                 request_json,
                                 protocol="json",
                                )

    def api_suggest(self, datatype, q, maximum=None):
        """Implements the OpenTSDB v2 API call to /api/suggest

Required arguments:

    datatype - the type of data to search for. Can be 'metrics' for metrics,
               'tagk' for tag keys, or'tagv' for tag values. This argument
               is called 'type' in the API but is renamed here to avoid
               conflict with the python 'type' type.

    q - the string to search with.

Optional arguments:

    maximum - maximum number of suggested results to return. Must be a 
              positive integer. This argument is called 'max' in the API
              but is renamed here to avoid conflict with the python 'max'
              function.

    Returns a Requests Response object that contains the result of the query. 
    A 200 status code will reflect a successful query. 400 is usually the 
    result of a failed requirement in the query, and 500 usually represents 
    a server problem.

"""
        datatype = datatype.lower()
        valid_types = ('metrics','tagk','tagv')
        if datatype not in valid_types:
            raise ValueError("type '%s' not one of %s" % 
                             (datatype, valid_types))
        suggest_query = {'type':datatype, 'q':q}
        if maximum:
            try:
                if int(maximum) < 1:
                    raise ValueError
            except (ValueError, TypeError):
                raise ValueError("Invalid value '%s' for maximum." % maximum)
            suggest_query['max'] = int(maximum)
        return self._config.post('api/suggest',
                                 json.dumps(suggest_query),
                                 protocol='json')  


    def make_metrics_subquery(self, aggregator, metric, rate=None, 
                             counter=None, countermax=None, resetvalue=None, 
                             downsample=None, tags=None):
        """Creates the data structure for a metrics subquery, suitable for parsing
down into the JSON request object.

Required arguments:
    aggregator - The aggregator function to use in this subquery. This will
                 be validated against a list of aggregators supplied by
                 OpenTSDB.

    metric - The metric you wish to query.

Optional arguments:
    rate - convert the data into deltas before returning.

    counter - the data is a monotonically increasing counter that may
              roll over.

    countermax - the maximum value for the counter.

    resetvalue - if the rate value exceeds this number, it will be clamped
                 to zero.

    downsample - a downsampling function.

    ags - a map containing tag/value pairs.

The above descriptions are only to jog your memory of the purposes of the
arguments. For more detailed information, consult the OpenTSDB docs.

Returns a dictionary suitable for use in a sub-query array for use in the 
api_query() method.
"""
        self._validate_aggregator(aggregator)
        subquery = {"aggregator":aggregator,
                    "metric":metric}
        if rate: subquery['rate']= True
        if downsample: subquery['downsample']= downsample
        if tags: 
            subquery['tags']= tags
        # Remove this else clause before flight. OpenTSDB bug workaround TODO
        else:
            subquery['tags'] = {'host':'*'}
        if counter or countermax or resetvalue:
            rateoptions = {}
            if counter: rateoptions['counter'] = True
            if countermax: rateoptions['counterMax'] = int(countermax)
            if resetvalue: rateoptions['resetValue'] = int(resetvalue)
            subquery['rateOptions'] = rateoptions
    
        return subquery                          
                
    def make_tsuid_subquery(self, aggregator, tsuids):
        """Creates the data structure for a TSU ID subquery, suitable for parsing
down into the JSON request object.

Required arguments:
    aggregator - The aggregator function to use in this subquery. This will
                 be validated against a list of aggregators supplied by
                 OpenTSDB.

    tsuids - A list of hexadecimal encoded TSU IDs

Returns a dictionary suitable for use in a sub-query array for use in the
api_query() method.
"""
        self._validate_aggregator(aggregator)
        return {'aggregator':aggregator, 'tsuids':tsuids}    
        
