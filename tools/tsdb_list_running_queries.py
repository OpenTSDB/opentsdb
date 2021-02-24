#!/usr/bin/env python
# pylint: disable=line-too-long,missing-docstring
#
# List the running queries on the local TSD, sorted ascending by age, with normalized start time, end time, and time range in secs

from __future__ import print_function
from __future__ import division

import httplib
import json
import os
import socket
import sys
import time


class ConnectionException(Exception):
    pass


class OpenTSDBListRunningQueries(object):

    format_string = '{:<19}\t{:<10}\t{:<19}\t{:<19}\t{:<16}\t{:<10}\t{:<10}\t{}'

    # used by ms_to_secs() method further down to compare the epoch and figure out if it is in secs or ms and normalize it
    # pulling this out of the iteratively called method is a little more efficient to not recalculate this for every query in the list
    now_length = len(str(int(time.time())))

    timestamp_multiplier = {
        'ms': 0.001,
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400,
        'w': 7 * 86400,
        'n': 30 * 86400,
        'y': 365 * 86400
    }

    def __init__(self):
        self.host = 'localhost'
        self.port = 4242
        self.uri = '/api/stats/query'
        self.server = httplib.HTTPConnection(self.host, self.port)
        self.server.auto_open = True

    def request(self):
        try:
            self.server.request('GET', self.uri)
            resp = self.server.getresponse().read()
            self.server.close()
        except socket.error as _:
            raise ConnectionException('Socket Error querying TSDB port: {}'.format(_))
        except httplib.HTTPException as _:
            raise ConnectionException('HTTP Error querying TSDB port: {}'.format(_))
        return json.loads(resp)

    # reference_timestamp is for comparing N-ago timestamps
    def convert_timestamp_to_epoch(self, timestamp, reference_timestamp):
        timestamp = str(timestamp).split('.')[0]
        reference_timestamp_struct = time.strptime(reference_timestamp, '%Y-%m-%d %H:%M:%S') # %z not supported in the C runtime, and no workaround until Python 3.2
        reference_timestamp_secs = time.mktime(reference_timestamp_struct)
        if '/' in timestamp:
            timestamp = time.strptime(timestamp, '%Y/%m/%d-%H:%M:%S')
            timestamp = time.mktime(timestamp)
        elif timestamp == 'now':
            timestamp = reference_timestamp_secs
        elif timestamp[-4:] == '-ago':
            timestamp = timestamp[:-4]
            (ago, multiplier) = (timestamp[:-1], timestamp[-1])
            secs_ago = int(ago) * self.timestamp_multiplier[multiplier]
            timestamp = int(reference_timestamp_secs) - secs_ago
        timestamp = int(float(timestamp))
        timestamp = self.ms_to_secs(timestamp)
        return timestamp

    def convert_human_time(self, epoch):
        epoch = self.ms_to_secs(epoch)
        human_time = time.strftime('%F %T', time.gmtime(float(epoch)))
        #print('converted epoch {} to {}'.format(epoch, human_time))
        return human_time

    def ms_to_secs(self, epoch):
        # convert from ms to secs if epoch is in ms
        if len(str(epoch)) > self.now_length:
            epoch = int(int(epoch) / 1000)
        return epoch

    def process_query(self, query):
        if os.getenv('DEBUG'):
            print(json.dumps(query, indent=4, sort_keys=True))
        user = query.get('headers', {}).get('X-WEBAUTH-USER', '')
        querystart = query.get('queryStart', '')
        querystart_secs = self.ms_to_secs(querystart)
        querystart = self.convert_human_time(querystart_secs)
        query = query['query']
        start = query['start']
        start_epoch = self.ms_to_secs(self.convert_timestamp_to_epoch(start, querystart))
        start = self.convert_human_time(start_epoch)
        end = query.get('end', '')
        if end:
            end_epoch = self.ms_to_secs(self.convert_timestamp_to_epoch(end, querystart))
        else:
            end_epoch = int(time.mktime(time.strptime(querystart, '%Y-%m-%d %H:%M:%S')))
        end = self.convert_human_time(end_epoch)
        timerange_secs = end_epoch - start_epoch
        running_subquery_count = 0
        for subquery in query.get('queries', []):
            metric = subquery.get('metric', '')
            aggregator = subquery.get('aggregator')
            downsample = subquery.get('downsample')
            print(self.format_string.format(querystart, user, start, end, timerange_secs, aggregator, downsample, metric))
            running_subquery_count += 1
        return running_subquery_count

    def main(self):
        stats = self.request()
        running_queries = stats.get('running', [])
        print('='*160)
        print(self.format_string.format('Date', 'User', 'Start', 'End', 'TimeRange (Secs)', 'Aggregator', 'Downsample', 'Metric'))
        print('='*160 + '\n')
        running_queries.sort(key=lambda x: int(x['queryStart']), reverse=False)
        running_subquery_count = 0
        for query in running_queries:
            running_subquery_count += self.process_query(query)
        running_query_count = len(running_queries)
        print('\nListed {} running queries, {} individual subqueries'.format(running_query_count, running_subquery_count))
        sys.stdout.flush()
        return 0


if __name__ == "__main__":
    try:
        OpenTSDBListRunningQueries().main()
    except ConnectionException as _:
        print(_, file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("Control-C, aborting...")
        sys.exit(3)
