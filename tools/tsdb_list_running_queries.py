#!/usr/bin/env python
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
from datetime import datetime
from collections import defaultdict


class OpenTSDBListRunningQueries(object):

    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M%S'

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
            print(_, file=sys.stderr)
            sys.exit(1)
        except httplib.HTTPException as _:
            print(_, file=sys.stderr)
            sys.exit(1)
        return json.loads(resp)

    # reference_timestamp is for comparing N-ago timestamps
    def convert_timestamp_to_epoch(self, timestamp, reference_timestamp):
        timestamp = str(timestamp).split('.')[0]
        reference_timestamp_datetime = datetime.strptime(reference_timestamp, '%Y-%m-%d %H:%M:%S') # %z not supported in the C runtime, and no workaround until Python 3.2
        reference_timestamp_secs = reference_timestamp_datetime.strftime('%s')
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
        elif '-' in timestamp and timestamp[-4:] != '-ago':
            timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            timestamp = time.mktime(timestamp)
        timestamp = int(float(timestamp))
        timestamp = self.ms_to_secs(timestamp)
        return timestamp

    def convert_human_time(self, epoch):
        epoch = self.ms_to_secs(epoch)
        human_time = time.strftime('%F %T', time.gmtime(float(epoch)))
        #print('converted epoch {} to {}'.format(epoch, human_time))
        return human_time

    @staticmethod
    def ms_to_secs(epoch):
        # convert from ms to secs if epoch is in ms
        if len(str(epoch)) > len(str(int(time.time()))):
            epoch = int(int(epoch) / 1000)
        return epoch

    def main(self):
        current_time = int(time.time())
        stats = self.request()

        running_queries = stats.get('running', [])
        format_string = '{:<19}\t{:<10}\t{:<19}\t{:<19}\t{:<16}\t{:<10}\t{:<10}\t{}'
        print('='*160)
        print(format_string.format('Date', 'User', 'Start', 'End', 'TimeRange (Secs)', 'Aggregator', 'Downsample', 'Metric'))
        print('='*160 + '\n')
        running_queries.sort(key=lambda x: int(x['queryStart']), reverse=False)
        running_subquery_count = 0
        for query in running_queries:
            if os.getenv('DEBUG'):
                print(json.dumps(query, indent=4, sort_keys=True))
            user = query.get('headers', {}).get('X-WEBAUTH-USER', '')
            querystart = query.get('queryStart', '')
            querystart_secs = self.ms_to_secs(querystart)
            querystart = self.convert_human_time(querystart_secs)
            query = query['query']
            start = query['start']
            #start = time.strftime(self.TIMESTAMP_FORMAT, time.gmtime(float(start)/1000))
            start_epoch = self.ms_to_secs(self.convert_timestamp_to_epoch(start, querystart))
            start = self.convert_human_time(start_epoch)
            end = query.get('end', '')
            if end:
                #end= time.strftime(self.TIMESTAMP_FORMAT, time.gmtime(float(end)/1000))
                end_epoch = self.ms_to_secs(self.convert_timestamp_to_epoch(end, querystart))
            else:
                end_epoch = int(datetime.strptime(querystart, '%Y-%m-%d %H:%M:%S').strftime('%s'))
            end = self.convert_human_time(end_epoch)
            timerange_secs = end_epoch - start_epoch
            for subquery in query.get('queries', []):
                metric = subquery.get('metric', '')
                aggregator = subquery.get('aggregator')
                downsample = subquery.get('downsample')
                print(format_string.format(querystart, user, start, end, timerange_secs, aggregator, downsample, metric))
                running_subquery_count +=1 

        running_query_count = len(running_queries)
        print('\nListed {} running queries, {} individual subqueries'.format(running_query_count, running_subquery_count))
        sys.stdout.flush()
        return 0


if __name__ == "__main__":
    try:
        OpenTSDBListRunningQueries().main()
    except KeyboardInterrupt:
        print("Control-C, aborting...")
