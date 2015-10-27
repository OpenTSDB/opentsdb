#!/usr/bin/python
"""Restart opentsdb. Called using -XX:OnOutOfMemoryError=<this script>

Because it's calling the 'service opentsdb' command, should be run as root.

This is known to work with python2.6 and above.
"""
import os
import subprocess

service_name = "opentsdb"
if 'NAME' in os.environ:
    service_name = os.environ['NAME']

subprocess.call(["service", service_name, "stop"])
# Close any file handles we inherited from our parent JVM. We need
# to do this before restarting so that the socket isn't held open.
openfiles = [int(f) for f in os.listdir("/proc/self/fd")]
# Don't need to close stdout/stderr/stdin, leave them open so
# that there is less chance of errors with those standard streams.
# Other files start at fd 3.
os.closerange(3, max(openfiles))
subprocess.call(["service", service_name, "start"])
