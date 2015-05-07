#!/usr/bin/python
#
# This little script can be used to replace TSDs while performing prolonged
# HBase or HDFS maintenances.  It runs a simple, low-end TCP server to accept
# all the data points from tcollectors and dump them to a bunch of files, one
# per client IP address.  These files can then later be batch-imported, once
# HBase is back up.
#
# This file is part of OpenTSDB.
# Copyright (C) 2013  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

import os
import socket
import sys
import SocketServer

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  allow_reuse_address = True


DRAINDIR = None

class Handler(SocketServer.StreamRequestHandler):
  def handle(self):
    sys.stdout.write("O")
    sys.stdout.flush()
    out = open(os.path.join(DRAINDIR, self.client_address[0]), "a")
    n = 0
    while True:
      req = self.rfile.readline()
      if not req:
        break
      if req == "version\n":
        self.wfile.write("drain.py\n")
        sys.stdout.write("V")
        sys.stdout.flush()
        continue
      if not req.startswith("put "):
        sys.stdout.write("!")
        sys.stdout.flush()
        continue
      out.write(req[4:])
      out.flush()
      if n % 100 == 0:
        sys.stdout.write(".")
        sys.stdout.flush()
      n += 1
    out.close()
    sys.stdout.write("X")
    sys.stdout.flush()


def main(args):
  if len(args) != 3:
    sys.stderr.write("Usage: %s <port> <directory>" % args[0])
    return 1
  global DRAINDIR
  port = int(args[1])
  DRAINDIR = args[2]
  if not os.path.isdir(DRAINDIR):
    os.makedirs(DRAINDIR)
  server = ThreadedTCPServer(("0.0.0.0", port), Handler)
  try:
    print ("Use Ctrl-C to stop me.")
    server.serve_forever()
  except KeyboardInterrupt:
    pass


if __name__ == "__main__":
  sys.exit(main(sys.argv))
