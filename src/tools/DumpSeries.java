// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

/**
 * Tool to dump the data straight from HBase.
 * Useful for debugging data induced problems.
 */
final class DumpSeries {

  private static final Logger LOG = LoggerFactory.getLogger(DumpSeries.class);

  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: scan"
        + " START-DATE [END-DATE] query [queries...]\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  public static void main(String[] args) {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    final TSDB tsdb = new TSDB(argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    argp = null;
    doDump(tsdb, args);
  }

  private static void doDump(final TSDB tsdb, final String[] args) {
    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);

    final StringBuilder buf = new StringBuilder();
    for (final Query query : queries) {
      final ResultScanner scanner = Core.getScanner(query);
      for (final Result result : scanner) {
        buf.setLength(0);
        final byte[] row = result.getRow();
        final long base_time = Core.baseTime(tsdb, row);
        // Print the row key.
        {
          buf.append(Arrays.toString(row))
            .append(' ')
            .append(Core.metricName(tsdb, row))
            .append(' ')
            .append(base_time)
            .append(" (").append(date(base_time)).append(") ")
            .append(Core.getTags(tsdb, row))
            .append('\n');
          System.out.print(buf);
        }

        // Print individual cells.
        buf.setLength(0);
        buf.append("  ");
        for (final KeyValue kv : result.raw()) {
          buf.setLength(2);
          final byte[] qualifier = kv.getQualifier();
          final short deltaflags = Bytes.toShort(qualifier);
          final short delta = (short) (deltaflags >>> 4);
          final byte[] cell = kv.getValue();
          final long lvalue = Core.extractLValue(deltaflags, kv);
          {
            buf.append(Arrays.toString(qualifier))
              .append(' ')
              .append(Arrays.toString(cell))
              .append('\t')
              .append(delta)
              .append('\t');
          }
          if ((deltaflags & 0x8) == 0x8) {
            buf.append("f ").append(Float.intBitsToFloat((int) lvalue));
          } else {
            buf.append("l ").append(lvalue);
          }
          {
            buf.append('\t')
              .append(base_time + delta)
              .append(" (").append(date(base_time + delta)).append(')');
          }
          buf.append('\n');
          System.out.print(buf);
        }
      }
    }
  }

  private static String date(final long timestamp) {
    return new Date(timestamp * 1000).toString();
  }

}
