// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

/**
 * Tool to dump the data straight from HBase.
 * Useful for debugging data induced problems.
 */
final class DumpSeries {

  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: scan"
        + " [--delete|--import] START-DATE [END-DATE] query [queries...]\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.\n"
        + "The --import flag changes the format in which the output is printed"
        + " to use a format suiteable for the 'import' command instead of the"
        + " default output format, which better represents how the data is"
        + " stored in HBase.\n"
        + "The --delete flag will delete every row matched by the query."
        + "  This flag implies --import.");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--import", "Prints the rows in a format suitable for"
                   + " the 'import' command.");
    argp.addOption("--delete", "Deletes rows as they are scanned.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    final HBaseClient client = CliOptions.clientFromOptions(argp);
    final byte[] table = argp.get("--table", "tsdb").getBytes();
    final TSDB tsdb = new TSDB(client, argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    final boolean delete = argp.has("--delete");
    final boolean importformat = delete || argp.has("--import");
    argp = null;
    try {
      doDump(tsdb, client, table, delete, importformat, args);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
  }

  private static void doDump(final TSDB tsdb,
                             final HBaseClient client,
                             final byte[] table,
                             final boolean delete,
                             final boolean importformat,
                             final String[] args) throws Exception {
    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);

    final StringBuilder buf = new StringBuilder();
    for (final Query query : queries) {
      final Scanner scanner = Core.getScanner(query);
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          buf.setLength(0);
          final byte[] key = row.get(0).key();
          final long base_time = Core.baseTime(tsdb, key);
          final String metric = Core.metricName(tsdb, key);
          // Print the row key.
          if (!importformat) {
            buf.append(Arrays.toString(key))
              .append(' ')
              .append(metric)
              .append(' ')
              .append(base_time)
              .append(" (").append(date(base_time)).append(") ");
            try {
              buf.append(Core.getTags(tsdb, key));
            } catch (RuntimeException e) {
              buf.append(e.getClass().getName() + ": " + e.getMessage());
            }
            buf.append('\n');
            System.out.print(buf);
          }

          // Print individual cells.
          buf.setLength(0);
          if (!importformat) {
            buf.append("  ");
          }
          for (final KeyValue kv : row) {
            // Discard everything or keep initial spaces.
            buf.setLength(importformat ? 0 : 2);
            formatKeyValue(buf, tsdb, importformat, kv, base_time, metric);
            buf.append('\n');
            System.out.print(buf);
          }

          if (delete) {
            final DeleteRequest del = new DeleteRequest(table, key);
            client.delete(del);
          }
        }
      }
    }
  }

  static void formatKeyValue(final StringBuilder buf,
                             final TSDB tsdb,
                             final KeyValue kv,
                             final long base_time) {
    formatKeyValue(buf, tsdb, true, kv, base_time,
                   Core.metricName(tsdb, kv.key()));
  }

  private static void formatKeyValue(final StringBuilder buf,
                                     final TSDB tsdb,
                                     final boolean importformat,
                                     final KeyValue kv,
                                     final long base_time,
                                     final String metric) {
    if (importformat) {
      buf.append(metric).append(' ');
    }
    final byte[] qualifier = kv.qualifier();
    final short deltaflags = Bytes.getShort(qualifier);
    final short delta = (short) ((0x0000FFFF & deltaflags) >>> 4);
    final byte[] cell = kv.value();
    final long lvalue = Core.extractLValue(deltaflags, kv);
    if (importformat) {
      buf.append(base_time + delta).append(' ');
    } else {
      buf.append(Arrays.toString(qualifier))
         .append(' ')
         .append(Arrays.toString(cell))
         .append('\t')
         .append(delta)
         .append('\t');
    }
    if ((deltaflags & 0x8) == 0x8) {
      buf.append(importformat ? "" : "f ")
         .append(Float.intBitsToFloat((int) lvalue));
    } else {
      buf.append(importformat ? "" : "l ")
         .append(lvalue);
    }
    if (importformat) {
      for (final Map.Entry<String, String> tag
           : Core.getTags(tsdb, kv.key()).entrySet()) {
        buf.append(' ').append(tag.getKey())
           .append('=').append(tag.getValue());
      }
    } else {
      buf.append('\t')
         .append(base_time + delta)
         .append(" (").append(date(base_time + delta)).append(')');
    }
  }

  /** Transforms a UNIX timestamp into a human readable date.  */
  static String date(final long timestamp) {
    return new Date(timestamp * 1000).toString();
  }

}
