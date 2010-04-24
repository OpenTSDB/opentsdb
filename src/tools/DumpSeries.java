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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.HBaseException;
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

  public static void main(String[] args) {
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

    final TSDB tsdb = new TSDB(argp.get("--table", "tsdb"),
                               argp.get("--uidtable", "tsdb-uid"));
    final boolean delete = argp.has("--delete");
    final boolean importformat = delete || argp.has("--import");
    argp = null;
    doDump(tsdb, delete, importformat, args);
  }

  private static void doDump(final TSDB tsdb,
                             final boolean delete,
                             final boolean importformat,
                             final String[] args) {
    final HTable table = delete ? Core.getHTable(tsdb) : null;
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
        if (!importformat) {
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
        if (!importformat) {
          buf.append("  ");
        }
        for (final KeyValue kv : result.raw()) {
          buf.setLength(importformat ? 0 : 2);
          if (importformat) {
            buf.append(Core.metricName(tsdb, row)).append(' ');
          }
          final byte[] qualifier = kv.getQualifier();
          final short deltaflags = Bytes.toShort(qualifier);
          final short delta = (short) (deltaflags >>> 4);
          final byte[] cell = kv.getValue();
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
                 : Core.getTags(tsdb, row).entrySet()) {
              buf.append(' ').append(tag.getKey())
                 .append('=').append(tag.getValue());
            }
          } else {
            buf.append('\t')
              .append(base_time + delta)
              .append(" (").append(date(base_time + delta)).append(')');
          }
          buf.append('\n');
          System.out.print(buf);
        }

        if (delete) {
          final Delete del = new Delete(row);
          try {
            table.delete(del);
          } catch (IOException e) {
            throw new HBaseException("Failed to delete row="
                                     + Arrays.toString(row), e);
          }
        }
      }
    }
  }

  private static String date(final long timestamp) {
    return new Date(timestamp * 1000).toString();
  }

}
