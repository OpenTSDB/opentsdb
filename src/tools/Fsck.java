// This file is part of OpenTSDB.
// Copyright (C) 2011  The OpenTSDB Authors.
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import net.opentsdb.core.Const;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

/**
 * Tool to look for and fix corrupted data in a TSDB.
 */
final class Fsck {

  private static final Logger LOG = LoggerFactory.getLogger(Fsck.class);

  /** Number of LSBs in time_deltas reserved for flags.  */
  static final short FLAG_BITS;
  /** Mask to select all the FLAG_BITS.  */
  static final short FLAGS_MASK;
  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those public and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = Const.class.getDeclaredField("FLAG_BITS");
      f.setAccessible(true);
      FLAG_BITS = (Short) f.get(null);
      f = Const.class.getDeclaredField("FLAGS_MASK");
      f.setAccessible(true);
      FLAGS_MASK = (Short) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: fsck"
        + " [--fix] START-DATE [END-DATE] query [queries...]\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.\n"
        + "The --fix flag will attempt to fix errors,"
        + " but be careful when using it.");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--fix", "Fix errors as they're found.");
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
    final boolean fix = argp.has("--fix");
    argp = null;
    int errors = 42;
    try {
      errors = fsck(tsdb, client, table, fix, args);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
    System.exit(errors == 0 ? 0 : 1);
  }

  private static int fsck(final TSDB tsdb,
                           final HBaseClient client,
                           final byte[] table,
                           final boolean fix,
                           final String[] args) throws Exception {

    /** Callback to asynchronously delete a specific {@link KeyValue}.  */
    final class DeleteOutOfOrder implements Callback<Deferred<Object>, Object> {

        private final KeyValue kv;

        public DeleteOutOfOrder(final KeyValue kv) {
          this.kv = kv;
        }

        public Deferred<Object> call(final Object arg) {
          return client.delete(new DeleteRequest(table, kv.key(),
                                                 kv.family(), kv.qualifier()));
        }

        public String toString() {
          return "delete out-of-order data";
        }
      }

    int errors = 0;
    int correctable = 0;

    final short metric_width = width(tsdb, "metrics");
    final short name_width = width(tsdb, "tag_names");
    final short value_width = width(tsdb, "tag_values");

    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);
    final StringBuilder buf = new StringBuilder();
    for (final Query query : queries) {
      final long start_time = System.nanoTime();
      long ping_start_time = start_time;
      LOG.info("Starting to fsck data covered by " + query);
      long kvcount = 0;
      long rowcount = 0;
      final Bytes.ByteMap<Seen> seen = new Bytes.ByteMap<Seen>();
      final Scanner scanner = Core.getScanner(query);
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          rowcount++;
          // Take a copy of the row-key because we're going to zero-out the
          // timestamp and use that as a key in our `seen' map.
          final byte[] key = row.get(0).key().clone();
          final long base_time = Bytes.getUnsignedInt(key, metric_width);
          for (int i = metric_width; i < metric_width + Const.TIMESTAMP_BYTES; i++) {
            key[i] = 0;
          }
          Seen prev = seen.get(key);
          if (prev == null) {
            prev = new Seen(base_time - 1, row.get(0));
            seen.put(key, prev);
          }
          for (final KeyValue kv : row) {
            kvcount++;
            if (kvcount % 100000 == 0) {
              final long now = System.nanoTime();
              ping_start_time = (now - ping_start_time) / 1000000;
              LOG.info("... " + kvcount + " KV analyzed in " + ping_start_time
                       + "ms (" + (100000 * 1000 / ping_start_time) + " KVs/s)");
              ping_start_time = now;
            }
            if (kv.qualifier().length != 2) {
              LOG.warn("Ignoring unsupported KV with a qualifier of "
                       + kv.qualifier().length + " bytes:" + kv);
              continue;
            }
            final short qualifier = Bytes.getShort(kv.qualifier());
            final short delta = (short) ((qualifier & 0xFFFF) >>> FLAG_BITS);
            final long timestamp = base_time + delta;
            if (kv.value().length > 8) {
              errors++;
              LOG.error("Value more than 8 byte long with a 2-byte"
                        + " qualifier.\n\t" + kv);
            }
            if (timestamp <= prev.timestamp()) {
              errors++;
              correctable++;
              if (fix) {
                final byte[] newkey = kv.key().clone();
                // Fix the timestamp in the row key.
                final long new_base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
                Bytes.setInt(newkey, (int) new_base_time, metric_width);
                final short newqual = (short) ((timestamp - new_base_time) << FLAG_BITS
                                               | (qualifier & FLAGS_MASK));
                final DeleteOutOfOrder delooo = new DeleteOutOfOrder(kv);
                if (timestamp < prev.timestamp()) {
                  client.put(new PutRequest(table, newkey, kv.family(),
                                            Bytes.fromShort(newqual), kv.value()))
                    // Only delete the offending KV once we're sure that the new
                    // KV has been persisted in HBase.
                    .addCallbackDeferring(delooo);
                } else {
                  // We have two data points at exactly the same timestamp.
                  // This can happen when only the flags differ.  This is
                  // typically caused by one data point being an integer and
                  // the other being a floating point value.  In this case
                  // we just delete the duplicate data point and keep the
                  // first one we saw.
                  delooo.call(null);
                }
              } else {
                buf.setLength(0);
                buf.append(timestamp < prev.timestamp()
                           ? "Out of order data.\n\t"
                           : "Duplicate data point with different flags.\n\t")
                  .append(timestamp)
                  .append(" (").append(DumpSeries.date(timestamp))
                  .append(") @ ").append(kv).append("\n\t");
                DumpSeries.formatKeyValue(buf, tsdb, kv, base_time);
                buf.append("\n\t  was found after\n\t").append(prev.timestamp)
                  .append(" (").append(DumpSeries.date(prev.timestamp))
                  .append(") @ ").append(prev.kv).append("\n\t");
                DumpSeries.formatKeyValue(buf, tsdb, prev.kv,
                                          Bytes.getUnsignedInt(prev.kv.key(), metric_width));
                LOG.error(buf.toString());
              }
            } else {
              prev.setTimestamp(timestamp);
              prev.kv = kv;
            }
          }
        }
      }
      final long timing = (System.nanoTime() - start_time) / 1000000;
      System.out.println(kvcount + " KVs (in " + rowcount
                         + " rows) analyzed in " + timing
                         + "ms (~" + (kvcount * 1000 / timing) + " KV/s)");
    }

    System.out.println(errors != 0 ? "Found " + errors + " errors."
                       : "No error found.");
    if (!fix && correctable > 0) {
      System.out.println(correctable + " of these errors are automatically"
                         + " correctable, re-run with --fix.\n"
                         + "Make sure you understand the errors above and you"
                         + " know what you're doing before using --fix.");
    }
    return errors;
  }

  /**
   * Returns the width (in bytes) of a given kind of Unique IDs.
   */
  private static short width(final TSDB tsdb, final String idkind) {
    try {
      final Field metrics = TSDB.class.getDeclaredField(idkind);
      metrics.setAccessible(true);
      final short width = ((UniqueId) metrics.get(tsdb)).width();
      return width;
    } catch (Exception e) {
      throw new RuntimeException("in width " + idkind, e);
    }
  }

  /**
   * The last data point we've seen for a particular time series.
   */
  private static final class Seen {
    /** A 32-bit unsigned integer that holds a UNIX timestamp in seconds.  */
    private int timestamp;
    /** The raw data point (or points if the KV contains more than 1).  */
    KeyValue kv;

    private Seen(final long timestamp, final KeyValue kv) {
      this.timestamp = (int) timestamp;
      this.kv = kv;
    }

    /** Returns the UNIX timestamp (in seconds) as a 32-bit unsigned int.  */
    public long timestamp() {
      return timestamp & 0x00000000FFFFFFFFL;
    }

    /** Updates the UNIX timestamp (in seconds) with a 32-bit unsigned int.  */
    public void setTimestamp(final long timestamp) {
      this.timestamp = (int) timestamp;
    }
  }

}
