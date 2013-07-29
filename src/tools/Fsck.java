// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

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
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

/**
 * Tool to look for and fix corrupted data in a TSDB.
 */
final class Fsck {

  private static final Logger LOG = LoggerFactory.getLogger(Fsck.class);

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

    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    final TSDB tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    final byte[] table = config.getString("tsd.storage.hbase.data_table").getBytes(); 
    final boolean fix = argp.has("--fix");
    argp = null;
    int errors = 42;
    try {
      errors = fsck(tsdb, tsdb.getClient(), table, fix, args);
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

        public DeleteOutOfOrder(final byte[] key, final byte[] family, 
            final byte[] qualifier) {
          this.kv = new KeyValue(key, family, qualifier, new byte[0]);
        }
        
        public Deferred<Object> call(final Object arg) {
          return client.delete(new DeleteRequest(table, kv.key(),
                                                 kv.family(), kv.qualifier()));
        }

        public String toString() {
          return "delete out-of-order data";
        }
      }

    /**
     * Internal class used for examining data points in a row to determine if
     * we have any duplicates. Can then be used to delete the duplicate columns.
     */
    final class DP {
      
      long stored_timestamp;
      byte[] qualifier;
      boolean compacted;
      
      DP(final long stored_timestamp, final byte[] qualifier, 
          final boolean compacted) {
        this.stored_timestamp = stored_timestamp;
        this.qualifier = qualifier;
        this.compacted = compacted;
      }
    }
    
    int errors = 0;
    int correctable = 0;

    final short metric_width = Internal.metricWidth(tsdb);

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
      final Scanner scanner = Internal.getScanner(query);
      ArrayList<ArrayList<KeyValue>> rows;
      
      // store every data point for the row in here 
      final TreeMap<Long, ArrayList<DP>> previous = 
        new TreeMap<Long, ArrayList<DP>>();
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          rowcount++;
          previous.clear();
          
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
            byte[] value = kv.value();
            final byte[] qual = kv.qualifier();
            if (qual.length < 2) {
              errors++;
              LOG.error("Invalid qualifier, must be on 2 bytes or more.\n\t"
                        + kv);
              continue;
            } else if (qual.length % 2 != 0) {
              // likely an annotation or other object
              // TODO - validate annotations
              continue;
            } else if (qual.length >= 4 && !Internal.inMilliseconds(qual[0])) {
              // compacted row
              if (value[value.length - 1] > Const.MS_MIXED_COMPACT) {
                errors++;
                LOG.error("The last byte of a compacted should be 0 or 1. Either"
                          + " this value is corrupted or it was written by a"
                          + " future version of OpenTSDB.\n\t" + kv);
                continue;
              }
              
              // add every cell in the compacted column to the previously seen
              // data point tree so that we can scan for duplicate timestamps
              final ArrayList<Cell> cells = Internal.extractDataPoints(kv); 
              for (Cell cell : cells) {
                final long ts = cell.timestamp(base_time);
                ArrayList<DP> dps = previous.get(ts);
                if (dps == null) {
                  dps = new ArrayList<DP>(1);
                  previous.put(ts, dps);
                }
                dps.add(new DP(kv.timestamp(), kv.qualifier(), true));
              }
              
              // TODO - validate the compaction
              continue;
            } // else: qualifier is on 2 or 4 bytes, it's an individual value.

            final long timestamp = 
              Internal.getTimestampFromQualifier(qual, base_time);
            ArrayList<DP> dps = previous.get(timestamp);
            if (dps == null) {
              dps = new ArrayList<DP>(1);
              previous.put(timestamp, dps);
            }
            dps.add(new DP(kv.timestamp(), kv.qualifier(), false));
            
            if (value.length > 8) {
              errors++;
              LOG.error("Value more than 8 byte long with a 2-byte"
                        + " qualifier.\n\t" + kv);
            }
            // TODO(tsuna): Don't hardcode 0x8 / 0x3 here.
            if (qual.length == 2 && 
                Internal.getFlagsFromQualifier(qual) == (0x8 | 0x3)) {  // float | 4 bytes
              // The qualifier says the value is on 4 bytes, and the value is
              // on 8 bytes, then the 4 MSBs must be 0s.  Old versions of the
              // code were doing this.  It's kinda sad.  Some versions had a
              // bug whereby the value would be sign-extended, so we can
              // detect these values and fix them here.
              if (value.length == 8) {
                if (value[0] == -1 && value[1] == -1
                    && value[2] == -1 && value[3] == -1) {
                  errors++;
                  correctable++;
                  if (fix) {
                    value = value.clone();  // We're going to change it.
                    value[0] = value[1] = value[2] = value[3] = 0;
                    client.put(new PutRequest(table, kv.key(), kv.family(),
                                              qual, value));
                  } else {
                    LOG.error("Floating point value with 0xFF most significant"
                              + " bytes, probably caused by sign extension bug"
                              + " present in revisions [96908436..607256fc].\n"
                              + "\t" + kv);
                  }
                } else if (value[0] != 0 || value[1] != 0
                           || value[2] != 0 || value[3] != 0) {
                  errors++;
                }
              } else if (value.length != 4) {
                errors++;
                LOG.error("This floating point value must be encoded either on"
                          + " 4 or 8 bytes, but it's on " + value.length
                          + " bytes.\n\t" + kv);
              }
            }
          }

          // scan for dupes
          for (Map.Entry<Long, ArrayList<DP>> time_map : previous.entrySet()) {
            if (time_map.getValue().size() < 2) {
              continue;
            }
            
            // for now, delete the non-compacted dupes
            int compacted = 0;
            long earliest_value = Long.MAX_VALUE;
            for (DP dp : time_map.getValue()) {
              if (dp.compacted) {
                compacted++;
              }
              if (dp.stored_timestamp < earliest_value) {
                earliest_value = dp.stored_timestamp;
              }
            }
            
            // if there are more than one compacted columns with the same
            // timestamp, something went pear shaped and we need more work to
            // figure out what to do
            if (compacted > 1) {
              errors++;
              buf.setLength(0);
              buf.append("More than one compacted column had a value for the same timestamp: ")
                 .append("timestamp: (")
                 .append(time_map.getKey())
                 .append(")\n");
              for (DP dp : time_map.getValue()) {
                buf.append("    ")
                   .append(Arrays.toString(dp.qualifier))
                   .append("\n");
              }
              LOG.error(buf.toString());
            } else {
              errors++;
              correctable++;
              if (fix) {
                if (compacted < 1) {
                  // keep the earliest value
                  boolean matched = false;
                  for (DP dp : time_map.getValue()) {
                    if (dp.stored_timestamp == earliest_value && !matched) {
                      matched = true;
                      continue;
                    }
                    final DeleteOutOfOrder delooo = 
                      new DeleteOutOfOrder(row.get(0).key(), 
                          "t".getBytes(), dp.qualifier);
                    delooo.call(null);
                  }
                } else {
                  // keep the compacted value
                  for (DP dp : time_map.getValue()) {
                    if (dp.compacted) {
                      continue;
                    }
                    
                    final DeleteOutOfOrder delooo = 
                      new DeleteOutOfOrder(row.get(0).key(),
                          "t".getBytes(), dp.qualifier);
                    delooo.call(null);
                  }
                }
              } else {
                buf.setLength(0);
                buf.append("More than one column had a value for the same timestamp: ")
                   .append("timestamp: (")
                   .append(time_map.getKey())
                   .append(")\n");
                for (DP dp : time_map.getValue()) {
                  buf.append("    ")
                     .append(Arrays.toString(dp.qualifier))
                     .append("\n");
                }
                LOG.error(buf.toString());
              }
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
   * The last data point we've seen for a particular time series.
   */
  private static final class Seen {
    /** A 32-bit unsigned integer that holds a UNIX timestamp in milliseconds.  */
    private long timestamp;
    /** The raw data point (or points if the KV contains more than 1).  */
    KeyValue kv;

    private Seen(final long timestamp, final KeyValue kv) {
      this.timestamp = timestamp;
      this.kv = kv;
    }

    /** Returns the UNIX timestamp (in seconds) as a 32-bit unsigned int.  */
    public long timestamp() {
      return timestamp;
    }

    /** Updates the UNIX timestamp (in seconds) with a 32-bit unsigned int.  */
    public void setTimestamp(final long timestamp) {
      this.timestamp = timestamp;
    }
  }

}
