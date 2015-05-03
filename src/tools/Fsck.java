// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import net.opentsdb.core.Const;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

/**
 * Tool to look for and fix corrupted data in a TSDB. FSCK can be used to
 * recover space, resolve duplicate data points, remove orphaned time series and
 * remove data errors. If one or more command line queries are provided, only
 * rows matching the query will be FSCK'd. Alternatively a full table scan can
 * be performed.
 * <p>
 * Scanning is done in three stages: 
 * 1) Each row key is parsed to make sure it's a valid OpenTSDB row. If it isn't
 * then the user can decide to delete it. If one or more UIDs cannot be resolved
 * to names (metric or tags) then the user can decide to purge it.
 * 2) All key value pairs in a row are parsed to determine the type of object. 
 * If it's a single data point, it's added to a tree map based on the data point 
 * timestamp. If it's a compacted column, the data points are exploded and 
 * added to the data point map. If it's some other object it may be purged if 
 * told to, or if it's a known type (e.g. annotations) simply ignored.
 * 3) If any data points were found, we iterate over each one looking for
 * duplicates, malformed encodings or potential value-length-encoding savings.
 * At the end, if told to, FSCK will fix up the values and optionally write a
 * new compacted cell, deleting all of the old values.
 * <p>
 * A number of metrics are tracked during the run and a report will be dumped
 * to the log at the end.
 * <p>
 * When iterating over the datapoints in step 3, the workers will usually compile
 * a set of compacted qualifiers and values so that at the end, if necessary, a
 * new compacted cell can be written and the old cells purged.
 * <p>
 * Note: some fields are package private so that we can easily unit test.
 */
final class Fsck {
  private static final Logger LOG = LoggerFactory.getLogger(Fsck.class);
  
  /** The TSDB to use for access */
  private final TSDB tsdb; 

  /** Options to use while iterating over rows */
  private final FsckOptions options;
  
  /** Counters incremented during processing. They have to be atomic counters
   * as we may be running multiple fsck threads. */
  final AtomicLong kvs_processed = new AtomicLong();
  final AtomicLong rows_processed = new AtomicLong();
  final AtomicLong valid_datapoints = new AtomicLong();
  final AtomicLong annotations = new AtomicLong();
  final AtomicLong bad_key = new AtomicLong();
  final AtomicLong bad_key_fixed = new AtomicLong();
  final AtomicLong duplicates = new AtomicLong();
  final AtomicLong duplicates_fixed = new AtomicLong();
  final AtomicLong duplicates_fixed_comp = new AtomicLong();
  final AtomicLong orphans = new AtomicLong();
  final AtomicLong orphans_fixed = new AtomicLong();
  final AtomicLong future = new AtomicLong();
  final AtomicLong unknown = new AtomicLong();
  final AtomicLong unknown_fixed = new AtomicLong();
  final AtomicLong bad_values = new AtomicLong();
  final AtomicLong bad_values_deleted = new AtomicLong();
  final AtomicLong value_encoding = new AtomicLong();
  final AtomicLong value_encoding_fixed = new AtomicLong();
  final AtomicLong fixable_compacted_columns = new AtomicLong();
  final AtomicLong bad_compacted_columns = new AtomicLong();
  final AtomicLong bad_compacted_columns_deleted = new AtomicLong();
  final AtomicLong vle = new AtomicLong();
  final AtomicLong vle_bytes = new AtomicLong();
  final AtomicLong vle_fixed = new AtomicLong();
  
  /** Length of the metric + timestamp for key validation */
  private static int key_prefix_length = Const.SALT_WIDTH() + 
      TSDB.metrics_width() + Const.TIMESTAMP_BYTES;
  
  /** Length of a tagk + tagv pair for key validation */
  private static int key_tags_length = TSDB.tagk_width() + TSDB.tagv_width();
  
  /** How often to report progress */
  private static long report_rows = 10000;
  
  /**
   * Default Ctor
   * @param tsdb The TSDB to use for access
   * @param options Options to use when iterating over rows
   */
  public Fsck(final TSDB tsdb, final FsckOptions options) {
    this.tsdb = tsdb;
    this.options = options;
  }
  
  /**
   * Fetches the max metric ID and splits the data table up amongst threads on
   * a naive split. By default we execute cores * 2 threads but the user can
   * specify more or fewer.
   * @throws Exception If something goes pear shaped.
   */
  public void runFullTable() throws Exception {
    LOG.info("Starting full table scan");
    final long start_time = System.currentTimeMillis() / 1000;
    final int workers = options.threads() > 0 ? options.threads() :
      Runtime.getRuntime().availableProcessors() * 2;
    
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, workers);
    LOG.info("Spooling up [" + scanners.size() + "] worker threads");
    final List<Thread> threads = new ArrayList<Thread>(scanners.size());
    int i = 0;
    for (final Scanner scanner : scanners) {
      final FsckWorker worker = new FsckWorker(scanner, i++);
      worker.setName("Fsck #" + i);
      worker.start();
      threads.add(worker);
    }

    final Thread reporter = new ProgressReporter();
    reporter.start();
    for (final Thread thread : threads) {
      thread.join();
      LOG.info("Thread [" + thread + "] Finished");
    }
    reporter.interrupt();
    
    logResults();
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed fsck in [" + duration + "] seconds");
  }
  
  /**
   * Scans the rows matching one or more standard queries. An aggregator is still
   * required though it's ignored.
   * @param queries The queries to execute
   * @throws Exception If something goes pear shaped.
   */
  public void runQueries(final List<Query> queries) throws Exception {
    final long start_time = System.currentTimeMillis() / 1000;
    
    // TODO - threadify it. We *could* have hundreds of queries and we don't 
    // want to create that many threads. For now we'll just execute each one
    // serially
    final Thread reporter = new ProgressReporter();
    reporter.start();
    
    for (final Query query : queries) {
      final List<Scanner> scanners = Internal.getScanners(query);
      final List<Thread> threads = new ArrayList<Thread>(scanners.size());
      int i = 0;
      for (final Scanner scanner : scanners) {
        final FsckWorker worker = new FsckWorker(scanner, i++);
        worker.setName("Fsck #" + i);
        worker.start();
        threads.add(worker);
      }

      for (final Thread thread : threads) {
        thread.join();
        LOG.info("Thread [" + thread + "] Finished");
      }
    }
    reporter.interrupt();
    
    logResults();
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed fsck in [" + duration + "] seconds");
  }
  
  /** @return The total number of errors detected during the run */
  long totalErrors() {
    return bad_key.get() + duplicates.get() + orphans.get() + unknown.get() +
        bad_values.get() + bad_compacted_columns.get() + 
        fixable_compacted_columns.get() + value_encoding.get();
  }
  
  /** @return The total number of errors fixed during the run */
  long totalFixed() {
    return bad_key_fixed.get() + duplicates_fixed.get() + orphans_fixed.get() +
        unknown_fixed.get() + value_encoding_fixed.get() + 
        bad_values_deleted.get();
  }
  
  /** @return The total number of errors that could be (or may have been) fixed */
  long correctable() {
    return bad_key.get() + duplicates.get() + orphans.get() + unknown.get() +
        bad_values.get() + bad_compacted_columns.get() + 
        fixable_compacted_columns.get() + value_encoding.get();
  }
  
  /**
   * A worker thread that takes a query or a chunk of the main data table and 
   * performs the actual FSCK process.
   */
  final class FsckWorker extends Thread {
    /** Id of the thread this worker belongs to */
    final int thread_id;
    /** Optional query to execute instead of a full table scan */
    final Query query;
    /** The scanner to use for iterating over a chunk of the table */
    final Scanner scanner;
    /** Set of TSUIDs this worker has seen. Used to avoid UID resolution for
     * previously processed row keys */
    final Set<String> tsuids = new HashSet<String>();
    
    /** Shared flags and values for compiling a compacted column */
    byte[] compact_qualifier = null;
    int qualifier_index = 0;
    byte[] compact_value = null;
    int value_index = 0;
    boolean compact_row = false;
    int qualifier_bytes = 0;
    int value_bytes = 0;
    
    /**
     * Ctor for running a worker on a chunk of the data table
     * @param scanner The scanner to use for iterationg
     * @param thread_id Id of the thread this worker is assigned for logging
     */
    FsckWorker(final Scanner scanner, final int thread_id) {
      this.scanner = scanner;
      this.thread_id = thread_id;
      query = null;
    }
    
    /**
     * Determines the type of scanner to use, i.e. a specific query scanner or
     * for a portion of the whole table. It then performs the actual scan, 
     * compiling a list of data points and fixing/compacting them when 
     * appropriate.
     */
    public void run() {
      // store every data point for the row in here 
      final TreeMap<Long, ArrayList<DP>> datapoints = 
        new TreeMap<Long, ArrayList<DP>>();
      byte[] last_key = null;
      ArrayList<ArrayList<KeyValue>> rows;
      
      try {
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          // keep in mind that with annotations and millisecond values, a row
          // can now have more than 4069 key values, the default for a scanner.
          // Since we don't know how many values may actually be in a row, we 
          // don't want to set the KV limit too high. Instead we'll just keep
          // working through the sets until we hit a different row key, then
          // process all of the data points. It puts more of a burden on fsck
          // memory but we should be able to keep ~3M data points in memory
          // without a problem.
          for (final ArrayList<KeyValue> row : rows) {
            if (last_key != null && Bytes.memcmp(row.get(0).key(), last_key) != 0) {
              // new row so flush the old one
              rows_processed.getAndIncrement();
              if (!datapoints.isEmpty()) {
                compact_qualifier = new byte[qualifier_bytes];
                compact_value = new byte[value_bytes+1];
                fsckDataPoints(datapoints);
                resetCompaction();
                datapoints.clear();
              }
            }
            last_key = row.get(0).key();
            fsckRow(row, datapoints);
          }
        }
        
        // handle the last row
        if (!datapoints.isEmpty()) {
          rows_processed.getAndIncrement();
          compact_qualifier = new byte[qualifier_bytes];
          compact_value = new byte[value_bytes+1];
          fsckDataPoints(datapoints);
        }
      } catch (Exception e) {
        LOG.error("Shouldn't be here", e);
      }
    }
    
    /**
     * Parses the row of KeyValues. First it validates the row key, then parses
     * each KeyValue to determine what kind of object it is. Data points are 
     * stored in the tree map and non-data point columns are handled per the
     * option flags
     * @param row The row of data to parse
     * @param datapoints The map of datapoints to append to.
     * @throws Exception If something goes pear shaped.
     */
    private void fsckRow(final ArrayList<KeyValue> row, 
        final TreeMap<Long, ArrayList<DP>> datapoints) throws Exception {
      // The data table should contain only rows with a metric, timestamp and
      // one or more tag pairs. Future version may use different prefixes or 
      // key formats but for now, we can safely delete any rows with invalid 
      // keys. This may check the same row key multiple times but that's good
      // as it will keep the data points from being pushed to the dp map
      if (!fsckKey(row.get(0).key())) {
        return;
      }
      
      final long base_time = Bytes.getUnsignedInt(row.get(0).key(), 
          Const.SALT_WIDTH() + TSDB.metrics_width());
      
      for (final KeyValue kv : row) {
        kvs_processed.getAndIncrement();
        // these are not final as they may be modified when fixing is enabled
        byte[] value = kv.value(); 
        byte[] qual = kv.qualifier();
        
        // all qualifiers must be at least 2 bytes long, i.e. a single data point
        if (qual.length < 2) {
          unknown.getAndIncrement();
          LOG.error("Invalid qualifier, must be on 2 bytes or more.\n\t" + kv);
          if (options.fix() && options.deleteUnknownColumns()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
            tsdb.getClient().delete(delete);
            unknown_fixed.getAndIncrement();
          }
          continue;
        }
        
        // All data point columns have an even number of bytes, so if we find
        // one that has an odd length, it could be an OpenTSDB object or it 
        // could be junk that made it into the table.
        if (qual.length % 2 != 0) {
          // If this test fails, the column is not a TSDB object such as an 
          // annotation or blob. Future versions may be able to compact TSDB 
          // objects so that their qualifier would be of a different length, but
          // for now we'll consider it an error.
          if (qual.length != 3 && qual.length != 5) {
            unknown.getAndIncrement();
            LOG.error("Unknown qualifier, must be 2, 3, 5 or an even number " +
                "of bytes.\n\t" + kv);
            if (options.fix() && options.deleteUnknownColumns()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              tsdb.getClient().delete(delete);
              unknown_fixed.getAndIncrement();
            }
            continue;
          }
          
          // TODO - create a list of TSDB objects and fsck them. Maybe a plugin
          // or interface.
          // TODO - perform validation of the annotation
          if (qual[0] == Annotation.PREFIX()) {
            annotations.getAndIncrement();
            continue;
          }
          LOG.warn("Found an object possibly from a future version of OpenTSDB\n\t"
              + kv);
          future.getAndIncrement();
          continue;
        }
        
        // This is (hopefully) a compacted column with multiple data points. It 
        // could have two points with second qualifiers or multiple points with
        // a mix of second and millisecond qualifiers
        if (qual.length == 4 && !Internal.inMilliseconds(qual[0])
            || qual.length > 4) {
          if (value[value.length - 1] > Const.MS_MIXED_COMPACT) {
            // TODO - figure out a way to fix these. Maybe lookup a row before 
            // or after and try parsing this for values. If the values are
            // somewhat close to the others, then we could just set the last
            // byte. Otherwise it could be a bad compaction and we'd need to 
            // toss it.
            bad_compacted_columns.getAndIncrement();
            LOG.error("The last byte of a compacted should be 0 or 1. Either"
                      + " this value is corrupted or it was written by a"
                      + " future version of OpenTSDB.\n\t" + kv);
            continue;
          }
          
          // add every cell in the compacted column to the data point tree so 
          // that we can scan for duplicate timestamps
          try {
            final ArrayList<Cell> cells = Internal.extractDataPoints(kv);
            
            // the extractDataPoints() method will automatically fix up some
            // issues such as setting proper lengths on floats and sorting the
            // cells to be in order. Rather than reproduce the extraction code or
            // add another method, we can just recompile the compacted qualifier
            // as we run through it. If the new one is different (indicating a fix)
            // then we'll replace it later on.
            final byte[] recompacted_qualifier = new byte[kv.qualifier().length];
            int qualifier_index = 0;
            for (final Cell cell : cells) {
              final long ts = cell.timestamp(base_time);

              ArrayList<DP> dps = datapoints.get(ts);
              if (dps == null) {
                dps = new ArrayList<DP>(1);
                datapoints.put(ts, dps);
              }
              dps.add(new DP(kv, cell));
              qualifier_bytes += cell.qualifier().length;
              value_bytes += cell.value().length;
              System.arraycopy(cell.qualifier(), 0, recompacted_qualifier, 
                  qualifier_index, cell.qualifier().length);
              qualifier_index += cell.qualifier().length;
            }
            
            if (Bytes.memcmp(recompacted_qualifier, kv.qualifier()) != 0) {
              LOG.error("Compacted column was out of order or requires a "
                  + "fixup: " + kv);
              fixable_compacted_columns.getAndIncrement();
            }
            compact_row = true;
          } catch (IllegalDataException e) {
            bad_compacted_columns.getAndIncrement();
            LOG.error(e.getMessage());
            if (options.fix() && options.deleteBadCompacts()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              tsdb.getClient().delete(delete);
              bad_compacted_columns_deleted.getAndIncrement();
            }
          }
          continue;
        }
        
        // at this point we *should* be dealing with a single data point encoded 
        // in seconds or milliseconds.
        final long timestamp = 
            Internal.getTimestampFromQualifier(qual, base_time);
        ArrayList<DP> dps = datapoints.get(timestamp);
        if (dps == null) {
          dps = new ArrayList<DP>(1);
          datapoints.put(timestamp, dps);
        }
        dps.add(new DP(kv));
        qualifier_bytes += kv.qualifier().length;
        value_bytes += kv.value().length;
      }
    }

    /**
     * Validates the row key. It must match the format 
     * {@code <metric><timestamp><tagpair>[...<tagpair>]}. If it doesn't, then
     * the row is considered an error. If the UIDs in a row key do not resolve
     * to a name, then the row is considered an orphan and the values contained
     * therein are NOT fsck'd. Also, if the TSUID in the row key has been seen
     * before, then we don't re-resolve the UIDs. Saves a bit of CPU time.
     * NOTE: We do not currently validate the timestamp in the row key. This
     * would be a good TODO.
     * NOTE: Global annotations are of the format {@code <metric=0><timestamp>}
     * but fsck will not scan over those rows. Full table scans start at metric
     * 1 and queries must match a valid name.
     * @param key The row key to validate
     * @return True if the row key is valid, false if it is not
     * @throws Exception If something goes pear shaped.
     */
    private boolean fsckKey(final byte[] key) throws Exception {
      if (key.length < key_prefix_length || 
          (key.length - key_prefix_length) % key_tags_length != 0) {
        LOG.error("Invalid row key.\n\tKey: " + UniqueId.uidToString(key));
        bad_key.getAndIncrement();
        
        if (options.fix() && options.deleteBadRows()) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
          tsdb.getClient().delete(delete);
          bad_key_fixed.getAndIncrement();
        }
        return false;
      }
      
      // Process the time series ID by resolving the UIDs to names if we haven't
      // already seen this particular TSUID. Note that getTSUID accounts for salt
      final byte[] tsuid = UniqueId.getTSUIDFromKey(key, TSDB.metrics_width(), 
          Const.TIMESTAMP_BYTES);
      if (!tsuids.contains(tsuid)) {
        try {
          RowKey.metricNameAsync(tsdb, key).joinUninterruptibly();
        } catch (NoSuchUniqueId nsui) {
          LOG.error("Unable to resolve the metric from the row key.\n\tKey: "
              + UniqueId.uidToString(key) + "\n\t" + nsui.getMessage());
          orphans.getAndIncrement();
          
          if (options.fix() && options.deleteOrphans()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
            tsdb.getClient().delete(delete);
            orphans_fixed.getAndIncrement();
          }
          return false;
        }
        
        try {
          Tags.resolveIds(tsdb, (ArrayList<byte[]>)
              UniqueId.getTagPairsFromTSUID(tsuid));
        } catch (NoSuchUniqueId nsui) {
          LOG.error("Unable to resolve the a tagk or tagv from the row key.\n\tKey: "
              + UniqueId.uidToString(key) + "\n\t" + nsui.getMessage());
          orphans.getAndIncrement();
          
          if (options.fix() && options.deleteOrphans()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
            tsdb.getClient().delete(delete);
            orphans_fixed.getAndIncrement();
          }
          return false;
        }
      }
      return true;
    }

    /**
     * Processes each data point parsed from the row. Validates the qualifiers
     * and values, fixing what it can and deleting those it can't. At the end
     * it may write a new compacted column and remove the others. Also handles
     * duplicate data point resolution.
     * @param datapoints The list of data points parsed from the row
     * @throws Exception If something goes pear shaped.
     */
    private void fsckDataPoints(final Map<Long, ArrayList<DP>> datapoints) 
        throws Exception {

      // store a unique set of qualifier/value columns to help us later when
      // we need to delete or update the row
      final ByteMap<byte[]> unique_columns = new ByteMap<byte[]>();
      byte[] key = null;
      boolean has_seconds = false;
      boolean has_milliseconds = false;
      boolean has_duplicates = false;
      boolean has_uncorrected_value_error = false;
      
      for (final Map.Entry<Long, ArrayList<DP>> time_map : datapoints.entrySet()) {
        if (key == null) {
          key = time_map.getValue().get(0).kv.key();
        }
        
        if (time_map.getValue().size() < 2) {
          // there was only one data point for this timestamp, no conflicts
          final DP dp = time_map.getValue().get(0);
          valid_datapoints.getAndIncrement();
          has_uncorrected_value_error |= Internal.isFloat(dp.qualifier()) ?
              fsckFloat(dp) : fsckInteger(dp);
          if (Internal.inMilliseconds(dp.qualifier())) {
            has_milliseconds = true;
          } else {
            has_seconds = true;
          }
          unique_columns.put(dp.kv.qualifier(), dp.kv.value());
          continue;
        }

        // sort so we can figure out which one we're going to keep, i.e. oldest
        // or newest
        Collections.sort(time_map.getValue());       
        has_duplicates = true;
        // We want to keep either the first or the last incoming datapoint 
        // and ignore delete the middle.

        final StringBuilder buf = new StringBuilder();
        buf.append("More than one column had a value for the same timestamp: ")
           .append("(")
           .append(time_map.getKey())
           .append(" - ")
           .append(new Date(time_map.getKey()))
           .append(")\n    row key: (")
           .append(UniqueId.uidToString(key))
           .append(")\n");

        int num_dupes = time_map.getValue().size();

        final int delete_range_start;
        final int delete_range_stop;
        final DP dp_to_keep;
        if (options.lastWriteWins()) {
          // Save the latest datapoint from extinction.
          delete_range_start = 0;
          delete_range_stop = num_dupes - 1;
          dp_to_keep = time_map.getValue().get(num_dupes - 1);
        } else {
          // Save the oldest datapoint from extinction.
          delete_range_start = 1;
          delete_range_stop = num_dupes;
          dp_to_keep = time_map.getValue().get(0);
          appendDatapointInfo(buf, dp_to_keep, " <--- keep oldest").append("\n");
        }

        unique_columns.put(dp_to_keep.kv.qualifier(), dp_to_keep.kv.value());
        valid_datapoints.getAndIncrement();
        has_uncorrected_value_error |= Internal.isFloat(dp_to_keep.qualifier()) ?
            fsckFloat(dp_to_keep) : fsckInteger(dp_to_keep);

        if (Internal.inMilliseconds(dp_to_keep.qualifier())) {
          has_milliseconds = true;
        } else {
          has_seconds = true;
        }

        for (int dp_index = delete_range_start; dp_index < delete_range_stop; 
            dp_index++) {
          duplicates.getAndIncrement();
          DP dp = time_map.getValue().get(dp_index);
          final byte flags = (byte)Internal.getFlagsFromQualifier(dp.kv.qualifier());
          buf.append("    ")
            .append("write time: (")
            .append(dp.kv.timestamp())
            .append(" - ")
            .append(new Date(dp.kv.timestamp()))
            .append(") ")
            .append(" compacted: (")
            .append(dp.compacted)
            .append(")  qualifier: ")
            .append(Arrays.toString(dp.kv.qualifier()))
            .append(" value: ")
            .append(Internal.isFloat(dp.kv.qualifier()) ?
              Internal.extractFloatingPointValue(dp.value(), 0, flags) :
              Internal.extractIntegerValue(dp.value(), 0, flags))
            .append("\n");
          unique_columns.put(dp.kv.qualifier(), dp.kv.value());
          if (options.fix() && options.resolveDupes()) {
            if (compact_row) {
              // Scheduled for deletion by compaction.
              duplicates_fixed_comp.getAndIncrement();
            } else if (!dp.compacted) {
              LOG.debug("Removing duplicate data point: " + dp.kv);
              tsdb.getClient().delete(
                new DeleteRequest(
                  tsdb.dataTable(), dp.kv.key(), dp.kv.family(), dp.qualifier()
                )
              );
              duplicates_fixed.getAndIncrement();
            }
          }
        }
        if (options.lastWriteWins()) {
          appendDatapointInfo(buf, dp_to_keep, " <--- keep latest").append("\n");
        }
        LOG.info(buf.toString());
      }
      
      // if an error was found in this row that was not marked for repair, then
      // we should bail at this point and not write a new compacted column.
      if ((has_duplicates && !options.resolveDupes()) || 
          (has_uncorrected_value_error && !options.deleteBadValues())) {
        LOG.warn("One or more errors found in row that were not marked for repair");
        return;
      }
      
      if ((options.compact() || compact_row) && options.fix() 
          && qualifier_index > 0) {
        if (qualifier_index == 2 || (qualifier_index == 4 && 
            Internal.inMilliseconds(compact_qualifier))) {
          // we may have deleted all but one value from the row and that one 
          // value may have a different qualifier than it originally had. We
          // can't write a compacted column with a single data point as the length
          // will be off due to the flag at the end. Therefore we just rollback
          // the length of the value array.
          value_index--;
        } else if (has_seconds && has_milliseconds) {
          // set mixed compact flag at end of the values array
          compact_value[value_index] = 1;
        }
        value_index++;
        final byte[] new_qualifier = Arrays.copyOfRange(compact_qualifier, 0, 
            qualifier_index);
        final byte[] new_value = Arrays.copyOfRange(compact_value, 0, 
            value_index);
        final PutRequest put = new PutRequest(tsdb.dataTable(), key, 
            TSDB.FAMILY(), new_qualifier, new_value);
        
        // it's *possible* that the hash of our new compacted qualifier is in
        // the delete list so double check. 
        if (unique_columns.containsKey(new_qualifier)) {
          LOG.info("Our qualifier was in the delete list!!!");
          if (Bytes.memcmp(unique_columns.get(new_qualifier), new_value) != 0) {
            // Important: Make sure to wait for the write to complete before
            // proceeding with the deletes.
            tsdb.getClient().put(put).joinUninterruptibly();
          }
          unique_columns.remove(new_qualifier);
        } else {
          // Important: Make sure to wait for the write to complete before
          // proceeding with the deletes.
          tsdb.getClient().put(put).joinUninterruptibly();
        }
        
        for (byte[] qualifier : unique_columns.keySet()) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key, 
              TSDB.FAMILY(), qualifier);
          tsdb.getClient().delete(delete);
        }
        duplicates_fixed.getAndAdd(duplicates_fixed_comp.longValue());
        duplicates_fixed_comp.set(0);
      }
    }
    
    /**
     * Handles validating a floating point value. Floats must be encoded on 4 
     * bytes for a Float and 8 bytes for a Double. The qualifier is compared to
     * the actual length in the case of single data points. In previous versions
     * of OpenTSDB, the qualifier flag may have been on 4 bytes but the actual
     * value on 8. This method will fix those issues as well as an old bug
     * where the first 4 bytes of the 8 byte value were sign-extended.
     * @param dp The data point to process
     * @return True if value was NOT fixed so the caller can avoid compacting.
     * If false, then the value was good or it was repaired.
     * @throws Exception If something goes pear shaped
     */
    private boolean fsckFloat(final DP dp) throws Exception {
      byte[] qual = dp.qualifier();
      byte[] value = dp.value();
      final byte length = Internal.getValueLengthFromQualifier(qual);
      // The qualifier says the value is on 4 bytes, and the value is
      // on 8 bytes, then the 4 MSBs must be 0s.  Old versions of the
      // code were doing this.  It's kinda sad.  Some versions had a
      // bug whereby the value would be sign-extended, so we can
      // detect these values and fix them here.
      if (length == 4 && value.length == 8) {
        if (value[0] == -1 && value[1] == -1
            && value[2] == -1 && value[3] == -1 && qual.length == 2) {
          value_encoding.getAndIncrement();
          LOG.error("Floating point value with 0xFF most significant"
              + " bytes, probably caused by sign extension bug"
              + " present in revisions [96908436..607256fc].\n"
              + "\t" + dp.kv);
          if (options.fix()) {
            final float value_as_float =
                Float.intBitsToFloat(Bytes.getInt(value, 4));
            value = Bytes.fromInt(
                Float.floatToRawIntBits((float)value_as_float));
            if (compact_row || options.compact()) {
              appendDP(qual, value, 4);
            } else if (!dp.compacted){
              final PutRequest put = new PutRequest(tsdb.dataTable(), 
                  dp.kv.key(), dp.kv.family(), qual, value);
              tsdb.getClient().put(put);
            } else {
              LOG.error("SHOULDN'T be here as we didn't compact or fix a "
                  + "single value");
            }
            value_encoding_fixed.getAndIncrement();
          } else {
            return true;
          }
        } else if (value[0] != 0 || value[1] != 0
                   || value[2] != 0 || value[3] != 0) {
          // can't happen if it was compacted
          LOG.error("Floating point value was marked as 4 bytes long but"
              + " was actually 8 bytes long and the first four bytes were"
              + " not zeroed\n\t" + dp);
          bad_values.getAndIncrement();
          if (options.fix() && options.deleteBadValues() && !dp.compacted) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), 
                dp.kv);
            tsdb.getClient().delete(delete);
            bad_values_deleted.getAndIncrement();
          } else if (dp.compacted) {
            LOG.error("The value was in a compacted column. This should "
                + "not be possible\n\t" + dp);
            bad_compacted_columns.getAndIncrement();
            return true;
          } else {
            return true;
          }
        } else {
          // can't happen if it was compacted
          LOG.warn("Floating point value was marked as 4 bytes long but"
              + " was actually 8 bytes long\n\t" + dp.kv);
          value_encoding.getAndIncrement();
          if (options.fix() && !dp.compacted) {
            final float value_as_float =
                Float.intBitsToFloat(Bytes.getInt(value, 4));
            value = Bytes.fromInt(
                Float.floatToRawIntBits((float)value_as_float));
            if (compact_row || options.compact()) {
              appendDP(qual, value, 4);
            } else if (!dp.compacted) {
              final PutRequest put = new PutRequest(tsdb.dataTable(), 
                  dp.kv.key(), dp.kv.family(), qual, value);
              tsdb.getClient().put(put);
            } else {
              LOG.error("SHOULDN'T be here as we didn't compact or fix a single value");
            }
            value_encoding_fixed.getAndIncrement();
          } else {
            return true;
          }
        }
      } else if (length == 8 && value.length == 4) {
        // could be a marked as a Double but actually encoded as a Float. BUT we
        // don't know that and can't parse it accurately so tank it
        bad_values.getAndIncrement();
        LOG.error("This floating point value was marked as 8 bytes long but"
                  + " was only " + value.length + " bytes.\n\t" + dp.kv);
        if (options.fix() && options.deleteBadValues() && !dp.compacted) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), dp.kv);
          tsdb.getClient().delete(delete);
          bad_values_deleted.getAndIncrement();
        } else if (dp.compacted) {
          LOG.error("The previous value was in a compacted column. This should "
              + "not be possible.");
          bad_compacted_columns.getAndIncrement();
        } else {
          return true;
        }
      } else if (value.length != 4 && value.length != 8) {
        bad_values.getAndIncrement();
        LOG.error("This floating point value must be encoded either on"
                  + " 4 or 8 bytes, but it's on " + value.length
                  + " bytes.\n\t" + dp.kv);
        if (options.fix() && options.deleteBadValues() && !dp.compacted) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), dp.kv);
          tsdb.getClient().delete(delete);
          bad_values_deleted.getAndIncrement();
        } else if (dp.compacted) {
          LOG.error("The previous value was in a compacted column. This should "
              + "not be possible.");
          bad_compacted_columns.getAndIncrement();
          return true;
        } else {
          return true;
        }
      }
      return false;
    }
    
    /**
     * Handles validating an integer value. Integers must be encoded on 1, 2, 4
     * or 8 bytes. Older versions of OpenTSDB wrote all integers on 8 bytes 
     * regardless of value. If the --fix flag is specified, this method will
     * attempt to re-encode small values to save space (up to 7 bytes!!). It also
     * makes sure the value length matches the length specified in the qualifier
     * @param dp The data point to process
     * @return True if value was NOT fixed so the caller can avoid compacting.
     * If false, then the value was good or it was repaired.
     * @throws Exception If something goes pear shaped
     */
    private boolean fsckInteger(final DP dp) throws Exception {
      byte[] qual = dp.qualifier();
      byte[] value = dp.value();
      
      // this should be a single integer value. Check the encoding to make
      // sure it's the proper length, and if the flag is set to fix encoding
      // we can save space with VLE.
      final byte length = Internal.getValueLengthFromQualifier(qual);

      if (value.length != length) {
        // can't happen in a compacted column
        bad_values.getAndIncrement();
        LOG.error("The integer value is " + value.length + " bytes long but "
            + "should be " + length + " bytes.\n\t" + dp.kv);
        if (options.fix() && options.deleteBadValues()) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), dp.kv);
          tsdb.getClient().delete(delete);
          bad_values_deleted.getAndIncrement();
        } else if (dp.compacted) {
          LOG.error("The previous value was in a compacted column. This should "
              + "not be possible.");
          bad_compacted_columns.getAndIncrement();
        } else {
          return true;
        }
        return false;
      }
      
      // OpenTSDB had support for VLE decoding of integers but only wrote
      // on 8 bytes originally. Lets see how much space we could save. 
      // We'll assume that a length other than 8 bytes is already VLE'd
      if (length == 8) {
        final long decoded = Bytes.getLong(value);
        if (Byte.MIN_VALUE <= decoded && decoded <= Byte.MAX_VALUE) {
          vle.getAndIncrement();
          vle_bytes.addAndGet(7);
          value = new byte[] { (byte) decoded };
        } else if (Short.MIN_VALUE <= decoded && decoded <= Short.MAX_VALUE) {
          vle.getAndIncrement();
          vle_bytes.addAndGet(6);
          value = Bytes.fromShort((short) decoded);
        } else if (Integer.MIN_VALUE <= decoded && 
            decoded <= Integer.MAX_VALUE) {
          vle.getAndIncrement();
          vle_bytes.addAndGet(4);
          value = Bytes.fromInt((int) decoded);
        } // else it needs 8 bytes, it's on 8 bytes, yipee

        if (length != value.length && options.fix()) {
          final byte[] new_qualifier = Arrays.copyOf(qual, qual.length);
          new_qualifier[new_qualifier.length - 1] &= 0xF0 | (value.length - 1);
          if (compact_row || options.compact()) {
            appendDP(new_qualifier, value, value.length);
          } else {
            // put the new value, THEN delete the old
            final PutRequest put = new PutRequest(tsdb.dataTable(), 
                dp.kv.key(), dp.kv.family(), new_qualifier, value);
            tsdb.getClient().put(put).joinUninterruptibly();
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), 
                dp.kv.key(), dp.kv.family(), qual);
            tsdb.getClient().delete(delete);
          }
          vle_fixed.getAndIncrement();
        } // don't return true here as we don't consider a VLE an error.
      } else {
        if (compact_row || options.compact()) {
          appendDP(qual, value, value.length);
        }
      }
      return false;
    }

    /**
     * Appends the given value to the running qualifier and value compaction 
     * byte arrays. It doesn't take a {@code DP} as we may be changing the
     * arrays before they're re-written.
     * @param new_qual The qualifier to append
     * @param new_value The value to append
     * @param value_length How much of the value to append
     */
    private void appendDP(final byte[] new_qual, final byte[] new_value, 
        final int value_length) {
      System.arraycopy(new_qual, 0, compact_qualifier, qualifier_index, new_qual.length);
      qualifier_index += new_qual.length;
      System.arraycopy(new_value, 0, compact_value, value_index, value_length);
      value_index += value_length;  
    }
    
    /**
     * Appends a representation of a datapoint to a string buffer
     * @param buf The buffer to modify
     * @param msg An optional message to append
     */
    private StringBuilder appendDatapointInfo(final StringBuilder buf, 
        final DP dp, final String msg) {
      buf.append("    ")
        .append("write time: (")
        .append(dp.kv.timestamp())
        .append(") ")
        .append(" compacted: (")
        .append(dp.compacted)
        .append(")  qualifier: ")
        .append(Arrays.toString(dp.kv.qualifier()))
        .append(msg);
      return buf;
    }

    /**
     * Resets the running compaction variables. This should be called AFTER a 
     * {@link fsckDataPoints()} has been run and before the next row of values
     * is processed. Note that we may overallocate some memory when creating
     * the arrays.
     */
    private void resetCompaction() {
      compact_qualifier = null;
      qualifier_index = 0;
      compact_value = null;
      value_index = 0;
      qualifier_bytes = 0;
      value_bytes = 0;
      compact_row = false;
    }

    /**
     * Internal class used for storing references to values during row parsing.
     * The object will hold onto the key value where the value was found as well
     * as the actual qualifier/value if the data point was compacted. It also
     * sorts on the actual HBase write timestamp so we can resolve duplicates
     * using the earliest or latest value.
     */
    final class DP implements Comparable<DP> {
      /** The KeyValue where this data point was found. May be a compacted column */
      KeyValue kv;
      /** Whether or not the value was in a compacted column */
      boolean compacted;
      /** The specific data point qualifier/value if the data point was compacted */
      Cell cell;
      
      /**
       * Default Ctor used for a single data point
       * @param kv The column where the value appeared.
       */
      DP(final KeyValue kv) {
        this.kv = kv;
        compacted = false;
      }
      
      /**
       * Overload for a compacted data point
       * @param kv The column where the value appeared.
       * @param cell The exploded data point
       */
      DP(final KeyValue kv, final Cell cell) {
        this.kv = kv;
        this.cell = cell;
        compacted = true;
      }
      
      /**
       * Compares data points.
       * @param dp The data point to compare to
       * @return 0 if the HBase write timestamps are the same, -1 if the local
       * object was written BEFORE the other data point, 1 if it was written
       * later.
       */
      public int compareTo(final DP dp) {
        if (kv.timestamp() == dp.kv.timestamp()) {
          return 0;
        } 
        return kv.timestamp() < dp.kv.timestamp() ? -1 : 1;
      }
      
      /** @return The qualifier of the data point (from the compaction or column) */
      public byte[] qualifier() {
        return compacted ? cell.qualifier() : kv.qualifier();
      }
      
      /** @return The value of the data point */
      public byte[] value() {
        return compacted ? cell.value() : kv.value();
      }
      
      /** @return The cell or key value string */
      public String toString() {
        return compacted ? cell.toString() : kv.toString();
      }
    }
  }
  
  /**
   * Silly little class to report the progress while fscking
   */
  final class ProgressReporter extends Thread {
    ProgressReporter() {
      super("Progress");
    }
    public void run() {
      long last_progress = 0;
      while(true) {
        try {
          long processed_rows = rows_processed.get();
          processed_rows = (processed_rows - (processed_rows % report_rows));
          if (processed_rows - last_progress >= report_rows) {
            last_progress = processed_rows;
            LOG.info("Processed " + processed_rows + " rows, " + 
                valid_datapoints.get() + " valid datapoints");   
          }
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: fsck"
        + " [flags] [START-DATE [END-DATE] query [queries...]] \n"
        + "Scans the OpenTSDB data table for errors. Use the --full-scan flag\n"
        + "to scan the entire data table or specify a command line query to "
        + "scan a subset.\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.\n"
        + "The --fix or --fix-all flags will attempt to fix errors,"
        + " but be careful when using them.\n");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  /**
   * Helper to dump the atomic counters to the log after a completed FSCK
   */
  private void logResults() {
    LOG.info("Key Values Processed: " + kvs_processed.get());
    LOG.info("Rows Processed: " + rows_processed.get());
    LOG.info("Valid Datapoints: " + valid_datapoints.get());
    LOG.info("Annotations: " + annotations.get());
    LOG.info("Invalid Row Keys Found: " + bad_key.get());
    LOG.info("Invalid Rows Deleted: " + bad_key_fixed.get());
    LOG.info("Duplicate Datapoints: " + duplicates.get());
    LOG.info("Duplicate Datapoints Resolved: " + duplicates_fixed.get());
    LOG.info("Orphaned UID Rows: " + orphans.get());
    LOG.info("Orphaned UID Rows Deleted: " + orphans_fixed.get());
    LOG.info("Possible Future Objects: " + future.get());
    LOG.info("Unknown Objects: " + unknown.get());
    LOG.info("Unknown Objects Deleted: " + unknown_fixed.get());
    LOG.info("Unparseable Datapoint Values: " + bad_values.get());
    LOG.info("Unparseable Datapoint Values Deleted: " + bad_values_deleted.get());
    LOG.info("Improperly Encoded Floating Point Values: " + value_encoding.get());
    LOG.info("Improperly Encoded Floating Point Values Fixed: " + 
        value_encoding_fixed.get());
    LOG.info("Unparseable Compacted Columns: " + bad_compacted_columns.get());
    LOG.info("Unparseable Compacted Columns Deleted: " + 
        bad_compacted_columns_deleted.get());
    LOG.info("Datapoints Qualified for VLE : " + vle.get());
    LOG.info("Datapoints Compressed with VLE: " + vle_fixed.get());
    LOG.info("Bytes Saved with VLE: " + vle_bytes.get());  
    LOG.info("Total Errors: " + totalErrors());
    LOG.info("Total Correctable Errors: " + correctable());
    LOG.info("Total Errors Fixed: " + totalFixed());
  }
  
  /**
   * The main class executed from the "tsdb" script
   * @param args Command line arguments to parse
   * @throws Exception If something goes pear shaped
   */
  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    argp.addOption("--help", "Print help information.");
    CliOptions.addCommon(argp);
    FsckOptions.addDataOptions(argp);
    args = CliOptions.parse(argp, args);

    if (argp.has("--help")) {
      usage(argp, "", 0);
    }

    Config config = CliOptions.getConfig(argp);
    final FsckOptions options = new FsckOptions(argp, config);
    final TSDB tsdb = new TSDB(config);
    final ArrayList<Query> queries = new ArrayList<Query>();
    if (args != null && args.length > 0) {
      CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);
    }
    if (queries.isEmpty() && !argp.has("--full-scan")) {
      usage(argp, "Must supply a query or use the '--full-scan' flag", 1);
    }
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
     
    argp = null;
    final Fsck fsck = new Fsck(tsdb, options);
    try {
      if (!queries.isEmpty()) {
        fsck.runQueries(queries);
      } else {
        fsck.runFullTable();
      }
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
    System.exit(fsck.totalErrors() == 0 ? 0 : 1);
  }
}
