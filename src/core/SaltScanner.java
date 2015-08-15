// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.meta.Annotation;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * A class that handles coordinating the various scanners created for each 
 * salt bucket when salting is enabled. Each scanner stores it's results in 
 * local maps and once everyone has reported in, then the maps are parsed and
 * combined into a proper set of spans to return to the {@link TsdbQuery} class.
 * 
 * Note that if one or more of the scanners throws an exception, then that 
 * exception will be returned to the caller in the deferred. Unfortunately we
 * don't have a good way to cancel a scan in progress so the first scanner with
 * an error will store it, then we wait for all of the other scanners to
 * complete.
 * 
 * Concurrency is important in this class as the scanners are executing
 * asynchronously and can modify variables at any time.
 */
public class SaltScanner {
  private static final Logger LOG = LoggerFactory.getLogger(SaltScanner.class);
  
  /** This is a map that the caller must supply. We'll fill it with data.
   * WARNING: The salted row comparator should be applied to this map. */
  private final TreeMap<byte[], Span> spans;
  
  /** The list of pre-configured scanners. One scanner should be created per
   * salt bucket. */
  private final List<Scanner> scanners;
  
  /** Stores the compacted columns from each scanner as it completes. After all
   * scanners are done, we process this into the span map above. */
  private final Map<Integer, List<KeyValue>> kv_map = 
          new ConcurrentHashMap<Integer, List<KeyValue>>();
  
  /** Stores annotations from each scanner as it completes */
  private final Map<byte[], List<Annotation>> annotation_map = 
          Collections.synchronizedMap(
              new TreeMap<byte[], List<Annotation>>(new RowKey.SaltCmp()));
  
  /** A deferred to call with the spans on completion */
  private final Deferred<TreeMap<byte[], Span>> results = 
          new Deferred<TreeMap<byte[], Span>>();
  
  /** The metric this scanner set is dealing with. If a row comes in with a 
   * different metric we toss an exception. This shouldn't happen though. */
  private final byte[] metric;
  
  /** The TSDB to which we belong */
  private final TSDB tsdb;
  
  /** A counter used to determine how many scanners are still running */
  private AtomicInteger completed_tasks = new AtomicInteger();
  
  /** When the scanning started. We store the scan latency once all scanners
   * are done.*/
  private long start_time; // milliseconds.

  /** A list of filters to iterate over when processing rows */
  private final List<TagVFilter> filters;
  
  /** A holder for storing the first exception thrown by a scanner if something
   * goes pear shaped. Make sure to synchronize on this object when checking
   * for null or assigning from a scanner's callback. */
  private volatile Exception exception;
  
  /**
   * Default ctor that performs some validation. Call {@link scan} after 
   * construction to actually start fetching data.
   * @param tsdb The TSDB to which we belong
   * @param metric The metric we're expecting to fetch
   * @param scanners A list of HBase scanners, one for each bucket
   * @param spans The span map to store results in
   * @param filters A list of filters for processing
   * @throws IllegalArgumentException if any required data was missing or
   * we had invalid parameters.
   */
  public SaltScanner(final TSDB tsdb, final byte[] metric, 
                                      final List<Scanner> scanners, 
                                      final TreeMap<byte[], Span> spans,
                                      final List<TagVFilter> filters) {
    if (Const.SALT_WIDTH() < 1) {
      throw new IllegalArgumentException(
          "Salting is disabled. Use the regular scanner");
    }
    if (tsdb == null) {
      throw new IllegalArgumentException("The TSDB argument was null.");
    }
    if (spans == null) {
      throw new IllegalArgumentException("Span map cannot be null.");
    }
    if (!spans.isEmpty()) {
      throw new IllegalArgumentException("The span map should be empty.");
    }
    if (scanners == null || scanners.isEmpty()) {
      throw new IllegalArgumentException("Missing or empty scanners list. "
          + "Please provide a list of scanners for each salt.");
    }
    if (scanners.size() != Const.SALT_BUCKETS()) {
      throw new IllegalArgumentException("Not enough or too many scanners " + 
          scanners.size() + " when the salt bucket count is " + 
          Const.SALT_BUCKETS());
    }
    if (metric == null) {
      throw new IllegalArgumentException("The metric array was null.");
    }
    if (metric.length != TSDB.metrics_width()) {
      throw new IllegalArgumentException("The metric was too short. It must be " 
          + TSDB.metrics_width() + "bytes wide.");
    }
    
    this.scanners = scanners;
    this.spans = spans;
    this.metric = metric;
    this.tsdb = tsdb;
    this.filters = filters;
  }

  /**
   * Starts all of the scanners asynchronously and returns the data fetched
   * once all of the scanners have completed. Note that the result may be an
   * exception if one or more of the scanners encountered an exception. The 
   * first error will be returned, others will be logged. 
   * @return A deferred to wait on for results.
   */
  public Deferred<TreeMap<byte[], Span>> scan() {
    start_time = System.currentTimeMillis();
    for (final Scanner scanner: scanners) {
      new ScannerCB(scanner).scan();
    }
    return results; 
  }

  /**
   * Called once all of the scanners have reported back in to record our
   * latency and merge the results into the spans map. If there was an exception
   * stored then we'll return that instead.
   */
  private void mergeAndReturnResults() {
    final long hbase_time = System.currentTimeMillis();
    TsdbQuery.scanlatency.add((int)(hbase_time - start_time));
    long rows = 0;

    if (exception != null) {
      LOG.error("After all of the scanners finished, at "
          + "least one threw an exception", exception);
      results.callback(exception);
      return;
    }
    
    // Merge sorted spans together
    for (final List<KeyValue> kvs : kv_map.values()) {
      if (kvs == null || kvs.isEmpty()) {
        LOG.warn("Found a key value list that was null or empty");
        continue;
      }
      
      for (final KeyValue kv : kvs) {
        if (kv == null) {
          LOG.warn("Found a key value item that was null");
          continue;
        }
        if (kv.key() == null) {
          LOG.warn("A key for a kv was null");
          continue;
        }

        Span datapoints = spans.get(kv.key());
        if (datapoints == null) {
          datapoints = new Span(tsdb);
          spans.put(kv.key(), datapoints);
        }

        if (annotation_map.containsKey(kv.key())) {
          for (final Annotation note: annotation_map.get(kv.key())) {
            datapoints.getAnnotations().add(note);
          }
          annotation_map.remove(kv.key());
        }
        try {  
          datapoints.addRow(kv);
          rows++;
        } catch (RuntimeException e) {
          LOG.error("Exception adding row to span", e);
          throw e;
        }
      }
    }
     
    kv_map.clear();

    for (final byte[] key : annotation_map.keySet()) {
      Span datapoints = spans.get(key);
      if (datapoints == null) {
        datapoints = new Span(tsdb);
        spans.put(key, datapoints);
      }

      for (final Annotation note: annotation_map.get(key)) {
        datapoints.getAnnotations().add(note);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning completed in " + (hbase_time - start_time) + " ms, " +
            rows + " rows, and stored in " + spans.size() + " spans");
      LOG.debug("It took " + (System.currentTimeMillis() - hbase_time) + " ms, "
            + " to merge and sort the rows into a tree map");
    }

    results.callback(spans);
  }

  /**
  * Scanner callback executed recursively each time we get a set of data
  * from storage. This is responsible for determining what columns are
  * returned and issuing requests to load leaf objects.
  * When the scanner returns a null set of rows, the method initiates the
  * final callback.
  */
  final class ScannerCB implements Callback<Object, 
    ArrayList<ArrayList<KeyValue>>> {
    private final Scanner scanner;
    private final List<KeyValue> kvs = new ArrayList<KeyValue>();
    private final ByteMap<List<Annotation>> annotations = 
            new ByteMap<List<Annotation>>();
    private final Set<String> skips = new HashSet<String>();
    private final Set<String> keepers = new HashSet<String>();
    
    public ScannerCB(final Scanner scanner) {
      this.scanner = scanner;
    }
    
    /** Error callback that will capture an exception from AsyncHBase and store
     * it so we can bubble it up to the caller.
     */
    class ErrorCb implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        LOG.error("Scanner " + scanner + " threw an exception", e);
        scanner.close();
        handleException(e);
        return null;
      }
    }
    
    /**
    * Starts the scanner and is called recursively to fetch the next set of
    * rows from the scanner.
    * @return The map of spans if loaded successfully, null if no data was
    * found
    */
    public Object scan() {
      return scanner.nextRows().addCallback(this).addErrback(new ErrorCb());
    }

    /**
    * Iterate through each row of the scanner results, parses out data
    * points (and optional meta data).
    * @return null if no rows were found, otherwise the TreeMap with spans
    */
    @Override
    public Object call(final ArrayList<ArrayList<KeyValue>> rows) 
            throws Exception {
      try {
        if (rows == null) {
          scanner.close();
          validateAndTriggerCallback(kvs, annotations);
          return null;
        }

        // used for UID resolution if a filter is involved
        final List<Deferred<Object>> lookups = 
            filters != null && !filters.isEmpty() ? 
                new ArrayList<Deferred<Object>>(rows.size()) : null;
                
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key();
          if (RowKey.rowKeyContainsMetric(metric, key) != 0) {
            scanner.close();
            handleException(new IllegalDataException(
                   "HBase returned a row that doesn't match"
                   + " our scanner (" + scanner + ")! " + row + " does not start"
                   + " with " + Arrays.toString(metric) + " on scanner " + this));
            return null;
          }

          // If any filters have made it this far then we need to resolve
          // the row key UIDs to their names for string comparison. We'll
          // try to avoid the resolution with some sets but we may dupe
          // resolve a few times.
          // TODO - more efficient resolution
          // TODO - byte set instead of a string for the uid may be faster
          if (filters != null && !filters.isEmpty()) {
            lookups.clear();
            final String tsuid = 
                UniqueId.uidToString(UniqueId.getTSUIDFromKey(key, 
                TSDB.metrics_width(), Const.TIMESTAMP_BYTES));
            if (skips.contains(tsuid)) {
              continue;
            }
            if (!keepers.contains(tsuid)) {
              /** CB to called after all of the UIDs have been resolved */
              class MatchCB implements Callback<Object, ArrayList<Boolean>> {
                @Override
                public Object call(final ArrayList<Boolean> matches) 
                    throws Exception {
                  for (final boolean matched : matches) {
                    if (!matched) {
                      skips.add(tsuid);
                      return null;
                    }
                  }
                  // matched all, good data
                  keepers.add(tsuid);
                  processRow(key, row);
                  return null;
                }
              }

              /** Resolves all of the row key UIDs to their strings for filtering */
              class GetTagsCB implements
                  Callback<Deferred<ArrayList<Boolean>>, Map<String, String>> {
                @Override
                public Deferred<ArrayList<Boolean>> call(
                    final Map<String, String> tags) throws Exception {
                  final List<Deferred<Boolean>> matches =
                      new ArrayList<Deferred<Boolean>>(filters.size());

                  for (final TagVFilter filter : filters) {
                    matches.add(filter.match(tags));
                  }
                  
                  return Deferred.group(matches);
                }
              }
 
              lookups.add(Tags.getTagsAsync(tsdb, key)
                  .addCallbackDeferring(new GetTagsCB())
                  .addBoth(new MatchCB()));
            } else {
              processRow(key, row);
            }
          } else {
            processRow(key, row);
          }
        }
           
        // either we need to wait on the UID resolutions or we can go ahead
        // if we don't have filters.
        if (lookups != null && lookups.size() > 0) {
          class GroupCB implements Callback<Object, ArrayList<Object>> {
            @Override
            public Object call(final ArrayList<Object> group) throws Exception {
              return scan();
            }
          }
          return Deferred.group(lookups).addCallback(new GroupCB());
        } else {
          return scan();
        }
      } catch (final RuntimeException e) {
        LOG.error("Unexpected exception on scanner " + this, e);
        scanner.close();
        handleException(e);
        return null;
      }
    }
    
    /**
     * Finds or creates the span for this row, compacts it and stores it.
     * @param key The row key to use for fetching the span
     * @param row The row to add
     */
    void processRow(final byte[] key, final ArrayList<KeyValue> row) {
      List<Annotation> notes = annotations.get(key);
      if (notes == null) {
        notes = new ArrayList<Annotation>();
        annotations.put(key, notes);
      }

      final KeyValue compacted;
      // let IllegalDataExceptions bubble up so the handler above can close
      // the scanner
      compacted = tsdb.compact(row, notes);
      if (compacted != null) { // Can be null if we ignored all KVs.
        kvs.add(compacted);
      }
    }
  }
  
  /**
   * Called each time a scanner completes with valid or empty data.
   * @param kvs The compacted columns fetched by the scanner
   * @param annotations The annotations fetched by the scanners
   */
  private void validateAndTriggerCallback(final List<KeyValue> kvs, 
          final Map<byte[], List<Annotation>> annotations) {

    final int tasks = completed_tasks.incrementAndGet();
    if (kvs.size() > 0) {
      kv_map.put(tasks, kvs);
    }
    
    for (final byte[] key : annotations.keySet()) {
      final List<Annotation> notes = annotations.get(key);
      if (notes.size() > 0) {
        // Optimistic write, expecting unique row keys
        annotation_map.put(key, notes);
      }
    }
    
    if (tasks >= Const.SALT_BUCKETS()) {
      try {
        mergeAndReturnResults();
      } catch (final Exception ex) {
        results.callback(ex);
      }
    }
  }

  /**
   * If one or more of the scanners throws an exception then we should close it
   * and pass the exception here so that we can catch and return it to the
   * caller. If all of the scanners have finished, this will callback to the 
   * caller immediately.
   * @param e The exception to store.
   */
  private void handleException(final Exception e) {
    // make sure only one scanner can set the exception
    if (exception == null) {
      synchronized (this) {
        if (exception == null) {
          exception = e;
        } else {
          // TODO - it would be nice to close and cancel the other scanners but
          // for now we have to wait for them to finish and/or throw exceptions.
          LOG.error("Another scanner threw an exception", e);
        }
      }
    }
    
    final int tasks = completed_tasks.incrementAndGet();
    if (tasks >= Const.SALT_BUCKETS()) {
      results.callback(exception);
    }
  }
}
