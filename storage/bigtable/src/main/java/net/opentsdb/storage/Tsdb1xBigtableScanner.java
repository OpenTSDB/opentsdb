// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.BigtableExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.TSUID;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Exceptions;

/**
 * A single scanner for a single metric within a single salt bucket 
 * (optionally). 
 * <p>
 * While the most efficient scanner is one with a fully configured
 * start and stop key and not {@link Tsdb1xBigtableScanners#filterDuringScan()}, if 
 * filters are present, then it will resolve the UIDs of the rows into 
 * the string IDs, then filter them and cache the results in sets.
 * <p>
 * If {@link Tsdb1xScanners#sequenceEnd()} is reached or 
 * {@link Tsdb1xScanners#isFull()} is returned, then the scanner can stop
 * mid-result and buffer some data till {@link #fetchNext(Tsdb1xBigtableQueryResult, Span)}
 * is called again.
 * <p>
 * When resolving filters, it's possible to ignore UIDs that fail to
 * resolve to a name by setting the {@link #skip_nsui} flag.
 * 
 * @since 3.0
 */
public class Tsdb1xBigtableScanner {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xBigtableScanner.class);
  
  /** The scanner owner to report to. */
  private final Tsdb1xBigtableScanners owner;
  
  /** The actual Bigtable scanner to execute. */
  private final ResultScanner<FlatRow> scanner;
  
  /** The 0 based index amongst salt buckets. */
  private final int idx;
  
  /** An optional rollup interval. */
  private final RollupInterval rollup_interval;
  
  /** The current state of this scanner. */
  private State state;
  
  /** When filtering, used to hold the TSUIDs being resolved. */
  protected TLongObjectMap<ResolvingId> keys_to_ids;
  
  /** The set of TSUID hashes that we have resolved and have failed our
   * filter set. */
  protected TLongSet skips;
  
  /** The set of TSUID hashes that we have resolved and matched our filter
   * set. */
  protected TLongSet keepers;
  
  /** A buffer for storing data when we either reach a segment end or 
   * have filled up the result set. Calls to 
   * {@link #fetchNext(Tsdb1xBigtableQueryResult, Span)} will process this list
   * before moving on to the scanner. */
  protected List<FlatRow> row_buffer;
  
  /** A singleton base timestamp for this scanner. */
  protected TimeStamp base_ts;
  
  protected int rows_scanned = 0;
  
  /**
   * Default ctor.
   * @param owner A non-null owner with configuration and reporting.
   * @param scanner A non-null HBase scanner to work with.
   * @param idx A zero based index when multiple salt scanners are in
   * use.
   * @throws IllegalArgumentException if the owner or scanner was null.
   */
  public Tsdb1xBigtableScanner(final Tsdb1xBigtableScanners owner, 
                               final ResultScanner<FlatRow> scanner, 
                               final int idx,
                               final RollupInterval rollup_interval) {
    if (owner == null) {
      throw new IllegalArgumentException("Owner cannot be null.");
    }
    if (scanner == null) {
      throw new IllegalArgumentException("Scanner cannot be null.");
    }
    this.owner = owner;
    this.scanner = scanner;
    this.idx = idx;
    this.rollup_interval = rollup_interval;
    state = State.CONTINUE;
    base_ts = new MillisecondTimeStamp(0);
    
    if (owner.filterDuringScan()) {
      keys_to_ids = new TLongObjectHashMap<ResolvingId>();
      skips = new TLongHashSet();
      keepers = new TLongHashSet();
    }
  }
  
  /**
   * Called by the {@link Tsdb1xScanners} to initiate the next fetch of
   * data from the buffer and/or scanner.
   * 
   * @param result A non-null result set to decode the columns we find.
   * @param span An optional tracing span.
   */
  public void fetchNext(final Tsdb1xBigtableQueryResult result, final Span span) {
    if (owner.hasException()) {
      try {
        scanner.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing scanner due to upstream result exception.");
      }
      state = State.COMPLETE;
      owner.scannerDone();
      return;
    }
    
    if (result.isFull()) {
      if (owner.node().pipelineContext().queryContext().mode() == 
          QueryMode.SINGLE) {
        state = State.EXCEPTION;
        owner.exception(new QueryExecutionException(
            result.resultIsFullErrorMessage(),
            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE.code()));
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pausing scanner as upstream is full.");
      }
      owner.scannerDone();
      return;
    }
    
    if (row_buffer != null) {
      if (owner.filterDuringScan()) {
        processBufferWithFilter(result, span);
      } else {
        processBuffer(result, span);
      }
    } else {
      // try for some data from HBase
      final Span child;
      if (span != null && span.isDebug()) {
        child = span.newChild(getClass().getName() + ".fetchNext_" + idx)
            .start();
      } else {
        child = span;
      }
      
      scan(result, child);
    }
  }
  
  /**
   * Called by {@link #fetchNext(Tsdb1xBigtableQueryResult, Span)} to process a 
   * non-null buffer without a scanner filter. Will continue scanning if
   * we haven't hit a segment end.
   * 
   * @param result The non-null result set to decode the columns we find.
   * @param span An optional tracing span.
   */
  private void processBuffer(final Tsdb1xBigtableQueryResult result, final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".processBuffer_" + idx)
          .start();
    } else {
      child = span;
    }
    
    try {
      // copy so we can delete and create a new one if necessary
      final List<FlatRow> row_buffer = this.row_buffer;
      this.row_buffer = null;
      final Iterator<FlatRow> it = row_buffer.iterator();
      while (it.hasNext()) {
        final FlatRow row = it.next();
        owner.node().schema().baseTimestamp(row.getRowKey().toByteArray(), base_ts);
        if (result.isFull() || owner.node().sequenceEnd() != null && 
            base_ts.compare(
                /*(scanner.isReversed() ? Op.LT : */Op.GT/*)*/, 
                  owner.node().sequenceEnd())) {
          // end of sequence encountered in the buffer. Push on up
          if (LOG.isDebugEnabled()) {
            LOG.debug("Hit next sequence end while in the scanner cache.");
          }
          this.row_buffer = row_buffer;
          if (child != null) {
            child.setSuccessTags()
                 .finish();
          }
          owner.scannerDone();
          return;
        }
        
        it.remove();
        result.decode(row, rollup_interval);
      }
      
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
             .log("Exception", e)
             .finish();
      }
      owner.exception(e);
      state = State.EXCEPTION;
      try {
        scanner.close();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      return;
    }
    
    if (child != null) {
      child.setSuccessTags()
           .finish();
    }
    // all good, keep going with the scanner now.
    scan(result, span);
  }
  
  /**
   * Called by {@link #fetchNext(Tsdb1xBigtableQueryResult, Span)} to process a 
   * non-null buffer with a scanner filter. Will continue scanning if
   * we haven't hit a segment end.
   * 
   * @param result The non-null result set to decode the columns we find.
   * @param span An optional tracing span.
   */
  void processBufferWithFilter(final Tsdb1xBigtableQueryResult result, final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + 
            ".processBufferWithFilter_" + idx)
          .start();
    } else {
      child = span;
    }
    
    try {
      // copy so we can delete and create a new one if necessary
      final List<FlatRow> row_buffer = this.row_buffer;
      this.row_buffer = null;
      
      final List<Deferred<Object>> deferreds = 
          Lists.newArrayListWithCapacity(row_buffer.size());
      final Iterator<FlatRow> it = row_buffer.iterator();
      
      /** Executed after all of the resolutions are complete. */
      class FilterGroupResolutionCB implements Callback<Object, ArrayList<Object>> {
        final boolean keep_going;
        
        FilterGroupResolutionCB(final boolean keep_going) {
          this.keep_going = keep_going;
        }
        
        @Override
        public Object call(final ArrayList<Object> ignored) throws Exception {
          if (child != null) {
            child.setSuccessTags()
                 .finish();
          }
          
          keys_to_ids.clear();
          if (owner.hasException()) {
            owner.scannerDone();
            state = State.COMPLETE;
          } else if (!result.isFull() && keep_going) {
            scan(result, span);
          } else {
            // told not to keep going.
            owner.scannerDone();
          }
          return null;
        }
      }
      
      boolean keep_going = true;
      synchronized (this) {
        while (it.hasNext()) {
          final FlatRow row = it.next();
          
          owner.node().schema().baseTimestamp(row.getRowKey().toByteArray(), base_ts);
          if (owner.node().sequenceEnd() != null && 
              base_ts.compare(
                  /*(scanner.isReversed() ? Op.LT : */Op.GT/*)*/, 
                      owner.node().sequenceEnd())) {
            // end of sequence encountered in the buffer. Push on up
            if (LOG.isDebugEnabled()) {
              LOG.debug("Hit next sequence end while in the scanner cache.");
            }
            this.row_buffer = row_buffer;
            keep_going = false;
            break;
          } else if (result.isFull()) {
            if (owner.node().pipelineContext().queryContext().mode() == 
                  QueryMode.SINGLE) {
              throw new QueryExecutionException(
                  result.resultIsFullErrorMessage(),
                  413);
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Owner is full while in the scanner cache.");
            }
            this.row_buffer = row_buffer;
            keep_going = false;
            break;
          }
          
          it.remove();
          
          final byte[] tsuid = owner.node().schema().getTSUID(row.getRowKey().toByteArray());
          deferreds.add(resolveAndFilter(tsuid, row, result, child));
        }
      }
      
      Deferred.group(deferreds)
        .addCallbacks(new FilterGroupResolutionCB(keep_going), new ErrorCB(child));
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
             .log("Exception", e)
             .finish();
      }
      owner.exception(e);
      state = State.EXCEPTION;
      try {
        scanner.close();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      return;
    }
  }

  /**
   * The method that walks the results from our scanner.
   * @param result The non-null results to populate.
   * @param span An optional tracing span.
   */
  protected void scan(final Tsdb1xBigtableQueryResult result, final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getName() + ".scan")
                  .start();
    } else {
      child = null;
    }
    
    while (true) {
      try {
        final FlatRow[] rows = scanner.next(owner.rowsPerScan());
        if (rows == null || rows.length < 1) {
          break;
        }
        
        rows_scanned += rows.length;
        if (owner.filterDuringScan()) {
          final List<Deferred<Object>> deferreds = 
              Lists.newArrayListWithCapacity(rows.length);
          boolean keep_going = true;
          for (int i = 0; i < rows.length; i++) {
            final FlatRow row = rows[i];
            if (row.getCells().isEmpty()) {
              // should never happen
              if (LOG.isDebugEnabled()) {
                LOG.debug("Received an empty row from result set: " + rows);
              }
              continue;
            }
            
            owner.node().schema().baseTimestamp(
                row.getRowKey().toByteArray(), base_ts);
            if (owner.node().sequenceEnd() != null && 
                base_ts.compare(
                    /*(scanner.isReversed() ? Op.LT : */Op.GT/*)*/, 
                        owner.node().sequenceEnd())) {
              // end of sequence encountered in the buffer. Push on up
              if (LOG.isDebugEnabled()) {
                LOG.debug("Hit next sequence end in the scanner. "
                    + "Buffering results and returning.");
              }
              buffer(i, rows, false);
              keep_going = false;
              break;
            } else if (result.isFull()) {
              if (owner.node().pipelineContext().queryContext().mode() == 
                  QueryMode.SINGLE) {
                throw new QueryExecutionException(
                    result.resultIsFullErrorMessage(), 413);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Owner is full while in the scanner cache.");
              }
              buffer(i, rows, false);
              keep_going = false;
              break;
            }
            
            final byte[] tsuid = owner.node().schema().getTSUID(
                row.getRowKey().toByteArray());
            deferreds.add(resolveAndFilter(tsuid, row, result, child));
          }
          
          Deferred.group(deferreds)
              .addCallbacks(new GroupResolutionCB(result, keep_going, child), 
                  new ErrorCB(child));
          return;
        } else {
          // load all
          for (int i = 0; i < rows.length; i++) {
            final FlatRow row = rows[i];
            if (row.getCells().isEmpty()) {
              // should never happen
              if (LOG.isDebugEnabled()) {
                LOG.debug("Received an empty row from result set: " + rows);
              }
              continue;
            }
            
            owner.node().schema().baseTimestamp(
                row.getRowKey().toByteArray(), base_ts);
            if ((owner.node().sequenceEnd() != null && 
                base_ts.compare(
                    /*(scanner.isReversed() ? Op.LT : */Op.GT/*)*/, 
                        owner.node().sequenceEnd()))) {
              
              // end of sequence encountered in the buffer. Push on up
              if (LOG.isDebugEnabled()) {
                LOG.debug("Hit next sequence end in the scanner. "
                    + "Buffering results and returning.");
              }
              buffer(i, rows, true);
              return;
            } else if (result.isFull()) {
              if (owner.node().pipelineContext().queryContext().mode() == 
                  QueryMode.SINGLE) {
                throw new QueryExecutionException(
                    result.resultIsFullErrorMessage(), 413);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Owner is full. Buffering results and returning.");
              }
              buffer(i, rows, true);
              return;
            }
            
            result.decode(row, rollup_interval);
          }
        }
        
        if (owner.hasException()) {
          complete(child, rows.length);
          return;
        } else if (!result.isFull()) {
          // keep going!
          if (child != null) {
            child.setSuccessTags()
                 .setTag("rows", rows.length)
                 .setTag("buffered", row_buffer == null ? 0 : row_buffer.size())
                 .finish();
          }
          // next set
          continue;
        } else if (owner.node().pipelineContext().queryContext().mode() == 
              QueryMode.SINGLE) {
          throw new QueryExecutionException(
              result.resultIsFullErrorMessage(),413);
        }
        owner.scannerDone();
        return;
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        complete(e, child, 0);
        return;
      }
    }
    
    // finished the while loop.
    complete(child, 0);
  }
    
  /**
   * Marks the scanner as complete, closing it and reporting to the owner
   * @param child An optional tracing span.
   * @param rows The number of rows found in this result set.
   */
  void complete(final Span child, final int rows) {
    complete(null, child, rows);
  }
  
  /**
   * Marks the scanner as complete, closing it and reporting to the owner
   * @param e An exception, may be null. If not null, calls 
   * {@link Tsdb1xScanners#exception(Throwable)}
   * @param child An optional tracing span.
   * @param rows The number of rows found in this result set.
   */
  void complete(final Exception e, final Span child, final int rows) {
    if (e != null) {
      if (child != null) {
        child.setErrorTags(e)
             .finish();
      }
//        if (span != null) {
//          span.setErrorTags(e)
//              .finish();
//        }
      state = State.EXCEPTION;
      owner.exception(e);
    } else {
      if (child != null) {
        child.setSuccessTags()
             .setTag("rows", rows)
             .setTag("buffered", row_buffer == null ? 0 : row_buffer.size())
             .finish();
      }
//        if (span != null) {
//          span.setSuccessTags()
//              .setTag("totalRows", rows_scanned)
//              .setTag("buffered", row_buffer == null ? 0 : row_buffer.size())
//              .finish();
//        }
      state = State.COMPLETE;
      owner.scannerDone();
    }
    try {
      scanner.close();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } // TODO - attach a callback for logging in case
    // something goes pear shaped.
    clear();
  }
  
  /** Called when the filter resolution is complete. */
  class GroupResolutionCB implements Callback<Object, ArrayList<Object>> {
      final Tsdb1xBigtableQueryResult result;
      final boolean keep_going;
      final Span child;
      
      GroupResolutionCB(final Tsdb1xBigtableQueryResult result, 
                        final boolean keep_going, 
                        final Span span) {
        this.result = result;
        this.keep_going = keep_going;
        this.child = span;
      }
      
      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        keys_to_ids.clear();
        if (owner.hasException()) {
          complete(child, 0);
        } else if (!result.isFull() && keep_going) {
          scan(result, child);
          return null;
        } else if (result.isFull() && 
            owner.node().pipelineContext().queryContext().mode() == 
              QueryMode.SINGLE) {
          complete(new QueryExecutionException(
              result.resultIsFullErrorMessage(), 413), child, 0);
        } else {
          // told not to keep going.
          owner.scannerDone();
        }
        return null;
      }
    }
  
  /**
   * Writes the remaining rows to the buffer.
   * @param i A zero based offset in the rows array to buffer.
   * @param rows The non-null rows list.
   * @param mark_scanner_done Whether or not to call {@link Tsdb1xScanners#scannerDone()}.
   */
  private void buffer(int i, 
                      final FlatRow[] rows, 
                      final boolean mark_scanner_done) {
    if (row_buffer == null) {
      row_buffer = Lists.newArrayListWithCapacity(rows.length - i);
    }
    for (; i < rows.length; i++) {
      row_buffer.add(rows[i]);
      TimeStamp t = new MillisecondTimeStamp(0);
      owner.node().schema().baseTimestamp(rows[i].getRowKey().toByteArray(), t);
    }
    if (mark_scanner_done) {
      owner.scannerDone();
    }
  }
  
  /** @return The state of this scanner. */
  State state() {
    return state;
  }
  
  /** Closes the scanner. */
  void close() {
    try {
      scanner.close();
    } catch (Exception e) {
      LOG.error("Failed to close scanner", e);
    }
    clear();
  }
  
  /** Clears the filter map and sets when the scanner is done so GC can
   * clean up quicker. */
  private void clear() {
    if (keys_to_ids != null) {
      keys_to_ids.clear();
    }
    if (skips != null) {
      skips.clear();
    }
    if (keepers != null) {
      keepers.clear();
    }
  }
  
  /** The error back used to catch all kinds of exceptions. Closes out 
   * everything after passing the exception to the owner. */
  final class ErrorCB implements Callback<Object, Exception> {
    final Span span;
    
    ErrorCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Object call(final Exception ex) throws Exception {
      LOG.error("Unexpected exception", 
          (ex instanceof DeferredGroupException ? 
              Exceptions.getCause((DeferredGroupException) ex) : ex));
      state = State.EXCEPTION;
      owner.exception((ex instanceof DeferredGroupException ? 
          Exceptions.getCause((DeferredGroupException) ex) : ex));
      scanner.close();
      clear();
      return null;
    }
  }

  @VisibleForTesting
  List<FlatRow> buffer() {
    return row_buffer;
  }

  /**
   * Evaluates a row against the skips, keepers and may resolve it if
   * necessary when we have to go through filters that couldn't be sent
   * to HBase.
   * 
   * @param tsuid A non-null TSUID.
   * @param row A non-null row to process.
   * @param result A non-null result to store successful matches into.
   * @param span An optional tracing span.
   * @return A deferred to wait on before starting the next fetch.
   */
  final Deferred<Object> resolveAndFilter(final byte[] tsuid, 
                                          final FlatRow row, 
                                          final Tsdb1xBigtableQueryResult result, 
                                          final Span span) {
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    synchronized (skips) {
      if (skips.contains(hash)) {
        // discard
        // TODO - counters
        return Deferred.fromResult(null);
      }
    }
    
    synchronized (keepers) {
      if (keepers.contains(hash)) {
        result.decode(row, rollup_interval);
        return Deferred.fromResult(null);
      }
    }
    
    ResolvingId id = keys_to_ids.get(hash);
    if (id == null) {
      ResolvingId new_id = new ResolvingId(tsuid, hash);
      final ResolvingId extant = keys_to_ids.putIfAbsent(hash, new_id);
      if (extant == null) {
        // start resolution of the tags to strings, then filter
        return new_id.decode(span)
            .addCallback(new ResolvedCB(row, result));
      } else {
        // add it
        return extant.deferred.addCallback(new ResolvedCB(row, result));
      }
    } else {
      return id.deferred.addCallback(new ResolvedCB(row, result));
    }
  }
  
  /** Simple class for rows waiting on resolution. */
  class ResolvedCB implements Callback<Object, Boolean> {
    private final FlatRow row;
    private final Tsdb1xBigtableQueryResult result;
    
    ResolvedCB(final FlatRow row, final Tsdb1xBigtableQueryResult result) {
      this.row = row;
      this.result = result;
    }
    
    @Override
    public Object call(final Boolean matched) throws Exception {
      if (matched != null && matched) {
        result.decode(row, rollup_interval);
      }
      return null;
    }
    
  }
  
  /**
   * An override of the {@link TSUID} class that holds a reference to the
   * resolution deferred so others rows with different timestamps but the
   * same TSUID can wait for a single result to be resolved.
   * <p>
   * <b>NOTE:</b> Do not call {@link TSUID#decode(boolean, Span)} directly!
   * Instead call {@link ResolvingId#decode(Span)}.
   * <p>
   * <b>NOTE:</b> If skip_nsui was set to true, this will return a false
   * for any rows that didn't resolve properly. If set to true, then this
   * will return a {@link NoSuchUniqueId} exception.
   */
  private class ResolvingId extends TSUID implements Callback<Void, TimeSeriesStringId> {
    /** The computed hash of the TSUID. */
    private final long hash;
    
    /** The resolution deferred for others to wait on. */
    private Deferred<Boolean> deferred;
    
    /** A child tracing span. */
    private Span child;
    
    /**
     * Default ctor.
     * @param tsuid A non-null TSUID.
     * @param hash The computed hash of the TSUID.
     */
    public ResolvingId(final byte[] tsuid, final long hash) {
      super(tsuid, owner.node().schema());
      this.hash = hash;
      deferred = new Deferred<Boolean>();
    }

    /**
     * Starts decoding the TSUID into a string and returns the deferred 
     * for other TSUIDs to wait on.
     * 
     * @param span An optional tracing span.
     * @return A deferred resolving to true if the TSUID passed all of
     * the scan filters, false if not. Or an exception if something went
     * pear shaped.
     */
    Deferred<Boolean> decode(final Span span) {
      if (span != null && span.isDebug()) {
        child = span.newChild(getClass().getName() + "_" + idx)
            .start();
      } else {
        child = span;
      }
      decode(false, child)
          .addCallback(this)
          .addErrback(new ErrorCB(null));
      return deferred;
    }
    
    @Override
    public Void call(final TimeSeriesStringId id) throws Exception {
      final Span grand_child;
      if (child != null && child.isDebug()) {
        grand_child = child.newChild(getClass().getName() + ".call_" + idx)
            .start();
      } else {
        grand_child = child;
      }
      if (FilterUtils.matchesTags(
          ((QuerySourceConfig) owner.node().config()).getFilter(), id.tags())) {
        synchronized (keepers) {
          keepers.add(hash);
        }
        if (grand_child != null) {
          grand_child.setSuccessTags()
                     .setTag("resolved", "true")
                     .setTag("matched", "true")
                     .finish();
        }
        if (child != null) {
          child.setSuccessTags()
               .setTag("resolved", "true")
               .setTag("matched", "true")
               .finish();
        }
        deferred.callback(true);
      } else {
        synchronized (skips) {
          skips.add(hash);
        }
        if (grand_child != null) {
          grand_child.setSuccessTags()
                     .setTag("resolved", "true")
                     .setTag("matched", "false")
                     .finish();
        }
        if (child != null) {
          child.setSuccessTags()
               .setTag("resolved", "true")
               .setTag("matched", "false")
               .finish();
        }
        deferred.callback(false);
      }
      return null;
    }
    
    class ErrorCB implements Callback<Void, Exception> {
      final Span grand_child;
      
      ErrorCB(final Span grand_child) {
        this.grand_child = grand_child;
      }
      
      @Override
      public Void call(final Exception ex) throws Exception {
        if (ex instanceof NoSuchUniqueId && owner.node().skipNSUI()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Row contained a bad UID: " + Bytes.pretty(tsuid) 
              + " " + ex.getMessage());
          }
          synchronized (skips) {
            skips.add(hash);
          }
          if (grand_child != null) {
            grand_child.setSuccessTags()
                       .setTag("resolved", "false")
                       .finish();
          }
          if (child != null) {
            child.setSuccessTags()
                 .setTag("resolved", "false")
                 .finish();
          }
          deferred.callback(false);
          return null;
        }
        if (grand_child != null) {
          grand_child.setErrorTags(ex)
                     .setTag("resolved", "false")
                     .finish();
        }
        if (child != null) {
          child.setErrorTags(ex)
               .setTag("resolved", "false")
               .finish();
        }
        deferred.callback(ex);
        return null;
      }
    }
    
  }
}