// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.idconverter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryNode;

/**
 * An entry for a data source to store the resoltuion state and decoded IDs
 * 
 * TODO - make this a pooled object and re-use the maps.
 * 
 * @since 3.0
 */
public class ByteToStringConverterForSource {
  private static final Logger LOG = LoggerFactory.getLogger(
      ByteToStringConverterForSource.class);
  
  /** Reference to the converter to send PTSs back. */
  protected final ByteToStringIdConverter converter;
  
  /** The map of set wrappers to re-use. */
  protected final Map<Long, PartialTimeSeriesSet> sets;
  
  /** The map of outstanding resolvers. */
  protected final Map<Long, Resolver> resolvers;
  
  /**
   * Default ctor.
   * @param converter The non-null converter.
   */
  protected ByteToStringConverterForSource(
      final ByteToStringIdConverter converter) {
    this.converter = converter;
    sets = Maps.newConcurrentMap();;
    resolvers = Maps.newConcurrentMap();
  }
  
  /**
   * Called by the converter to resolve an ID.
   * @param pts The non-null PTs to process.
   */
  protected void resolve(final PartialTimeSeries pts) {
    if (converter.pipelineContext().hasId(pts.idHash(), Const.TS_STRING_ID)) {
      sendUp(pts);
      return;
    }
    
    Resolver resolver;
    resolver = resolvers.get(pts.idHash());
    if (resolver == null) {
      resolver = new Resolver();
      final Resolver extant = resolvers.putIfAbsent(pts.idHash(), resolver);
      if (extant != null) {
        resolver = extant;
      }
    }
    resolver.resolve(pts);
  }
  
  /**
   * Method that fetches and/or instantiates the set wrapper and sends the series
   * upstream.
   * @param pts The non-null PTs to send up.
   */
  void sendUp(final PartialTimeSeries pts) {
    PartialTimeSeriesSet set_wrapper;
    set_wrapper = sets.get(pts.set().start().epoch());
    if (set_wrapper == null) {
      set_wrapper = new WrappedPartialTimeSeriesSet(pts.set());
      final PartialTimeSeriesSet set = sets.putIfAbsent(
          pts.set().start().epoch(), set_wrapper);
      if (set != null) {
        set_wrapper = set;
      }
    }
    
    converter.sendUpstream(new WrappedPartialTimeSeries(pts, set_wrapper));
  }
  
  /**
   * An outstanding resolution that buffers series until we get the string ID.
   * This will only be in the map while resolving, then after we've put the 
   * ID in the decoded ID list we'll remove this so GC can clean it up.
   */
  class Resolver implements Callback<Object, Exception> {
    /** A CAS boolean to decide when we're done. */
    protected AtomicBoolean resolved;
    
    /** A list of series waiting on the resolution before sending upstream. */
    protected List<PartialTimeSeries> series;
    
    /** The deferred we're waiting on for resolution. */
    protected volatile Deferred<TimeSeriesStringId> deferred;
    
    /**
     * Default ctor.
     */
    protected Resolver() {
      resolved = new AtomicBoolean();
      series = Lists.newArrayList();
    }
    
    @Override
    public Object call(final Exception e) {
      LOG.error("Failed to resolve PTS", e);
      converter.onError(e);
      if (resolved.compareAndSet(false, true)) {
        deferred = null;
        // remove ourself
        if (series.size() > 0) {
          resolvers.remove(series.get(0).idHash());
        }
        series = null; // release it to the GC
      } // else we lost the race so another thread is working on array
      return null;
    }
    
    /**
     * The callback for resolution.
     */
    class ResolvedCB implements Callback<Void, TimeSeriesStringId> {
      final PartialTimeSeries pts;
      
      /**
       * Default ctor.
       * @param pts The non-null series.
       */
      protected ResolvedCB(final PartialTimeSeries pts) {
        this.pts = pts;
      }
      
      @Override
      public Void call(final TimeSeriesStringId id) throws Exception {
        converter.pipelineContext().addId(pts.idHash(), id);
        if (resolved.compareAndSet(false, true)) {
          for (final PartialTimeSeries pts : series) {     
            sendUp(pts);
          }
          series = null; // release it to the GC
          deferred = null;
          // remove ourself
          resolvers.remove(pts.idHash());
        } // else we lost the race so another thread is working on array
        return null;
      }
    }
    
    /**
     * Handles either starting a new resolution, queuing a PTS for an outstanding
     * resolution or sends the PTS upstream if the resolution is done.
     * @param pts The non-null PTS.
     */
    void resolve(final PartialTimeSeries pts) {
      if (resolved.get()) {
        sendUp(pts);
      } else {
        // double check locking so we can resolve once.
        synchronized (this) {
          if (resolved.get()) {
            sendUp(pts);
            return;
          }
          
          series.add(pts);
          if (deferred == null) {
            final TimeSeriesId id = converter.pipelineContext().getId(
                pts.idHash(), pts.idType());
            if (id == null) {
              // this should never happen.
              converter.onError(new QueryExecutionException("Failed to find an ID for " 
                  + pts.idHash() + " in set " + pts.set(), 500));
              return;
            }
            
            // start resolution. Otherwise we've added it to the list.
            deferred = ((TimeSeriesByteId) id).dataStore().resolveByteId(
                (TimeSeriesByteId) id, null /* TODO */);
            deferred.addCallback(new ResolvedCB(pts))
                    .addErrback(this);
          }
        }
      }
    }
    
  }
  
  /**
   * A wrapper around a time series that sets the source to redirect IDs to our
   * string ID map.
   */
  class WrappedPartialTimeSeries implements PartialTimeSeries<TimeSeriesDataType> {
    protected final PartialTimeSeries<TimeSeriesDataType> source;
    protected final PartialTimeSeriesSet set;
    
    WrappedPartialTimeSeries(final PartialTimeSeries<TimeSeriesDataType> source, 
                             final PartialTimeSeriesSet set) {
      this.source = source;
      this.set = set;
    }
    
    @Override
    public void close() throws Exception {
      source.close();
    }

    @Override
    public long idHash() {
      return source.idHash();
    }
    
    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public PartialTimeSeriesSet set() {
      return set;
    }
    
    @Override
    public TimeSeriesDataType value() {
      return source.value();
    }
    
  }
  
  /**
   * A class to wrap sets with references to our local decoded ID map.
   */
  class WrappedPartialTimeSeriesSet implements PartialTimeSeriesSet {
    final PartialTimeSeriesSet source;
    
    WrappedPartialTimeSeriesSet(final PartialTimeSeriesSet source) {
      this.source = source;
    }
    
    @Override
    public void close() throws Exception {
      source.close();
    }

    @Override
    public int totalSets() {
      return source.totalSets();
    }

    @Override
    public boolean complete() {
      return source.complete();
    }

    @Override
    public QueryNode node() {
      return source.node();
    }

    @Override
    public String dataSource() {
      return source.dataSource();
    }

    @Override
    public TimeStamp start() {
      return source.start();
    }

    @Override
    public TimeStamp end() {
      return source.end();
    }
    
    @Override
    public int timeSeriesCount() {
      return source.timeSeriesCount();
    }

    @Override
    public TimeSpecification timeSpecification() {
      return source.timeSpecification();
    }
    
  }
  
}
