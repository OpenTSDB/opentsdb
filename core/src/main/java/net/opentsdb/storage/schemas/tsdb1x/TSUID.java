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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.Exceptions;

/**
 * A simple implementation of the TimeSeriesByteId that is instantiated
 * from a 1x schema TSUID.
 * 
 * @since 3.0
 */
public class TSUID implements TimeSeriesByteId {
  /** The TSUID. */
  protected final byte[] tsuid;
  
  /** The schema. */
  protected final Schema schema;
  
  /** The lazily initialized cache of tag mappings. */
  protected ByteMap<byte[]> tags;
  
  /** The set of UIDs, lazily initialized. Just the TSUID. */
  protected ByteSet uids;
  
  /** The cached ID when decoded. */
  protected TimeSeriesStringId string_id;
  
  /**
   * Default ctor.
   * @param tsuid A non-null and non-empty TSUID of the proper length.
   * @param schema A non-null schema.
   * @throws IllegalArgumentException if the tsuid was null or empty,
   * the schema was null or the tsuid was of the wrong length, e.g.
   * it must have a metric and at least one tag pair.
   */
  public TSUID(final byte[] tsuid, final Schema schema) {
    if (Bytes.isNullOrEmpty(tsuid)) {
      throw new IllegalArgumentException("TSUID cannot be null or empty.");
    }
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    if (tsuid.length <= schema.metricWidth()) {
      throw new IllegalArgumentException("TSUID is too short. Must "
          + "contain at least a metric.");
    }
    if ((tsuid.length - schema.metricWidth()) % 
        (schema.tagkWidth() + schema.tagvWidth()) != 0) {
      throw new IllegalArgumentException("TSUID does not conform to the "
          + "schema by including tag key and tag value pairs.");
    }
    this.tsuid = tsuid;
    this.schema = schema;
  }

  @Override
  public boolean encoded() {
    return true;
  }
  
  @Override
  public Deferred<TimeSeriesStringId> decode(final boolean cache, 
                                             final Span span) {
    if (string_id != null) {
      return Deferred.fromResult(string_id);
    }
    
    synchronized (this) {
      // still has a race here where we may fire off more than one 
      // resolution, but it should be infrequent.
      if (string_id != null) {
        return Deferred.fromResult(string_id);
      }
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".decode")
          .start();
    } else {
      child = null;
    }
    
    // resolve the tag keys
    final int tagkv_size = schema.tagkWidth() + schema.tagvWidth();
    int tag_size = (tsuid.length - schema.metricWidth()) / tagkv_size;
    
    final BaseTimeSeriesStringId.Builder builder = 
        BaseTimeSeriesStringId.newBuilder();
    final String[] tag_keys = new String[tag_size > 0 ? tag_size : 1];
    final String[] tag_values = new String[tag_size > 0 ? tag_size : 1];
    
    class FinalCB implements Callback<TimeSeriesStringId, ArrayList<Object>> {
      @Override
      public TimeSeriesStringId call(final ArrayList<Object> ignored) throws Exception {
        for (int i = 0; i < tag_keys.length; i++) {
          builder.addTags(tag_keys[i], tag_values[i]);
        }
        
        final TimeSeriesStringId id = builder.build();
        if (child != null) {
          child.setSuccessTags()
               .finish();
        }
        if (cache) {
          synchronized (TSUID.this) {
            string_id = id;
          }
        }
        return id;
      }
    }
    
    class TagKeyCB implements Callback<Object, List<String>> {
      @Override
      public Object call(final List<String> names) throws Exception {
        for (int i = 0; i < names.size(); i++) {
          tag_keys[i] = names.get(i);
          if (Strings.isNullOrEmpty(tag_keys[i])) {
            int offset = schema.metricWidth() + 
                (i * (schema.tagkWidth() + schema.tagvWidth()));
            throw new NoSuchUniqueId(Schema.TAGK_TYPE, 
                Arrays.copyOfRange(tsuid, offset,
                    offset + schema.tagkWidth()));
          }
        }
        return null;
      }
    }
    
    class TagValueCB implements Callback<Object, List<String>> {
      @Override
      public Object call(final List<String> values) throws Exception {
        for (int i = 0; i < values.size(); i++) {
          tag_values[i] = values.get(i);
          if (Strings.isNullOrEmpty(tag_values[i])) {
            int offset = schema.metricWidth() + schema.tagkWidth() +
                (i * (schema.tagkWidth() + schema.tagvWidth()));
            throw new NoSuchUniqueId(Schema.TAGV_TYPE, 
                Arrays.copyOfRange(tsuid, offset,
                    offset + schema.tagvWidth()));
          }
        }
        return null;
      }
    }
    
    class MetricCB implements Callback<Object, String> {
      @Override
      public Object call(final String metric) throws Exception {
        if (Strings.isNullOrEmpty(metric)) {
          throw new NoSuchUniqueId(Schema.METRIC_TYPE, metric());
        }
        builder.setMetric(metric);
        return null;
      }
    }
    
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
               .log("Exception", 
                   (ex instanceof DeferredGroupException) ? 
                       Exceptions.getCause((DeferredGroupException) ex) : ex)
               .finish();
        }
        if (ex instanceof DeferredGroupException) {
          final Exception t = (Exception) Exceptions
              .getCause((DeferredGroupException) ex);
          throw t;
        }
        throw ex;
      }
    }
    
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithCapacity(3);
    try {
      // resolve the metric
      deferreds.add(schema.getName(UniqueIdType.METRIC, 
          Arrays.copyOfRange(tsuid, 0, schema.metricWidth()), 
          child != null ? child : span)
            .addCallback(new MetricCB()));
      
      List<byte[]> ids = Lists.newArrayListWithCapacity(tag_size > 0 ? tag_size : 1);
      for (int i = schema.metricWidth(); i < tsuid.length; i += tagkv_size) {
        ids.add(Arrays.copyOfRange(tsuid, i, i + schema.tagkWidth()));
      }
      deferreds.add(schema.getNames(UniqueIdType.TAGK, ids, 
          child != null ? child : span)
            .addCallback(new TagKeyCB()));
      
      ids = Lists.newArrayListWithCapacity(tag_size > 0 ? tag_size : 1);
      for (int i = schema.metricWidth() + schema.tagkWidth(); 
               i < tsuid.length; i += tagkv_size) {
        ids.add(Arrays.copyOfRange(tsuid, i, i + schema.tagvWidth()));
      }
      
      deferreds.add(schema.getNames(UniqueIdType.TAGV, ids, 
          child != null ? child : span)
          .addCallback(new TagValueCB()));
      
      return Deferred.group(deferreds)
          .addCallbacks(new FinalCB(), new ErrCB());
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

  @Override
  public int compareTo(final TimeSeriesByteId o) {
    // TODO - implement
    throw new UnsupportedOperationException("IMPLEMENT ME!");
  }

  @Override
  public byte[] alias() {
    return null;
  }

  @Override
  public byte[] namespace() {
    return null;
  }

  @Override
  public byte[] metric() {
    return Arrays.copyOfRange(tsuid, 0, schema.metricWidth());
  }

  @Override
  public ByteMap<byte[]> tags() {
    if (tags != null) {
      return tags;
    }
    
    synchronized (this) {
      if (tags == null) {
        final int tagkv_size = schema.tagkWidth() + schema.tagvWidth();
        tags = new ByteMap<byte[]>();
        for (int i = schema.metricWidth(); i < tsuid.length; i += tagkv_size) {
          byte[] tagk = Arrays.copyOfRange(tsuid, i, i + schema.tagkWidth());
          tags.put(tagk, Arrays.copyOfRange(tsuid, i + schema.tagvWidth(), i + tagkv_size));
        }
      }
    }
    return tags;
    
  }

  @Override
  public List<byte[]> aggregatedTags() {
    return Collections.emptyList();
  }

  @Override
  public List<byte[]> disjointTags() {
    return Collections.emptyList();
  }

  @Override
  public ByteSet uniqueIds() {
    if (uids != null) {
      return uids;
    }
    
    synchronized (this) {
      if (uids == null) {
        uids = new ByteSet();
        uids.add(tsuid);
      }
    }
    return uids;
  }

  @Override
  public boolean skipMetric() {
    return false;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_BYTE_ID;
  }

  @Override
  public TimeSeriesDataStore dataStore() {
    return schema;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias() != null ? alias() : "null")
        .append(", namespace=")
        .append(namespace())
        .append(", metric=")
        .append(metric())
        .append(", tags=")
        .append(tags())
        .append(", aggregated_tags=")
        .append(aggregatedTags())
        .append(", disjoint_tags=")
        .append(disjointTags())
        .append(", uniqueIds=")
        .append(uniqueIds());
    return buf.toString();
  }
  
  /** @return The original TSUID. */
  public byte[] tsuid() {
    return tsuid;
  }
}
