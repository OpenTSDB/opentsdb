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
package net.opentsdb.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A basic {@link TimeSeriesByteId} implementation that accepts strings for all
 * parameters. Includes a useful builder and after building, all lists are 
 * immutable.
 * 
 * @since 3.0
 */
public class BaseTimeSeriesByteId implements TimeSeriesByteId {
  
  /** The data store used to resolve the encoded ID to strings. */
  protected final TimeSeriesDataStore data_store; 
  
  /** Whether or not the byte arrays are specially encoded values. */
  protected boolean encoded;
  
  /** An optional alias. */
  protected byte[] alias;
  
  /** An optional namespace. */
  protected byte[] namespace;
  
  /** The required non-null and non-empty metric name. */
  protected byte[] metric;
  
  /** A map of tag key/value pairs for the ID. */
  protected ByteMap<byte[]> tags;
  
  /** An optional list of aggregated tags for the ID. */
  protected List<byte[]> aggregated_tags;
  
  /** An optional list of disjoint tags for the ID. */
  protected List<byte[]> disjoint_tags;
  
  /** A list of unique IDs rolled up into the ID. */
  protected ByteSet unique_ids;
  
  /** A cached hash code ID. May have some */
  protected volatile long cached_hash; 
  
  /** Whether or not to skip the metric during decoding. */
  protected boolean skip_metric;
  
  /**
   * Private CTor used by the builder. Converts the Strings to byte arrays
   * using UTF8.
   * @param builder A non-null builder.
   */
  private BaseTimeSeriesByteId(final Builder builder) {
    data_store = builder.data_store;
    encoded = builder.encoded;
    alias = builder.alias;
    namespace = builder.namespace;
    metric = builder.metric;
    if (Bytes.isNullOrEmpty(metric)) {
      throw new IllegalArgumentException("Metric cannot be null or empty.");
    }
    if (builder.tags != null && !builder.tags.isEmpty()) {
      for (final Entry<byte[], byte[]> pair : builder.tags.entrySet()) {
        // key cannot be null in a ByteMap.
        if (pair.getValue() == null) {
          throw new IllegalArgumentException("Tag value cannot be null.");
        }
        //tags = Collections.unmodifiableMap(builder.tags);
        // TODO - need an unmodifiable wrapper around the byte map
        tags = builder.tags;
      }
    } else {
      tags = new ByteMap<byte[]>();
    }
    if (builder.aggregated_tags != null && !builder.aggregated_tags.isEmpty()) {
      try {
        Collections.sort(builder.aggregated_tags, Bytes.MEMCMP);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Aggregated tags cannot contain nulls");
      }
      aggregated_tags = Collections.unmodifiableList(builder.aggregated_tags);
    } else {
      aggregated_tags = Collections.emptyList();
    }
    if (builder.disjoint_tags != null && !builder.disjoint_tags.isEmpty()) {
      try {
        Collections.sort(builder.disjoint_tags, Bytes.MEMCMP);
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("Disjoint Tags cannot contain nulls");
      }
      disjoint_tags = Collections.unmodifiableList(builder.disjoint_tags);
    } else {
      disjoint_tags = Collections.emptyList();
    }
    if (builder.unique_ids != null) {
      //unique_ids = Collections.unmodifiableSet(builder.unique_ids);
      // TODO - need an unmodifiable wrapper around the byte set
      unique_ids = builder.unique_ids;
    } else {
      unique_ids = new ByteSet();
    }
    skip_metric = builder.skip_metric;
  }

  @Override
  public TimeSeriesDataStore dataStore() {
    return data_store;
  }
  
  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public byte[] alias() {
    return alias;
  }

  @Override
  public byte[] namespace() {
    return namespace;
  }

  @Override
  public byte[] metric() {
    return metric;
  }

  @Override
  public ByteMap<byte[]> tags() {
    return tags;
  }

  @Override
  public List<byte[]> aggregatedTags() {
    return aggregated_tags;
  }

  @Override
  public List<byte[]> disjointTags() {
    return disjoint_tags;
  }

  @Override
  public ByteSet uniqueIds() {
    return unique_ids;
  }

  @Override
  public boolean skipMetric() {
    return skip_metric;
  }
  
  @Override
  public int compareTo(final TimeSeriesByteId o) {
    return ComparisonChain.start()
        .compare(alias, o.alias(), Bytes.MEMCMPNULLS)
        .compare(namespace, o.namespace(), Bytes.MEMCMPNULLS)
        .compare(metric, o.metric(), Bytes.MEMCMPNULLS)
        .compare(tags, o.tags(), Bytes.BYTE_MAP_CMP)
        .compare(aggregated_tags, o.aggregatedTags(), Bytes.BYTE_LIST_CMP)
        .compare(disjoint_tags, o.disjointTags(), Bytes.BYTE_LIST_CMP)
        .compare(unique_ids, o.uniqueIds(), ByteSet.BYTE_SET_CMP)
        .result();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof TimeSeriesByteId))
      return false;
    
    final TimeSeriesByteId id = (TimeSeriesByteId) o;
    
    if (Bytes.memcmpMaybeNull(alias, id.alias()) != 0) {
      return false;
    }
    if (Bytes.memcmpMaybeNull(namespace(), id.namespace()) != 0) {
      return false;
    }
    if (Bytes.memcmpMaybeNull(metric(), id.metric()) != 0) {
      return false;
    }
    if (!Bytes.equals(tags(), id.tags())) {
      return false;
    }
    if (!Bytes.equals(aggregatedTags(), id.aggregatedTags())) {
      return false;
    }
    if (!Bytes.equals(disjointTags(), id.disjointTags())) {
      return false;
    }
    if (!uniqueIds().equals(id.uniqueIds())) {
      return false;
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    if (cached_hash == 0) {
      cached_hash = buildHashCode();
    }
    return Long.hashCode(cached_hash);
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public long buildHashCode() {
    try {
      final ByteArrayOutputStream buf = new ByteArrayOutputStream();
      if (!Bytes.isNullOrEmpty(alias)) {
        buf.write(alias);
      }
      if (!Bytes.isNullOrEmpty(namespace)) {
        buf.write(namespace);
      }
      buf.write(metric);
      if (tags != null) {
        for (final Entry<byte[], byte[]> pair : tags.entrySet()) {
          buf.write(pair.getKey());
          buf.write(pair.getValue());
        }
      }
      if (aggregated_tags != null) {
        for (final byte[] t : aggregated_tags) {
          buf.write(t);
        }
      }
      if (disjoint_tags != null) {
        for (final byte[] t : disjoint_tags) {
          buf.write(t);
        }
      }
      if (unique_ids != null) {
        final List<byte[]> sorted = Lists.newArrayList(unique_ids);
        Collections.sort(sorted, Bytes.MEMCMPNULLS);
        for (final byte[] id : sorted) {
          buf.write(id);
        }
      }
      return (int) LongHashFunction.xx_r39().hashChars(buf.toString());
    } catch (IOException e) {
      throw new RuntimeException("WTF? Shouldn't have happened.", e);
    }
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias != null ? alias : "null")
        .append(", namespace=")
        .append(namespace)
        .append(", metric=")
        .append(metric)
        .append(", tags=")
        .append(tags)
        .append(", aggregated_tags=")
        .append(aggregated_tags)
        .append(", disjoint_tags=")
        .append(disjoint_tags)
        .append(", uniqueIds=")
        .append(unique_ids);
    return buf.toString();
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_BYTE_ID;
  }
  
  @Override
  public Deferred<TimeSeriesStringId> decode(final boolean cache, 
                                             final Span span) {
    return data_store.resolveByteId(this, span);
  }
  
  /**
   * Return a builder for the ID.
   * @param data_store A non-null store used to encode this ID.
   * @return A new builder.
   * @throws IllegalArgumentException if the data store was null.
   */
  public static Builder newBuilder(final TimeSeriesDataStore data_store) {
    return new Builder(data_store);
  }
  
  public static final class Builder {
    protected final TimeSeriesDataStore data_store;
    protected boolean encoded;
    protected byte[] alias;
    protected byte[] namespace;
    protected byte[] metric;
    protected ByteMap<byte[]> tags;
    protected List<byte[]> aggregated_tags;
    protected List<byte[]> disjoint_tags;
    protected ByteSet unique_ids; 
    protected boolean skip_metric;
    
    /**
     * Default private ctor.
     * @param data_store A non-null store.
     */
    private Builder(final TimeSeriesDataStore data_store) {
      if (data_store == null) {
        throw new IllegalArgumentException("Storage schema cannot be null.");
      }
      this.data_store = data_store;
    }
    
    public Builder setEncoded(final boolean encoded) {
      this.encoded = encoded;
      return this;
    }
    
    public Builder setAlias(final byte[] alias) {
      this.alias = alias;
      return this;
    }
    
    public Builder setNamespace(final byte[] namespace) {
      this.namespace = namespace;
      return this;
    }
    
    public Builder setMetric(final byte[] metric) {
      this.metric = metric;
      return this;
    }
    
    /**
     * Sets the tags map. <b>NOTE:</b> This will maintain a reference to the
     * map and will NOT make a copy. Be sure to avoid mutating the map after 
     * passing it to the builder.
     * @param metrics A non-null list of metrics.
     * @return The builder object.
     */
    public Builder setTags(final ByteMap<byte[]> tags) {
      this.tags = tags;
      return this;
    }
    
    public Builder addTags(final byte[] key, final byte[] value) {
      if (tags == null) {
        tags = new ByteMap<byte[]>();
      }
      tags.put(key, value);
      return this;
    }
    
    public Builder setAggregatedTags(final List<byte[]> aggregated_tags) {
      this.aggregated_tags = aggregated_tags;
      return this;
    }
    
    public Builder addAggregatedTag(final byte[] tag) {
      if (aggregated_tags == null) {
        aggregated_tags = Lists.newArrayList();
      }
      aggregated_tags.add(tag);
      return this;
    }
    
    public Builder setDisjointTags(final List<byte[]> disjoint_tags) {
      this.disjoint_tags = disjoint_tags;
      return this;
    }
    
    public Builder addDisjointTag(final byte[] tag) {
      if (disjoint_tags == null) {
        disjoint_tags = Lists.newArrayList();
      }
      disjoint_tags.add(tag);
      return this;
    }
    
    public Builder setUniqueId(final ByteSet unique_ids) {
      this.unique_ids = unique_ids;
      return this;
    }
    
    public Builder addUniqueId(final byte[] id) {
      if (id == null) {
        throw new IllegalArgumentException("Null unique IDs are not allowed.");
      }
      if (unique_ids == null) {
        unique_ids = new ByteSet();
      }
      unique_ids.add(id);
      return this;
    }
    
    public Builder setSkipMetric(final boolean skip_metric) {
      this.skip_metric = skip_metric;
      return this;
    }
    
    public BaseTimeSeriesByteId build() {
      return new BaseTimeSeriesByteId(this);
    }
  }

}
