// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;

import net.opentsdb.core.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An ID that can be used to merge multiple time series into one. Use by
 * calling {@link #newBuilder()}, setting an opitonal Alias and calling 
 * {@link Builder#addSeries(TimeSeriesId)} as many times as needed. It has the
 * following logic:
 * <p>
 * <ul>
 * <li>Alias' for the individual IDs are ignored. Use 
 * {@link Builder#setAlias(byte[])}.</li>
 * <li>Namespaces are merged into a unique set of values.</li>
 * <li>Metrics are merged into a unique set of values.</li>
 * <li>Tags, Aggregated Tags and Disjoint Tags are merged in the following way:
 *   <ul>
 *   <li>Tag pairs with the same key and value across all IDs are merged in the
 *   {@link #tags()} map.</li>
 *   <li>Tag keys that appear across all IDs (or in the {@link #aggregatedTags()}
 *   list) are placed in the {@link #aggregatedTags()} list.</li>
 *   <li>Tag keys that do NOT appear consistently across all IDs in the 
 *   {@link #tags()} or {@link #aggregatedTags()} sets are moved to the 
 *   {@link #disjointTags()} list.</li> 
 *   </ul></li>
 * </ul>
 * 
 * @since 3.0
 */
public class MergedTimeSeriesId implements TimeSeriesId, Comparable<TimeSeriesId> {

  private byte[] alias;
  private List<byte[]> namespaces;
  private List<byte[]> metrics;
  private ByteMap<byte[]> tags = new ByteMap<byte[]>();
  private List<byte[]> aggregated_tags;
  private List<byte[]> disjoint_tags;
  private ByteSet unique_ids = new ByteSet();
  
  protected MergedTimeSeriesId(final Builder builder) {
    alias = builder.alias;
    
    ByteSet namespaces = null;
    ByteSet metrics = null;
    ByteMap<byte[]> tags = null;
    ByteSet aggregated_tags = null;
    ByteSet disjoint_tags = null;
    ByteSet unique_ids = null;
    
    int i = 0;
    for (final TimeSeriesId id : builder.ids) {
      if (id.namespaces() != null && !id.namespaces().isEmpty()) {
        if (namespaces == null) {
          namespaces = new ByteSet();
        }
        namespaces.addAll(id.namespaces());
      }
      
      if (id.metrics() != null && !id.metrics().isEmpty()) {
       if (metrics == null) {
         metrics = new ByteSet();
       }
        metrics.addAll(id.metrics());
      }
      
      // agg and disjoint BEFORE tags
      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        for (final byte[] tag : id.aggregatedTags()) {
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
            if (aggregated_tags == null) {
              aggregated_tags = new ByteSet();
            }
            aggregated_tags.add(tag);
          } else {
            if (disjoint_tags != null && disjoint_tags.contains(tag)) {
              // no-op
            } else {
              if (aggregated_tags == null) {
                aggregated_tags = new ByteSet();
              }
              if (i < 1) {
                aggregated_tags.add(tag);
              } else {
                if (aggregated_tags != null && !aggregated_tags.contains(tag)) {
                  if (disjoint_tags == null) {
                    disjoint_tags = new ByteSet();
                  }
                  disjoint_tags.add(tag);
                }
              }
            }
          }
        }
      }
      
      if (id.disjointTags() != null && !id.disjointTags().isEmpty()) {
        for (final byte[] tag : id.disjointTags()) {
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
          }
          if (aggregated_tags != null && aggregated_tags.contains(tag)) {
            aggregated_tags.remove(tag);
          }
          if (disjoint_tags == null) {
            disjoint_tags = new ByteSet();
          }
          disjoint_tags.add(tag);
        }
      }
      
      if (id.tags() != null && !id.tags().isEmpty()) {
        if (tags == null) {
          tags = new ByteMap<byte[]>();
        }
        
        for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
          if (disjoint_tags != null && disjoint_tags.contains(pair.getKey())) {
            continue;
          }
          if (aggregated_tags != null && aggregated_tags.contains(pair.getKey())) {
            continue;
          }
          
          // check tag and values!
          final byte[] existing_value = tags.get(pair.getKey());
          if (existing_value == null) {
            if (i > 0) {
              // disjoint!
              if (disjoint_tags == null) {
                disjoint_tags = new ByteSet();
              }
              disjoint_tags.add(pair.getKey());
            } else {
              tags.put(pair.getKey(), pair.getValue());
            }
          } else if (Bytes.memcmp(existing_value, pair.getValue()) != 0) {
            // move to agg
            if (aggregated_tags == null) {
              aggregated_tags = new ByteSet();
            }
            aggregated_tags.add(pair.getKey());
            tags.remove(pair.getKey());
          }
        }
        
        // reverse
        if (tags != null) {
          final Iterator<Entry<byte[], byte[]>> it = tags.iterator();
          while (it.hasNext()) {
            final Entry<byte[], byte[]> pair = it.next();
            if (!id.tags().containsKey(pair.getKey())) {
              // disjoint!
              if (disjoint_tags == null) {
                disjoint_tags = new ByteSet();
              }
              disjoint_tags.add(pair.getKey());
              it.remove();
            }
          }
        }
      } // end id tags check
      
      // reverse agg
      if (aggregated_tags != null && i > 0) {
        final Iterator<byte[]> it = aggregated_tags.iterator();
        while(it.hasNext()) {
          final byte[] tag = it.next();
          if (id.tags() != null && id.tags().containsKey(tag)) {
            // good, keep it
            continue;
          }
          if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
            boolean matched = false;
            for (final byte[] other : id.aggregatedTags()) {
              if (Bytes.memcmpMaybeNull(tag, other) == 0) {
                matched = true;
                break;
              }
            }
            if (matched) {
              continue;
            }
          }
          
          if (disjoint_tags == null) {
            disjoint_tags = new ByteSet();
          }
          disjoint_tags.add(tag);
          it.remove();
        }
      } // end reverse agg check
      
      if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
        if (unique_ids == null) {
          unique_ids = new ByteSet();
        }
        unique_ids.addAll(id.uniqueIds());
      }
      i++;
    }
    
    if (namespaces != null && !namespaces.isEmpty()) {
      this.namespaces = Lists.newArrayList(namespaces);
    }
    if (metrics != null && !metrics.isEmpty()) {
      this.metrics = Lists.newArrayList(metrics);
    }
    if (tags != null && !tags.isEmpty()) {
      this.tags = tags;
    }
    if (aggregated_tags != null && !aggregated_tags.isEmpty()) {
      this.aggregated_tags = Lists.newArrayList(aggregated_tags);
    }
    if (disjoint_tags != null && !disjoint_tags.isEmpty()) {
      this.disjoint_tags = Lists.newArrayList(disjoint_tags);
    }
  }
  
  @Override
  public boolean encoded() {
    // TODO
    return false;
  }
  
  @Override
  public byte[] alias() {
    return alias;
  }

  @Override
  public List<byte[]> namespaces() {
    return namespaces == null ? Collections.<byte[]>emptyList() : namespaces;
  }

  @Override
  public List<byte[]> metrics() {
    return metrics == null ? Collections.<byte[]>emptyList() : metrics;
  }

  @Override
  public ByteMap<byte[]> tags() {
    return tags;
  }

  @Override
  public List<byte[]> aggregatedTags() {
    return aggregated_tags == null ? 
        Collections.<byte[]>emptyList() : aggregated_tags;
  }

  @Override
  public List<byte[]> disjointTags() {
    return disjoint_tags == null ? 
        Collections.<byte[]>emptyList() : disjoint_tags;
  }

  @Override
  public ByteSet uniqueIds() {
    return unique_ids;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    
    final TimeSeriesId id = (TimeSeriesId) o;
    
    // long slog through byte arrays.... :(
    if (Bytes.memcmpMaybeNull(alias, id.alias()) != 0) {
      return false;
    }
    if (!Bytes.equals(namespaces, id.namespaces())) {
      return false;
    }
    if (!Bytes.equals(metrics, id.metrics())) {
      return false;
    }
    if (!Bytes.equals(tags, id.tags())) {
      return false;
    }
    if (!Bytes.equals(aggregated_tags, id.aggregatedTags())) {
      return false;
    }
    if (!Bytes.equals(disjoint_tags, id.aggregatedTags())) {
      return false;
    }
    return true;
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    if (alias != null) {
        hasher.putBytes(alias);
    }
    return hasher.putObject(namespaces, Bytes.BYTE_LIST_FUNNEL)
        .putObject(metrics, Bytes.BYTE_LIST_FUNNEL)
        .putObject(tags, Bytes.BYTE_MAP_FUNNEL)
        .putObject(aggregated_tags, Bytes.BYTE_LIST_FUNNEL)
        .putObject(disjoint_tags, Bytes.BYTE_LIST_FUNNEL)
        .hash();
  }
  
  @Override
  public int compareTo(final TimeSeriesId o) {
    return ComparisonChain.start()
        .compare(alias, o.alias(), Bytes.MEMCMP)
        .compare(namespaces, o.namespaces(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(metrics, o.metrics(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(tags, o.tags(), Bytes.BYTE_MAP_CMP)
        .compare(aggregated_tags, o.aggregatedTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .compare(disjoint_tags, o.disjointTags(), 
            Ordering.from(Bytes.MEMCMP).lexicographical().nullsFirst())
        .result();
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("alias=")
        .append(alias != null ? new String(alias, Const.UTF8_CHARSET) : "null")
        .append(", namespaces=")
        .append(Bytes.toString(namespaces, Const.UTF8_CHARSET))
        .append(", metrics=")
        .append(Bytes.toString(metrics, Const.UTF8_CHARSET))
        .append(", tags=")
        .append(Bytes.toString(tags, Const.UTF8_CHARSET, Const.UTF8_CHARSET))
        .append(", aggregated_tags=")
        .append(Bytes.toString(aggregated_tags, Const.UTF8_CHARSET))
        .append(", disjoint_tags=")
        .append(Bytes.toString(disjoint_tags, Const.UTF8_CHARSET))
        .append(", uniqueIds=")
        .append(unique_ids);
    return buf.toString();
  }
  
  /** @return A builder for the merged ID */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private List<TimeSeriesId> ids;
    private byte[] alias;
    
    public Builder setAlias(final String alias) {
      if (!Strings.isNullOrEmpty(alias)) {
        this.alias = alias.getBytes(Const.UTF8_CHARSET);
      }
      return this;
    }
    
    public Builder setAlias(final byte[] alias) {
      this.alias = alias;
      return this;
    }
    
    public Builder addSeries(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      if (ids == null) {
        ids = Lists.newArrayListWithExpectedSize(2);
      }
      ids.add(id);
      return this;
    }

    public MergedTimeSeriesId build() {
      return new MergedTimeSeriesId(this);
    }
  }
  
}
