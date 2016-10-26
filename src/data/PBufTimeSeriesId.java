// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.hbase.async.Bytes.ByteMap;

import com.google.protobuf.ByteString;

import net.opentsdb.data.pbuf.TimeSeriesIdPB.TimeSeriesId;

/**
 * A time series ID backed by a protobuf object.
 * <p>
 * Fiends are lazily initialized on first access to avoid duplicating data.
 * <p>
 * This class is thread safe.
 * <p>
 * TODO - use the "encoded" flag properly when we need it.
 * TODO - return unmodifiable objects
 * @since 3.0
 */
public class PBufTimeSeriesId implements net.opentsdb.data.TimeSeriesId {
  /** The time series ID protobuf object */
  private final TimeSeriesId id;
  
  /** A list of the namespaces, lazily initialized */
  private List<byte[]> namespaces;
  
  /** A list of the metrics, lazily initialized */
  private List<byte[]> metrics;
  
  /** A map of the tags, lazily initialized */
  private ByteMap<byte[]> tags;
  
  /** A list of the aggregated tags, lazily initialized */
  private List<byte[]> aggregated_tags;
  
  /** A list of the disjoint tags, lazily initialized */
  private List<byte[]> disjoint_tags;
  
  /**
   * Default Ctor.
   * @param id A non-null protobuf ID object.
   * @throws IllegalArgumentException if the id was null.
   */
  public PBufTimeSeriesId(final TimeSeriesId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null");
    }
    this.id = id;
  }

  @Override
  public boolean encoded() {
    return id.getEncoded();
  }

  @Override
  public byte[] alias() {
    return id.getAliasBytes().toByteArray();
  }

  @Override
  public List<byte[]> namespaces() {
    if (namespaces == null) {
      synchronized (this) {
        if (namespaces == null) {
          namespaces = new ArrayList<byte[]>(id.getNamespacesCount());
          for (final ByteString namespace : id.getNamespacesList()) {
            namespaces.add(namespace.toByteArray());
          }
        }
      }
    }
    return namespaces;
  }

  @Override
  public List<byte[]> metrics() {
    if (metrics == null) {
      synchronized (this) {
        if (metrics == null) {
          metrics = new ArrayList<byte[]>(id.getMetricsCount());
          for (final ByteString metric : id.getMetricsList()) {
            metrics.add(metric.toByteArray());
          }
        }
      }
    }
    return metrics;
  }

  @Override
  public ByteMap<byte[]> tags() {
    if (tags == null) {
      synchronized (this) {
        if (tags == null) {
          if (id.getTagKeysCount() != id.getTagValuesCount()) {
            throw new IllegalStateException("The underlying protobuf ID did "
                + "not have the same number of tag values as tag keys: " + id);
          }
          tags = new ByteMap<byte[]>();
          for (int i = 0; i < id.getTagKeysCount(); i++) {
            tags.put(id.getTagKeys(i).toByteArray(), 
                id.getTagValues(i).toByteArray());
          }
        }
      }
    }
    return tags;
  }

  @Override
  public List<byte[]> aggregatedTags() {
    if (aggregated_tags == null) {
      synchronized (this) {
        if (aggregated_tags == null) {
          aggregated_tags = new ArrayList<byte[]>(id.getAggregatedTagsCount());
          for (final ByteString tags : id.getAggregatedTagsList()) {
            aggregated_tags.add(tags.toByteArray());
          }
        }
      }
    }
    return aggregated_tags;
  }

  @Override
  public List<byte[]> disjointTags() {
    if (disjoint_tags == null) {
      synchronized (this) {
        if (disjoint_tags == null) {
          disjoint_tags = new ArrayList<byte[]>(id.getDisjointTagsCount());
          for (final ByteString tags : id.getDisjointTagsList()) {
            disjoint_tags.add(tags.toByteArray());
          }
        }
      }
    }
    return disjoint_tags;
  }

  /** @return the underlying protobuf ID */
  public TimeSeriesId getPBufID() {
    return id;
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder()
        .append("{alias=")
        .append(id.getAlias())
        .append(", namespaces=[");
    for (int i = 0; i < namespaces().size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(new String(namespaces().get(i)));
    }
    buf.append("], metrics=[");
    for (int i = 0; i < metrics().size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(new String(metrics().get(i)));
    }
    buf.append("], tags=[");
    int ctr = 0;
    for (final Entry<byte[], byte[]> pair : tags().entrySet()) {
      if (ctr++ > 0) {
        buf.append(", ");
      }
      buf.append(new String(pair.getKey()))
         .append("=")
         .append(new String(pair.getValue()));
    }
    buf.append("], aggTags=[");
    for (int i = 0; i < aggregatedTags().size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(new String(aggregatedTags().get(i)));
    }
    buf.append("], disjointTags=[");
    for (int i = 0; i < disjointTags().size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(new String(disjointTags().get(i)));
    }
    buf.append("], encoded=")
       .append(id.getEncoded())
       .append("}");
    return buf.toString();
  }
}
