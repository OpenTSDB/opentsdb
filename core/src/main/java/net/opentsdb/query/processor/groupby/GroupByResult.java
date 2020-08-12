// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryResult;

/**
 * A result from the {@link GroupBy} node for a segment. The grouping is 
 * performed on the tags specified in the config and then grouped by hash code
 * instead of string on the resulting time series IDs.
 * 
 * @since 3.0
 */
public class GroupByResult extends BaseWrappedQueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByResult.class);
  
  /** Used to denote when all of the upstreams are done with this result set. */
  protected final CountDownLatch latch;
  
  /** The parent node. */
  protected final GroupBy node;
  
  /** The list of groups. */
  protected final List<TimeSeries> results;

  private boolean sourceProcessInParallel;

  private int tsCountInQuery;
  
  /**
   * The default ctor.
   * @param node The non-null group by node this result belongs to.
   * @param next The non-null original query result.
   * @throws IllegalArgumentException if the node or result was null.
   */
  public GroupByResult(final GroupBy node, final QueryResult next) {
    super(node, next);
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (next == null) {
      throw new IllegalArgumentException("Query results cannot be null.");
    }
    
    this.sourceProcessInParallel = next.processInParallel();
    latch = new CountDownLatch(node.upstreams());
    this.node = node;
    final TLongObjectMap<TimeSeries> groups = new TLongObjectHashMap<TimeSeries>();
    if (next.idType().equals(Const.TS_STRING_ID)) {
      List<TimeSeries> timeSeries = next.timeSeries();
      tsCountInQuery = timeSeries.size();
      for (final TimeSeries series : timeSeries) {
        final TimeSeriesStringId id = (TimeSeriesStringId) series.id();
        final StringBuilder buf = new StringBuilder()
            .append(id.metric());
        
        if (!((GroupByConfig) node.config()).getTagKeys().isEmpty()) {
          boolean matched = true;
          for (final String key : ((GroupByConfig) node.config()).getTagKeys()) {
            final String tagv = id.getTagValue(key);
            if (tagv == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping series from group by due to missing tag key: " 
                    + key + "  " + series.id());
              }
              matched = false;
              break;
            }
            buf.append(tagv);
          }
          
          if (!matched) {
            continue;
          }
        }
        
        final long hash = LongHashFunction.xx().hashChars(buf.toString());
        GroupByTimeSeries group = (GroupByTimeSeries) groups.get(hash);
        if (group == null) {
          if (((GroupByConfig) node.config()).getMergeIds() || 
              ((GroupByConfig) node.config()).getFullMerge()) {
            group = new GroupByTimeSeries(node, this);  
          } else {
            BaseTimeSeriesStringId.Builder group_id = 
                BaseTimeSeriesStringId.newBuilder()
                  .setAlias(id.alias())
                  .setNamespace(id.namespace())
                  .setMetric(id.metric());
            if (!((GroupByConfig) node.config()).getTagKeys().isEmpty()) {
              for (final String key : ((GroupByConfig) node.config()).getTagKeys()) {
                group_id.addTags(key, id.tags().get(key));
              }
            }
            group = new GroupByTimeSeries(node, this, group_id.build());
          }
          groups.put(hash, group);
        }
        group.addSource(series);
      }
    } else if (next.idType().equals(Const.TS_BYTE_ID)) {
      final List<byte[]> keys = ((GroupByConfig) node.config()).getEncodedTagKeys();
      List<TimeSeries> timeSeries = next.timeSeries();
      tsCountInQuery = timeSeries.size();
      for (final TimeSeries series : timeSeries) {
        final TimeSeriesByteId id = (TimeSeriesByteId) series.id();
        // TODO - bench me, may be a better way
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        
        try {
          buf.write(id.metric());
          if (!((GroupByConfig) node.config()).getTagKeys().isEmpty()) {
            boolean matched = true;
            for (final byte[] key : keys) {
              if (id.tags() == null || id.tags().size() < 1) {
                matched = false;
                break;
              }
              
              final byte[] tagv = id.tags().get(key);
              if (tagv == null) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Dropping series from group by due to "
                      + "missing tag key: " + key + "  " + series.id());
                }
                matched = false;
                break;
              }
              buf.write(tagv);
            }
            
            if (!matched) {
              continue;
            }
          }
          
          final long hash = LongHashFunction.xx().hashBytes(buf.toByteArray());
          GroupByTimeSeries group = (GroupByTimeSeries) groups.get(hash);
          if (group == null) {
            if (((GroupByConfig) node.config()).getMergeIds() || 
                ((GroupByConfig) node.config()).getFullMerge()) {
              group = new GroupByTimeSeries(node, this);  
            } else {
              BaseTimeSeriesByteId.Builder group_id = 
                  BaseTimeSeriesByteId.newBuilder(id.dataStore())
                    .setAlias(id.alias())
                    .setNamespace(id.namespace())
                    .setMetric(id.metric());
              if (!((GroupByConfig) node.config()).getTagKeys().isEmpty()) {
                for (final byte[] key : ((GroupByConfig) node.config()).getEncodedTagKeys()) {
                  group_id.addTags(key, id.tags().get(key));
                }
              }
              group = new GroupByTimeSeries(node, this, group_id.build());
            }
            groups.put(hash, group);
          }
          group.addSource(series);
          
        } catch (IOException e) {
          throw new RuntimeException("Failed to build the group-by hash", e);
        }
        
      }
    } else {
      // TODO - proper exception type
      throw new RuntimeException("Unhandled time series ID type: " + next.idType());
    }
    results = Lists.newArrayList(groups.valueCollection());
  }
  
  @Override
  public List<TimeSeries> timeSeries() {
    return results;
  }

  /** @return The downstream result. */
  QueryResult downstreamResult() {
    return result;
  }

  public boolean isSourceProcessInParallel() {
    return sourceProcessInParallel;
  }

  public int getTsCountInQuery() {
    return tsCountInQuery;
  }
}
