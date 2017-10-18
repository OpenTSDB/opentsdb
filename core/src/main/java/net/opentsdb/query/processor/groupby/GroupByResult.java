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
package net.opentsdb.query.processor.groupby;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * A result from the {@link GroupBy} node for a segment. The grouping is 
 * performed on the tags specified in the config and then grouped by hash code
 * instead of string on the resulting time series IDs.
 * 
 * @since 3.0
 */
public class GroupByResult implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByResult.class);
  
  /** The parent node. */
  private final GroupBy node;
  
  /** The downstream result received by the group by node. */
  private final QueryResult next;
  
  /** The map of hash codes to groups. */
  private final Map<Long, TimeSeries> groups;
  
  /**
   * The default ctor.
   * @param node The non-null group by node this result belongs to.
   * @param next The non-null original query result.
   * @throws IllegalArgumentException if the node or result was null.
   */
  public GroupByResult(final GroupBy node, final QueryResult next) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (next == null) {
      throw new IllegalArgumentException("Query results cannot be null.");
    }
    this.node = node;
    this.next = next;
    groups = Maps.newHashMap();
    
    for (final TimeSeries series : next.timeSeries()) {
      final StringBuilder buf = new StringBuilder()
          .append(series.id().metric());
      
      boolean matched = true;
      for (final String key : ((GroupByConfig) node.config()).getTagKeys()) {
        final String tagv = series.id().tags().get(key);
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
      
      final long hash = LongHashFunction.xx_r39().hashChars(buf.toString());
      GroupByTimeSeries group = (GroupByTimeSeries) groups.get(hash);
      if (group == null) {
        group = new GroupByTimeSeries(node);
        groups.put(hash, group);
      }
      group.addSource(series);
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return next.timeSpecification();
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return groups.values();
  }
  
  @Override
  public long sequenceId() {
    return next.sequenceId();
  }
  
  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public void close() {
    next.close();
  }
}
