//This file is part of OpenTSDB.
//Copyright (C) 2017-2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.core.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.DateTime;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 * 
 * TODO - this is ugly and needs a lot of re-org and work.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = QuerySourceConfig.Builder.class)
public class QuerySourceConfig extends BaseQueryNodeConfig {
  /** The original and complete time series query. */
  private TimeSeriesQuery query;
  
  /** The starting timestamp of the query. */
  private final String start;
  private final TimeStamp start_ts;
  
  /** The end timestamp of the query. If null or empt, assume "now". */
  private final String end;
  private final TimeStamp end_ts;
  
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;
  
  /** A list of data types to fetch. If empty, fetch all. */
  private final List<String> types;
  
  /** A non-null metric filter used to determine the metric(s) to fetch. */
  private final MetricFilter metric;
  
  /** An optional filter ID found in the query. */
  private final String filter_id;
  
  /** An optional filter. If filter_id is set, this is ignored. */
  private final QueryFilter filter;
  
  /**
   * Private ctor for the builder.
   * @param builder The non-null builder.
   */
  protected QuerySourceConfig(final Builder builder) {
    super(builder);
    if (Strings.isNullOrEmpty(builder.start)) {
      throw new IllegalArgumentException("Start time cannot be null "
          + "or empty.");
    }
    if (builder.metric == null) {
      throw new IllegalArgumentException("Metric filter cannot be null.");
    }
    query = builder.query;
    start = builder.start;
    end = builder.end;
    timezone = builder.timezone;
    types = builder.types;
    metric = builder.metric;
    filter_id = builder.filterId;
    filter = builder.filter;
    start_ts = new MillisecondTimeStamp(
        DateTime.parseDateTimeString(start, timezone));
    if (Strings.isNullOrEmpty(end)) {
      end_ts = new MillisecondTimeStamp(DateTime.currentTimeMillis());
    } else {
      end_ts = new MillisecondTimeStamp(
          DateTime.parseDateTimeString(end, timezone));
    }
  }
  
  /** @return The non-null query object. */
  public TimeSeriesQuery getQuery() {
    return query;
  }
  
  /** @param query A non-null query to replace the existing query. */
  public void setTimeSeriesQuery(final TimeSeriesQuery query) {
    this.query = query;
  }
  
  /** @return user given start date/time, could be relative or absolute */
  public String getStart() {
    return start;
  }

  /** @return user given end date/time, could be relative, absolute or empty */
  public String getEnd() {
    return end;
  }

  /** @return user's timezone used for converting absolute human readable dates */
  public String getTimezone() {
    return timezone;
  }
  
  /** @return Returns the parsed start time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp startTime() {
    return start_ts;
  }
  
  /** @return Returns the parsed end time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp endTime() {
    return end_ts;
  }
  
  /** @return A list of data types to filter on. If null or empty, fetch
   * all. */
  public List<String> getTypes() {
    return types;
  }
  
  /** @return The non-null metric filter. */
  public MetricFilter getMetric() {
    return metric;
  }
  
  /** @return An optional filter ID to fetch. */
  public String getFilterId() {
    return filter_id;
  }
  
  /** @return The optional filter. If {@link #getFilterId()} is not null or
   * empty, this will always return null. */
  public QueryFilter getFilter() {
    if (Strings.isNullOrEmpty(filter_id)) {
      return filter;
    } 
    return null;
  }
  
  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public int compareTo(final QueryNodeConfig o) {
    if (!(o instanceof QuerySourceConfig)) {
      return -1;
    }
    
    // TODO - implement
    return ComparisonChain.start()
        .compare(id, ((QuerySourceConfig) o).id, Ordering.natural().nullsFirst())
        
        .result();
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(Const.HASH_FUNCTION().newHasher().putString(id == null ? "null" : id, 
        Const.UTF8_CHARSET).hash());
    // TODO - implement in full
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder {
    @JsonProperty
    private TimeSeriesQuery query;
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    @JsonProperty
    private String timezone;
    @JsonProperty
    private List<String> types;
    @JsonProperty
    private MetricFilter metric;
    @JsonProperty
    private String filterId;
    @JsonProperty
    private QueryFilter filter;
    
    /** 
     * @param query The non-null query to execute.
     * @return The builder. 
     */
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
      return this;
    }
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }
    
    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }
    
    public Builder setTimezone(final String timezone) {
      this.timezone = timezone;
      return this;
    }
    
    public Builder setTypes(final List<String> types) {
      this.types = types;
      return this;
    }
    
    public Builder addType(final String type) {
      if (types == null) {
        types = Lists.newArrayList();
      }
      types.add(type);
      return this;
    }
    
    public Builder setMetric(final MetricFilter metric) {
      this.metric = metric;
      return this;
    }
    
    public Builder setFilterId(final String filter_id) {
      this.filterId = filter_id;
      return this;
    }
    
    public Builder setQueryFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public QuerySourceConfig build() {
      return new QuerySourceConfig(this);
    }
  }

}
