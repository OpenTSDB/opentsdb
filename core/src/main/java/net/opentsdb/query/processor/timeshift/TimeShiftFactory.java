// This file is part of OpenTSDB.
// Copyright (C) 2019 The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that generates a time shift node.
 * <b>NOTE:</b> This isn't meant to be used by an end-user query, rather the
 * TimeSeriesDataSourceConfig nodes should instantiate this in the graph when
 * it's needed.
 * 
 * @since 3.0
 */
public class TimeShiftFactory extends BaseQueryNodeFactory<TimeShiftConfig, TimeShift> {
  public static final String TYPE = "Timeshift";
  private static final Logger LOG = LoggerFactory.getLogger(TimeShiftFactory.class);


  public TimeShiftFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, new NumericArrayIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeShift newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public TimeShift newNode(final QueryPipelineContext context,
                           final TimeShiftConfig config) {
    return new TimeShift(this, context, config);
  }
  
  @Override
  public TimeShiftConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("timeShiftInterval");
    if (n != null) {
      builder.setTimeshiftInterval(n.asText());
    }
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    n = node.get("sources");
    if (n != null && !n.isNull()) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    TimeShiftConfig ts_config = (TimeShiftConfig) builder.build();

    return (TimeShiftConfig) ts_config;
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final TimeShiftConfig config,
                         final QueryPlanner plan) {
    // no-op
  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory<TimeShift, NumericType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericIterator(result,
                                          sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericIterator(result,
                                          sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }

  }

  /**
   * The default numeric iterator factory.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory<TimeShift, NumericArrayType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
        final QueryResult result,
        final Collection<TimeSeries> sources,
        final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericArrayIterator(result,
          sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
        final QueryResult result,
        final Map<String, TimeSeries> sources,
        final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericArrayIterator(result,
          sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return Lists.newArrayList(NumericArrayType.TYPE);
    }

  }
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory<TimeShift, NumericSummaryType> {

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
        final QueryResult result,
        final Collection<TimeSeries> sources,
        final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericSummaryIterator(result,
          sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final TimeShift node,
        final QueryResult result,
        final Map<String, TimeSeries> sources,
        final TypeToken<? extends TimeSeriesDataType> type) {
      return new TimeShiftNumericSummaryIterator(result,
          sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }

  }
}
