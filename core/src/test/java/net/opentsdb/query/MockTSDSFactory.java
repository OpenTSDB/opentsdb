/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package net.opentsdb.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockTSDSFactory extends BaseTSDBPlugin implements
        TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig , TimeSeriesDataSource> {

  public static final List<Class<? extends QueryNodeConfig>> PUSHDOWN_ALL =
          Lists.newArrayList();

  public List<TimeSeriesDataSource> store_nodes; // for planner UTs or tracking
                                                 // how often new nodes are returned.
  public RollupConfig rollupConfig;
  public List<Class<? extends QueryNodeConfig>> pushdowns;
  public TypeToken<? extends TimeSeriesId> idType = Const.TS_STRING_ID;
  public boolean setupGraph;
  public boolean supportsQuery;

  public MockTSDSFactory(final String id) {
    this.id = id;
    supportsQuery = true;
  }

  public MockTSDSFactory(final String id,
                         final List<Class<? extends QueryNodeConfig>> pushdowns) {
    this.id = id;
    this.pushdowns = pushdowns;
    rollupConfig = DefaultRollupConfig.newBuilder()
            .addAggregationId("sum", 0)
            .addAggregationId("max", 1)
            .addAggregationId("min", 2)
            .addAggregationId("count", 3)
            .addAggregationId("avg", 5)
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1s")
                    .setRowSpan("1h")
                    .setTable("foo")
                    .setPreAggregationTable("foo2")
                    .setDefaultInterval(true)
                    .build())
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    supportsQuery = true;
  }

  public MockTSDSFactory(final String id,
                         final List<Class<? extends QueryNodeConfig>> pushdowns,
                         final RollupConfig rollupConfig) {
    this.id = id;
    this.pushdowns = pushdowns;
    this.rollupConfig = rollupConfig;
  }

  @Override
  public TimeSeriesDataSourceConfig parseConfig(ObjectMapper mapper,
                                                TSDB tsdb, JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context,
                               final TimeSeriesDataSourceConfig config) {
    return supportsQuery ? true : false;
  }

  @Override
  public void setupGraph(QueryPipelineContext context,
                         TimeSeriesDataSourceConfig config,
                         QueryPlanner planner) {
    if (setupGraph) {
      planner.baseSetupGraph(context, config);
    }
  }

  @Override
  public TimeSeriesDataSource newNode(QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimeSeriesDataSource newNode(QueryPipelineContext context,
                                      TimeSeriesDataSourceConfig config) {
    TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
    when(node.config()).thenReturn(config);
    when(node.initialize(null)).thenAnswer(new Answer<Deferred<Void>>() {
      @Override
      public Deferred<Void> answer(InvocationOnMock invocation)
              throws Throwable {
        return Deferred.<Void>fromResult(null);
      }
    });
    when(node.toString()).thenReturn("Mock: " + config.getId());
    if (store_nodes != null) {
      store_nodes.add(node);
    }
    return node;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return idType;
  }

  @Override
  public boolean supportsPushdown(
          Class<? extends QueryNodeConfig> operation) {
    if (pushdowns == PUSHDOWN_ALL) {
      return true;
    }
    return pushdowns == null ? false : pushdowns.contains(operation);
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id,
                                                    Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> join_keys,
                                               Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(List<String> join_metrics,
                                                  Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String type() {
    return "MockUTFactory";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollupConfig;
  }

  public void reset() {
    pushdowns = null;
    idType = Const.TS_STRING_ID;
    supportsQuery = true;
    rollupConfig = DefaultRollupConfig.newBuilder()
            .addAggregationId("sum", 0)
            .addAggregationId("max", 1)
            .addAggregationId("min", 2)
            .addAggregationId("count", 3)
            .addAggregationId("avg", 5)
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1s")
                    .setRowSpan("1h")
                    .setTable("foo")
                    .setPreAggregationTable("foo2")
                    .setDefaultInterval(true)
                    .build())
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
  }
}