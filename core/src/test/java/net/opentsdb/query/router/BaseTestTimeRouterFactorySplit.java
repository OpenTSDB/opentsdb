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

package net.opentsdb.query.router;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.AbstractQueryPipelineContext;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.MockTSDSFactory;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.execution.serdes.JsonV3QuerySerdesOptions;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.BaseTestDefaultQueryPlanner;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.Merger;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.slidingwindow.SlidingWindowConfig;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class BaseTestTimeRouterFactorySplit extends BaseTestDefaultQueryPlanner {

  protected static TimeRouterFactory FACTORY;
  protected static TimeSeriesDataSource SRC_MOCK;
  protected static MockTSDSFactory S3;
  protected static List<Class<? extends QueryNodeConfig>> DEFAULT_PUSHDOWNS;

  protected static final int BASE_TS = 1610668800;

  @BeforeClass
  public static void beforeClass() throws Exception {
    BaseTestDefaultQueryPlanner.beforeClass();
    SRC_MOCK = mock(TimeSeriesDataSource.class);
    DEFAULT_PUSHDOWNS = Lists.newArrayList(
            DownsampleConfig.class, GroupByConfig.class, SlidingWindowConfig.class, RateConfig.class);
    S3 = new MockTSDSFactory("s3", DEFAULT_PUSHDOWNS);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s3", S3);

    FACTORY = new TimeRouterFactory();
    FACTORY.registerConfigs(TSDB);
    FACTORY.initialize(TSDB, null).join(250);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, FACTORY);
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.SPLIT_KEY), true);
//
//    QueryNodeConfig config = mock(QueryNodeConfig.class);
//    when(config.getId()).thenReturn("mock");
//    when(SRC_MOCK.config()).thenReturn(config);
  }

  @Before
  public void before() throws Exception {
    super.before();
//    ctx_node = mock(QueryNode.class);
//    context = mock(QueryContext.class);
//    when(context.tsdb()).thenReturn(TSDB);
//
    S1.pushdowns = DEFAULT_PUSHDOWNS;
//    s1.idType = Const.TS_STRING_ID;
    S1.rollupConfig = DefaultRollupConfig.newBuilder()
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
            .build();
    S1.setupGraph = false;
    S2.pushdowns = DEFAULT_PUSHDOWNS;
//    s2.idType = Const.TS_STRING_ID;
    S2.setupGraph = false;
    S3.reset();
    S3.pushdowns = DEFAULT_PUSHDOWNS;
//    s3.idType = Const.TS_STRING_ID;
    S3.setupGraph = false;

    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.OVERLAP_DURATION_KEY), "5m");
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), false);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        System.out.println("[TRACE] " + invocation.getArguments()[0]);
        return null;
      }
    }).when(context).logTrace(anyString());
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        System.out.println("[TRACE] " + invocation.getArguments()[0]);
        return null;
      }
    }).when(context).logDebug(anyString());
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        System.out.println("[TRACE] " + invocation.getArguments()[0]);
        return null;
      }
    }).when(context).logWarn(anyString());
  }

  /**
   * Returns a spy config entry, required because we can't easily mock out the
   * DateTime or Systme classes, so we have to fudge the entry with the "now"
   * timestamp so it can compute the UT start and end times properly.
   * @param start The storage start time as relative or absolute epoch seconds
   * @param end The storage end time as relative or absolute epoch seconds
   * @param now The current timestamp in epoch seconds, for use with relative.
   * @param factory The factory to return for this entry.
   * @return The mock.
   */
  protected TimeRouterConfigEntry mockEntry(final String start,
                                            final String end,
                                            final long now,
                                            final MockTSDSFactory factory) {
    return mockEntry(start, end, now, factory, null);
  }

  protected TimeRouterConfigEntry mockEntry(final String start,
                                            final String end,
                                            final long now,
                                            final MockTSDSFactory factory,
                                            final String dataType) {
    return mockEntry(start, end, now, factory, dataType, false);
  }

  protected TimeRouterConfigEntry mockEntry(final String start,
                                            final String end,
                                            final long now,
                                            final MockTSDSFactory factory,
                                            final String dataType,
                                            final boolean fullOnly) {
    TimeRouterConfigEntry.Builder builder = TimeRouterConfigEntry.newBuilder()
            .setStart(start)
            .setSourceId(factory.id())
            .setFactory(factory)
            .setDataType(dataType)
            .setFullOnly(fullOnly)
            .setNow((int) now);
    if (end != null) {
      builder.setEnd(end);
    }
    TimeRouterConfigEntry entry = builder.build();
    entry.factory = factory;
    TimeRouterConfigEntry spy = spy(entry);
    when(spy.getStart(anyLong())).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return entry.getStart(now);
      }
    });
    when(spy.getEnd(anyLong())).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return entry.getEnd(now);
      }
    });
    doAnswer(new Answer<MatchType>() {
      @Override
      public MatchType answer(InvocationOnMock invocation) throws Throwable {
        QueryPipelineContext context = (QueryPipelineContext) invocation.getArguments()[0];
        TimeSeriesDataSourceConfig.Builder builder =
                (TimeSeriesDataSourceConfig.Builder) invocation.getArguments()[1];
        return entry.match(context, builder, (int) now);
      }
    }).when(spy).match(any(QueryPipelineContext.class),
            any(TimeSeriesDataSourceConfig.Builder.class),
            anyInt());
    return spy;
  }

//  protected class MockPipelineContext extends AbstractQueryPipelineContext {
//
//    /**
//     * Default ctor.
//     *
//     * @throws IllegalArgumentException if any argument was null.
//     */
//    public MockPipelineContext() {
//      super(BaseTestTimeRouterFactorySplit.this.context);
//    }
//
//    @Override
//    public Deferred<Void> initialize(Span span) {
//      return initializeGraph(null);
//    }
//
//  }

  protected static void setEntryConfigs(TimeRouterConfigEntry... entries) {
    FACTORY.setConfig(Lists.newArrayList(entries));
  }

//  protected void setupQuery(int start,
//                            int end,
//                            String[] serdes,
//                            String timeShift,
//                            QueryNodeConfig... nodes) {
//    List<QueryNodeConfig> executionGraph = Lists.newArrayList();
//    executionGraph.add(DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                    .setMetric("sys.if.in")
//                    .build())
//            .setTimeShiftInterval(timeShift)
//            .setId("m1")
//            .build());
//    if (nodes != null) {
//      for (int i = 0; i < nodes.length; i++) {
//        executionGraph.add(nodes[i]);
//      }
//    }
//    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
//            .setMode(QueryMode.SINGLE)
//            .setStart(Integer.toString(start))
//            .setEnd(Integer.toString(end))
//            .setExecutionGraph(executionGraph)
//            .setLogLevel(LogLevel.TRACE);
//    if (serdes != null) {
//      for (final String serde : serdes) {
//        builder.addSerdesConfig(JsonV3QuerySerdesOptions.newBuilder()
//                        .addFilter(serde)
//                        .setId("JsonV3QuerySerdes")
//                .build());
//      }
//    }
//    when(context.query()).thenReturn(builder.build());
//  }

//  protected QueryNodeConfig groupBy(String... sources) {
//    GroupByConfig.Builder builder = GroupByConfig.newBuilder()
//            .setAggregator("sum")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("gb");
//    for (String source : sources) {
//      builder.addSource(source);
//    }
//    return builder.build();
//  }
//
//  protected QueryNodeConfig downsample(String interval, String... sources) {
//    DownsampleConfig.Builder builder = DownsampleConfig.newBuilder()
//            .setAggregator("avg")
//            .setInterval(interval)
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("ds");
//    for (String source : sources) {
//      builder.addSource(source);
//    }
//    return builder.build();
//  }
//

//
//  protected QueryNodeConfig window(String window, String... sources) {
//    SlidingWindowConfig.Builder builder = SlidingWindowConfig.newBuilder()
//            .setAggregator("avg")
//            .setWindowSize(window)
//            .setId("w");
//    for (String source : sources) {
//      builder.addSource(source);
//    }
//    return builder.build();
//  }

  enum Op {
    PLUS,
    MINUS;
  }

  protected int basePlus(String duration) {
    return basePlus(duration, null, null);
  }

  protected int basePlus(String duration, Op op, String secondDuration) {
    long seconds = DateTime.parseDuration(duration) / 1000;
    if (secondDuration != null) {
      long second = DateTime.parseDuration(secondDuration) / 1000;
      if (op == Op.PLUS) {
        seconds += second;
      } else {
        seconds -= second;
      }
    }
    return (int) (BASE_TS + seconds);
  }

  protected int baseMinus(String duration) {
    return baseMinus(duration, null, null);
  }

  protected int baseMinus(String duration, Op op, String secondDuration) {
    long seconds = DateTime.parseDuration(duration) / 1000;
    if (secondDuration != null) {
      long second = DateTime.parseDuration(secondDuration) / 1000;
      if (op == Op.PLUS) {
        seconds += second;
      } else {
        seconds -= second;
      }
    }
    return (int) (BASE_TS - seconds);
  }

  protected void setCurrentQuery3OverlappingSources() {
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "1h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "22h-ago", BASE_TS, S3)
    );
  }

  protected void setCurrentQuery3FixOverlappingSources() {
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "1h-ago", BASE_TS, S2),
            mockEntry("1w-ago", Integer.toString(baseMinus("22h")), BASE_TS, S3)
    );
  }

  /**
   * 0 == String ID of the TimeSeriesDataSourceConfig node we expect.
   * 1 == Epoch timestamp of the node start time override.
   * 2 == Epoch timestamp of the node end time override.
   * @param nodeAndTimestamps A non-null list of nodes and their timestamps.
   */
  protected void assertTimestamps(Object[]... nodeAndTimestamps) {
    for (int i = 0; i < nodeAndTimestamps.length; i++) {
      Object[] validation = nodeAndTimestamps[i];
      QueryNode node = pipelineContext.plan().nodeForId((String) validation[0]);
      if (node == null) {
        throw new AssertionError("No node '" + validation[0] + "' in plan.");
      }
      TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) node.config();
      int start = (int) validation[1];
      if (start != tsdsc.startTimestamp().epoch()) {
        throw new AssertionError("Start time [" +
                tsdsc.startTimestamp().epoch() + "] did not match the " +
                "expected value [" + start +"] for " + tsdsc.getId());
      }

      int end = (int) validation[2];
      if (end != tsdsc.endTimestamp().epoch()) {
        throw new AssertionError("End time [" +
                tsdsc.endTimestamp().epoch() + "] did not match the " +
                "expected value [" + end +"] for " + tsdsc.getId());
      }
    }
  }

//  /**
//   * Asserts edges.
//   * @param vertices The even number of vertices to check for edges.
//   */
//  protected void assertEdges(String... vertices) {
//    for (int i = 0; i < vertices.length; i += 2) {
//      QueryNode node1 = planner.nodeForId(vertices[i]);
//      QueryNode node2 = planner.nodeForId(vertices[i + 1]);
//      if (node1 == null) {
//        throw new AssertionError("Node [" + vertices[i] +
//                "] is not a part of the graph.");
//      }
//      if (node2 == null) {
//        throw new AssertionError("Node [" + vertices[i + 1] +
//                "] is not a part of the graph.");
//      }
//
//      if (!planner.graph().hasEdgeConnecting(node1, node2)) {
//        throw new AssertionError("No edge from [" + vertices[i] +
//                "] to [" + vertices[i + 1] + "]");
//      }
//    }
//  }
//
  protected void assertIsMerger(String node) {
    QueryNode qn = pipelineContext.plan().nodeForId(node);
    if (qn == null) {
      throw new AssertionError("Node [" + node +
              "] is not a part of the graph.");
    }
    assertTrue(qn instanceof Merger);
  }
//
//  protected void assertEdgeToContextNode(String... nodes) {
//    for (int i = 0; i< nodes.length; i++) {
//      String node = nodes[i];
//      QueryNode qn = planner.nodeForId(node);
//      if (qn == null) {
//        throw new AssertionError("Node [" + node +
//                "] is not a part of the graph.");
//      }
//
//      if (!planner.graph().hasEdgeConnecting(ctx_node, qn)) {
//        throw new AssertionError("No edge from [CONTEXT NODE] to " +
//                "[" + node + "]");
//      }
//    }
//  }
//
//  /**
//   * Pairs of node ID strings then the ID of the factory. E.g. "s3_m1", "s3"...
//   * @param sourceAndIds The non-null list of Ids.
//   */
//  protected void assertSources(String... sourceAndIds) {
//    for (int i = 0; i < sourceAndIds.length; i += 2) {
//      String nodeId = sourceAndIds[i];
//      String factoryId = sourceAndIds[i + 1];
//      QueryNode node = planner.nodeForId(nodeId);
//      if (node == null) {
//        throw new AssertionError("No node '" + nodeId + "' in plan.");
//      }
//      TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) node.config();
//      assertEquals(factoryId, tsdsc.getSourceId());
//    }
//  }
//
//  protected void assertPushdowns(String node, Class<? extends QueryNodeConfig>... pds) {
//    QueryNode qn = planner.nodeForId(node);
//    if (qn == null) {
//      throw new AssertionError("Node [" + node +
//              "] is not a part of the graph.");
//    }
//    TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) qn.config();
//    List<QueryNodeConfig> pushdowns = tsdsc.getPushDownNodes();
//    if (pds == null) {
//      if (pushdowns != null && !pushdowns.isEmpty()) {
//        throw new AssertionError("Pushdowns were not empty as " +
//                "expected: " + pushdowns);
//      }
//      return;
//    }
//
//    assertEquals(pds.length, pushdowns.size());
//    for (int i = 0; i < pds.length; i++) {
//      QueryNodeConfig config = pushdowns.get(i);
//      assertTrue("Config " + config + "\nwas not assignable from " +
//              pds[i] + ".\nPDs:" + pushdowns,
//              config.getClass().isAssignableFrom(pds[i]));
//    }
//  }

  protected void debug() {
    System.out.println(pipelineContext.plan().printConfigGraph());
    for (QueryNodeConfig config : pipelineContext.plan().configGraph().nodes()) {
      if (config instanceof TimeSeriesDataSourceConfig) {
        System.out.println("Src: " + config.getId() + " => " + config.resultIds());
        System.out.println("      Start: " + ((TimeSeriesDataSourceConfig<?, ?>) config).startTimestamp());
        System.out.println("        End: " + ((TimeSeriesDataSourceConfig<?, ?>) config).endTimestamp());
      }
    }
  }
}
