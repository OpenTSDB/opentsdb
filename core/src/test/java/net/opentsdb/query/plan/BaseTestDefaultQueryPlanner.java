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

package net.opentsdb.query.plan;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryPipelineContext;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.MockTSDSFactory;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.execution.serdes.JsonV3QuerySerdesOptions;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.Merger;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.slidingwindow.SlidingWindowConfig;
import net.opentsdb.query.processor.summarizer.SummarizerConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseTestDefaultQueryPlanner {
  protected static final String START = "1514764800";
  protected static final String END = "1514768400";

  protected static MockTSDB TSDB;
  protected static MockTSDSFactory STORE_FACTORY;
  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static List<TimeSeriesDataSource> STORE_NODES;
  protected static MockTSDSFactory S1;
  protected static MockTSDSFactory S2;

  protected QueryContext context;
  protected MockQPC pipelineContext;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();

    STORE_NODES = Lists.newArrayList();
    S1 = new MockTSDSFactory("s1");
    S2 = new MockTSDSFactory("s2");

    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
            TimeSeriesDataSourceFactory.class, "s1", (TSDBPlugin) S1);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
            TimeSeriesDataSourceFactory.class, "s2", (TSDBPlugin) S2);
    S1.store_nodes = STORE_NODES;
    S1.setupGraph = true;
    S2.store_nodes = STORE_NODES;
    S2.setupGraph = true;

    NUMERIC_CONFIG = (NumericInterpolatorConfig)
            NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                    .setDataType(NumericType.TYPE.toString())
                    .build();
  }

  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);

    STORE_NODES.clear();
    if (STORE_FACTORY != null) {
      STORE_FACTORY.reset();
    }

    S1.reset();
    S2.reset();
  }

  public static void setupDefaultStore() {
    STORE_FACTORY = new MockTSDSFactory("Default");
    STORE_FACTORY.setupGraph = true;
    STORE_FACTORY.store_nodes = STORE_NODES;
    ((DefaultRegistry) TSDB.registry).registerPlugin(
            TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) STORE_FACTORY);
  }

  public void run() throws Exception {
    pipelineContext.initialize(null).join(250);
  }

  public QueryNodeConfig metric(final String id, final String metric) {
    return DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric(metric)
                    .build())
            .setId(id)
            .build();
  }

  public QueryNodeConfig metric(final String id,
                                final String metric,
                                final String source) {
    return DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric(metric)
                    .build())
            .setSourceId(source)
            .setId(id)
            .build();
  }

  public QueryNodeConfig dsConfig(final String id,
                                  final String interval,
                                  final String... srcs) {
    DownsampleConfig.Builder builder = DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval)
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId(id);
    for (int i = 0; i < srcs.length; i++) {
      builder.addSource(srcs[i]);
    }
    return builder.build();
  }

  public QueryNodeConfig gbConfig(final String id, final String... srcs) {
    GroupByConfig.Builder builder =  GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId(id);
    for (int i = 0; i < srcs.length; i++) {
      builder.addSource(srcs[i]);
    }
    return builder.build();
  }

  public QueryNodeConfig slidingConfig(final String id,
                                       final String interval,
                                       final String... srcs) {
    SlidingWindowConfig.Builder builder= SlidingWindowConfig.newBuilder()
            .setWindowSize(interval)
            .setAggregator("sum")
            .setId(id);
    for (int i = 0; i < srcs.length; i++) {
      builder.addSource(srcs[i]);
    }
    return builder.build();
  }

  public QueryNodeConfig expression(final String id,
                                    final String exp,
                                    final String... srcs) {
    QueryNodeConfig.Builder builder = ExpressionConfig.newBuilder()
            .setExpression(exp)
            .setAs("MyExp")
            .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                    .setJoinType(JoinType.NATURAL)
                    .build())
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId(id);
    for (int i = 0; i < srcs.length; i++) {
      builder.addSource(srcs[i]);
    }
    return builder.build();
  }

  protected QueryNodeConfig rate(String id, String... sources) {
    RateConfig.Builder builder = RateConfig.newBuilder()
            .setInterval("1s")
            .setId(id);
    for (String source : sources) {
      builder.addSource(source);
    }
    return builder.build();
  }

  public SummarizerConfig summarizer(final String id, final String... srcs) {
    SummarizerConfig.Builder builder = SummarizerConfig.newBuilder()
            .setSummaries(Lists.newArrayList("avg", "max", "count"))
            .setId(id);
    for (String src : srcs) {
      builder.addSource(src);
    }
    return builder.build();
  }

  protected static RollupConfig defaultRollupConfig() {
    return DefaultRollupConfig.newBuilder()
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

  protected static DefaultRollupConfig.Builder defaultRollupBuilder() {
    return DefaultRollupConfig.newBuilder()
            .addAggregationId("sum", 0)
            .addAggregationId("max", 1)
            .addAggregationId("min", 2)
            .addAggregationId("count", 3)
            .addAggregationId("avg", 5);
  }

  /**
   * Asserts edges.
   * @param vertices The even number of vertices to check for edges.
   */
  protected void assertEdge(String... vertices) {
    for (int i = 0; i < vertices.length; i += 2) {
      QueryNode node1 = pipelineContext.plan().nodeForId(vertices[i]);
      QueryNode node2 = pipelineContext.plan().nodeForId(vertices[i + 1]);
      if (node1 == null) {
        throw new AssertionError("Node [" + vertices[i] +
                "] is not a part of the graph.");
      }
      if (node2 == null) {
        throw new AssertionError("Node [" + vertices[i + 1] +
                "] is not a part of the graph.");
      }

      if (!pipelineContext.plan().graph().hasEdgeConnecting(node1, node2)) {
        throw new AssertionError("No edge from [" + vertices[i] +
                "] to [" + vertices[i + 1] + "]");
      }
    }

    int edgeToContext = pipelineContext.plan().graph().successors(pipelineContext).size();
    if (pipelineContext.plan().configGraph().edges().size() - edgeToContext /* The context node */
        != (vertices.length / 2)) {
      throw new AssertionError("The number of edges [" +
              (pipelineContext.plan().configGraph().edges().size() - edgeToContext)+
              "] (minus the context edges) was different than the expected count [" +
              (vertices.length / 2) + "]");
    }
  }

  protected void assertEdgeToContextNode(String... nodes) {
    for (int i = 0; i< nodes.length; i++) {
      String node = nodes[i];
      QueryNode qn = pipelineContext.plan().nodeForId(node);
      if (qn == null) {
        throw new AssertionError("Node [" + node +
                "] is not a part of the graph.");
      }

      if (!pipelineContext.plan().graph().hasEdgeConnecting(pipelineContext, qn)) {
        throw new AssertionError("No edge from [CONTEXT NODE] to " +
                "[" + node + "]");
      }
    }

    int edgeToContext = pipelineContext.plan().graph().successors(pipelineContext).size();
    if (edgeToContext != nodes.length) {
      throw new AssertionError("Expected number of edges to context node [" +
              nodes.length + "] did not match the actual number [" + edgeToContext + "]");
    }
  }

  protected void assertNodesAndEdges(int nodes, int edges) {
    assertEquals(nodes, pipelineContext.plan().graph().nodes().size());
    assertEquals(edges, pipelineContext.plan().graph().edges().size());
  }

  protected void assertSerializationSource(String... nodeThenSources) {
    if (pipelineContext.plan().serializationSources().size() !=
            nodeThenSources.length / 2) {
      throw new AssertionError("Serialization sources " +
              pipelineContext.plan().serializationSources() + " size was" +
              " different than the expected size of [" +
              (nodeThenSources.length / 2) + "]");
    }
    for (int i = 0; i < nodeThenSources.length; i += 2) {
      QueryResultId id = new DefaultQueryResultId(nodeThenSources[i],
              nodeThenSources[i + 1]);
      if (!pipelineContext.plan().serializationSources().contains(id)) {
        throw new AssertionError("No node " + id +
                " found in serialization sources " +
                pipelineContext.plan().serializationSources());
      }
    }
  }

  protected void assertMergerExpecting(String node, String... sources) {
    QueryNode qn = pipelineContext.plan().nodeForId(node);
    if (qn == null) {
      throw new AssertionError("Node [" + node +
              "] is not a part of the graph.");
    }
    if (!(qn instanceof Merger)) {
      throw new AssertionError("Node [" + node + "] was not a " +
              "merger node. It was " + qn.getClass());
    }
    MergerConfig config = (MergerConfig) qn.config();
    for (int i = 0; i < sources.length; i++) {
//      QueryResultId id = new DefaultQueryResultId(nodeThenSources[i],
//              nodeThenSources[i + 1]);
      if (!config.sortedSources().contains(sources[i])) {
        throw new AssertionError("No node " + sources[i] +
                " found in merger sources: " + config.sortedSources());
      }
    }
  }

  protected void assertMergerTimeouts(String node, String... timeouts) {
    QueryNode qn = pipelineContext.plan().nodeForId(node);
    if (qn == null) {
      throw new AssertionError("Node [" + node +
              "] is not a part of the graph.");
    }
    if (!(qn instanceof Merger)) {
      throw new AssertionError("Node [" + node + "] was not a " +
              "merger node. It was " + qn.getClass());
    }
    MergerConfig config = (MergerConfig) qn.config();
    if (config.timeouts() == null || config.timeouts().size() != timeouts.length) {
      throw new AssertionError("Length of timeouts " + config.timeouts()
              + " differed from the expected values " + Arrays.toString(timeouts));
    }
    for (int i = 0; i < timeouts.length; i++) {
      if (config.timeouts().get(i).equals(timeouts[i])) {
        throw new AssertionError("Merger timeout [" +
                config.timeouts().get(i) + "] was not the expecte value [" +
                timeouts[i] + "]");
      }
    }
  }

  protected void assertResultIds(String node, String... nodeThenIds) {
    QueryNode qn = pipelineContext.plan().nodeForId(node);
    if (qn == null) {
      throw new AssertionError("Node [" + node +
              "] is not a part of the graph.");
    }

    for (int i = 0; i < nodeThenIds.length; i += 2) {
      QueryResultId id = new DefaultQueryResultId(nodeThenIds[i],
              nodeThenIds[i + 1]);
      if (!qn.config().resultIds().contains(id)) {
        throw new AssertionError("No node " + id +
                " found in result IDs " + qn.config().resultIds());
      }
    }
    if (qn.config().resultIds().size() != (nodeThenIds.length / 2)) {
      throw new AssertionError("Expected [" + (nodeThenIds.length / 2) +
              "] result Ids but got [" + qn.config().resultIds().size() + "] for " +
              qn.config().resultIds());
    }
  }

  /**
   *
   * @param node
   * @param size Number of pushdowns.
   * @param verifications In order in the PD list:
   *                      - Class
   *                      - String node ID
   *                      - String source ID (yes just one for now)
   *                      - Either:
   *                        - pair of Strings for result ID
   *                        - OR Array of pairs of strings for multiple result IDs.
   */
  protected void assertPushdowns(String node, int size, Object... verifications) {
    QueryNode qn = pipelineContext.plan().nodeForId(node);
    if (qn == null) {
      throw new AssertionError("Node [" + node +
              "] is not a part of the graph.");
    }
    TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) qn.config();
    List<QueryNodeConfig> pushdowns = tsdsc.getPushDownNodes();
    if (verifications == null) {
      if (pushdowns != null && !pushdowns.isEmpty()) {
        throw new AssertionError("Pushdowns were not empty as " +
                "expected: " + pushdowns);
      }
      return;
    }

    if (pushdowns.size() != size) {
      throw new AssertionError("Pushdowns size [" + pushdowns.size() +
              "] was not the expected size [" + size + "]");
    }

    int verificationIndex = 0;
    for (int i = 0; i < pushdowns.size(); i++) {
      final QueryNodeConfig config = pushdowns.get(i);
      Class<? extends QueryNodeConfig> clazz =
              (Class<? extends QueryNodeConfig>) verifications[verificationIndex++];
      if (!config.getClass().isAssignableFrom(clazz)) {
        throw new AssertionError("[" + i + "] " + config +
                "\nwas not assignable from [" + clazz + "].\nPDs:" + pushdowns);
      }

      String id = (String) verifications[verificationIndex++];
      if (!config.getId().equals(id)) {
        throw new AssertionError("[" + i + "] " + config +
                "\nID [" + config.getId() +"] did not equal [" + id +
                "].\nPDs:" + pushdowns);
      }

      id = (String) verifications[verificationIndex++];
      if (!config.getSources().get(0).equals(id)) {
        throw new AssertionError("[" + i + "] " + config +
                "\ndid not include source [" + id + "] in " +
                config.getSources() + ".\nPDs:" + pushdowns);
      }

      if (config.getSources().size() > 1) {
        throw new AssertionError("[" + i + "] " + config +
                "\nhad more than one source " +
                config.getSources() + ".\nPDs:" + pushdowns);
      }

      if (verifications[verificationIndex] instanceof String) {
        int ids = 0;
        while (verificationIndex < verifications.length &&
                verifications[verificationIndex] instanceof String) {
          DefaultQueryResultId resultId = new DefaultQueryResultId(
                  (String) verifications[verificationIndex++],
                  (String) verifications[verificationIndex++]
          );
          if (!config.resultIds().contains(resultId)) {
            throw new AssertionError("[" + i + "] " + config +
                    "\ndid not include result ID [" + resultId + "] in " +
                    config.resultIds() + ".\nPDs:" + pushdowns);
          }
          ids++;
        }

        if (config.resultIds().size() != ids) {
          throw new AssertionError("[" + i + "] " + config +
                  "\nhad more result IDs than expected [" + ids + "] in " +
                  config.resultIds() + ".\nPDs:" + pushdowns);
        }
      } else if (verifications[verificationIndex] instanceof String[]) {
        throw new UnsupportedOperationException("Code me up!");
      } else {
        throw new IllegalArgumentException(verifications[verificationIndex].getClass() +
                " must be a pair of strings or a string array for the result IDs.");
      }
    }
  }

  protected void setupQuery(QueryNodeConfig... nodes) {
    setupQuery(null, nodes);
  }

  protected void setupQuery(String[] serdes, QueryNodeConfig... nodes) {
    List<QueryNodeConfig> graph = Lists.newArrayList();
    for (final QueryNodeConfig config : nodes) {
      graph.add(config);
    }

    List<SerdesOptions> serdesOptions = Lists.newArrayList();
    if (serdes != null) {
      for (int i = 0; i < serdes.length; i++) {
        final SerdesOptions config = JsonV3QuerySerdesOptions.newBuilder()
                .setId("JsonV3QuerySerdes_" + i)
                .addFilter(serdes[i])
                .build();
        serdesOptions.add(config);
      }
    }

    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(START)
            .setEnd(END)
            .setExecutionGraph(graph);
    if (!serdesOptions.isEmpty()) {
      builder.setSerdesConfigs(serdesOptions);
    }
    when(context.query()).thenReturn(builder.build());
    pipelineContext = new MockQPC(context);
  }

  protected void setupQuery(int start,
                            int end) {
    setupQuery(start, end, null, null, null);
  }

  protected void setupQuery(int start,
                            int end,
                            QueryNodeConfig... nodes) {
    setupQuery(start, end, null, null, nodes);
  }
  protected void setupQuery(int start,
                            int end,
                            String[] serdes,
                            String timeShift,
                            QueryNodeConfig... nodes) {
    List<QueryNodeConfig> executionGraph = Lists.newArrayList();
    executionGraph.add(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.if.in")
                    .build())
            .setTimeShiftInterval(timeShift)
            .setId("m1")
            .build());
    if (nodes != null) {
      for (int i = 0; i < nodes.length; i++) {
        executionGraph.add(nodes[i]);
      }
    }
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(start))
            .setEnd(Integer.toString(end))
            .setExecutionGraph(executionGraph)
            .setLogLevel(LogLevel.TRACE);
    if (serdes != null) {
      for (final String serde : serdes) {
        builder.addSerdesConfig(JsonV3QuerySerdesOptions.newBuilder()
                .addFilter(serde)
                .setId("JsonV3QuerySerdes")
                .build());
      }
    }
    when(context.query()).thenReturn(builder.build());
    pipelineContext = new MockQPC(context);
  }

  /**
   * Pairs of node ID strings then the ID of the factory. E.g. "s3_m1", "s3"...
   * @param sourceAndIds The non-null list of Ids.
   */
  protected void assertSources(String... sourceAndIds) {
    for (int i = 0; i < sourceAndIds.length; i += 2) {
      String nodeId = sourceAndIds[i];
      String factoryId = sourceAndIds[i + 1];
      QueryNode node = pipelineContext.plan().nodeForId(nodeId);
      if (node == null) {
        throw new AssertionError("No node '" + nodeId + "' in plan.");
      }
      TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) node.config();
      assertEquals(factoryId, tsdsc.getSourceId());
    }
  }

  protected void debug() {
    System.out.println(pipelineContext.plan().printConfigGraph());

    for (QueryNodeConfig config : pipelineContext.plan().source_nodes) {
      TimeSeriesDataSourceConfig tsdc = (TimeSeriesDataSourceConfig) config;
      if (tsdc.getPushDownNodes() != null) {
        System.out.println(tsdc.getId() + " Pushdowns = " + tsdc.getPushDownNodes());
      }
    }
  }

  public class MockQPC extends AbstractQueryPipelineContext {

    /**
     * Default ctor.
     *
     * @param context The user's query context.
     * @throws IllegalArgumentException if any argument was null.
     */
    public MockQPC(QueryContext context) {
      super(context);
    }

    @Override
    public Deferred<Void> initialize(Span span) {
      return plan.plan(null);
    }

    @Override
    public net.opentsdb.core.TSDB tsdb() {
      return TSDB;
    }

    @Override
    public DefaultQueryPlanner plan() {
      return plan;
    }
  }
}
