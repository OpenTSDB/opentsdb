package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.graph.MutableGraph;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestDefaultTimeSeriesDataSourceConfig {

  private DefaultQueryPlanner planner;

  @Before
  public void before() throws Exception {

    planner = mock(DefaultQueryPlanner.class);
    MutableGraph<QueryNodeConfig> graph = mock(MutableGraph.class);
    when(planner.configGraph()).thenReturn(graph);


  }

  @Test
  public void builder() {

    final QueryNodeConfig build = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setMetric(MetricLiteralFilter.newBuilder().setMetric("system.cpu.use").build())
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .build();

    assertEquals("UT", build.getId());
    assertEquals(2, build.getSources().size());
    assertTrue(build.getSources().contains("colo1"));
    assertTrue(build.getSources().contains("colo2"));
    assertEquals("TimeSeriesDataSource", build.getType());

  }

  @Test
  public void setUpTimeShiftSingleNode() {
    final QueryNodeConfig build = DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setSourceId("HBase")
        .setNamespace("Verizon")
        .setTimeShiftInterval("1h")
        .setPreviousIntervals(1)
        .setMetric(MetricLiteralFilter.newBuilder().setMetric("system.cpu.use").build())
        .setId("UT")
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .build();

    DefaultTimeSeriesDataSourceConfig.setupTimeShift((TimeSeriesDataSourceConfig) build, planner);
    PowerMockito.verifyStatic(Mockito.times(1));
    DefaultTimeSeriesDataSourceConfig.rebuildPushDownNodesForTimeShift(build, build.toBuilder(), "UT-previous-PT1H");

  }

}
