package net.opentsdb.core;

import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;

public class TestRollupSpanSalted extends TestRollupSpan {

  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    
    hour1 = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING);
    hour2 = getRowKey(METRIC_STRING, 1357002000, TAGK_STRING, TAGV_STRING);
    hour3 = getRowKey(METRIC_STRING, 1357005600, TAGK_STRING, TAGV_STRING);
    
    rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-10m")
            .setPreAggregationTable("tsdb-rollup-agg-10m")
            .setInterval("10m")
            .setRowSpan("6h"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-agg-1h")
            .setInterval("1h")
            .setRowSpan("1d"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1d")
            .setPreAggregationTable("tsdb-rollup-agg-1d")
            .setInterval("1d")
            .setRowSpan("1n"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
  }
}
