package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericInterpolatorFactories;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryResult;

public class TestGroupByResult {

  private GroupBy node;
  private GroupByConfig config;
  private QueryResult result;
  private TimeSpecification time_spec;
  private NumericMillisecondShard ts1;
  private NumericMillisecondShard ts2;
  private NumericMillisecondShard ts3;
  private NumericMillisecondShard ts4;
  
  @Before
  public void before() throws Exception {
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(
            new NumericInterpolatorFactories.Null())
        .build();
    node = mock(GroupBy.class);
    result = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    
    when(node.config()).thenReturn(config);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts1.add(1000, 1);
    ts1.add(3000, 5);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .addTags("dc", "lga")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(3000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .addTags("host", "web01")
        .addTags("dc", "phx")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0);
    ts3.add(3000, 7);
    
    ts4 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts4.add(1000, 0);
    ts4.add(3000, 7);
    
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts1, ts2, ts3, ts4));
    when(result.sequenceId()).thenReturn(42l);
    when(result.timeSpecification()).thenReturn(time_spec);
  }
  
  @Test
  public void ctor() throws Exception {
    GroupByResult gbr = new GroupByResult(node, result);
    assertEquals(42, gbr.sequenceId());
    assertSame(time_spec, gbr.timeSpecification());
  }
}
