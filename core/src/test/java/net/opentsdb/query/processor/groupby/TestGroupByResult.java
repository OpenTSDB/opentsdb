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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;

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
            new NumericInterpolatorFactory.Default())
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
