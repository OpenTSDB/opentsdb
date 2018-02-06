// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
