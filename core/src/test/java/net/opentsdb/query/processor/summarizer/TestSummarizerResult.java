// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.summarizer.SummarizerNonPassThroughResult.SummarizerTimeSeries;

public class TestSummarizerResult {

  private static TimeSeries SERIES;
  private QueryResult results;
  private SummarizerConfig config;
  private Summarizer node;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(0L), 42));
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(60L), 24));
  }
  
  @Before
  public void before() throws Exception {
    results = mock(QueryResult.class);
    node = mock(Summarizer.class);
    
    when(results.timeSeries()).thenReturn(Lists.newArrayList(SERIES));
    
    config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setId("summarizer")
        .build();
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void ctor() throws Exception {
    SummarizerNonPassThroughResult result = new SummarizerNonPassThroughResult(node, results);
    assertNull(result.timeSpecification());
    assertEquals(1, result.timeSeries().size());
    assertTrue(result.timeSeries().iterator().next() instanceof SummarizerTimeSeries);
    assertSame(node, result.source());
  }
}
