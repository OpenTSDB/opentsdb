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
package net.opentsdb.query.processor.dedup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.dedup.DedupConfig;
import net.opentsdb.query.processor.dedup.DedupNode;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestDedupNode {

  @Mock
  private QueryPipelineContext pipelineContext;

  @Mock
  private QueryResult queryResult;

  @Mock
  private TimeSeries timeSeries;

  @Mock
  private QueryNode upStream;

  @Captor
  private ArgumentCaptor<QueryResult> upStreamCaptor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRemovesDuplicateTimeseries() {

    MockTimeSeries series = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("foo")
        .build());
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532455200), 43363892));
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532455200), 44421636.8));
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532483800), 49255974.4));
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.45831));
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532482900), 44013684.8));
    series.addValue(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.4));

    List<TimeSeriesValue<? extends TimeSeriesDataType>> expectedValues = 
        new ArrayList() {{
        add(new MutableNumericValue(new SecondTimeStamp(1532455200), 44421636.8));
        add(new MutableNumericValue(new SecondTimeStamp(1532483800), 49255974.4));
        add(new MutableNumericValue(new SecondTimeStamp(1532482900), 44013684.8));
        add(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.4));
    }};
    
    when(pipelineContext.upstream(any(DedupNode.class))).thenReturn(
        new ArrayList<QueryNode>(){{add(upStream);}});
    when(queryResult.timeSeries()).thenReturn(new ArrayList<TimeSeries>() {{
        add(series);
    }});

    DedupNode dedupNode = new DedupNode(null, pipelineContext, null, 
        (DedupConfig) DedupConfig.newBuilder()
          .setId("dedupe")
          .build());
    dedupNode.initialize(null);

    dedupNode.onNext(queryResult);

    verify(upStream).onNext(upStreamCaptor.capture());
    QueryResult dedupedResult = upStreamCaptor.getValue();

    Collection<TimeSeries> dedupedTSCollection = dedupedResult.timeSeries();
    assertEquals(1, dedupedTSCollection.size());
    TimeSeries dedupedTs = dedupedTSCollection.iterator().next();
    Optional<TypedTimeSeriesIterator> optional = 
        dedupedTs.iterator(NumericType.TYPE);
    assertTrue(optional.isPresent());
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> dedupedIterator = optional.get();
    List<TimeSeriesValue<? extends TimeSeriesDataType>> dedupedValues = new ArrayList<>();
    dedupedIterator.forEachRemaining(dedupedValues::add);

    assertTrue(expectedValues.containsAll(dedupedValues));
  }

}
