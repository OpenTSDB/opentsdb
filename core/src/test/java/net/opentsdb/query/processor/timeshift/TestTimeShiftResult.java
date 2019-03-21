// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.timeshift.TimeShiftResult.TimeShiftTimeSeries;
import net.opentsdb.utils.DateTime;

public class TestTimeShiftResult {

  private static TimeSeries SERIES;
  private static TimeShift NODE;
  private static TimeShiftFactory FACTORY;
  private QueryResult result;

  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = mock(TimeSeries.class);
    when(SERIES.types()).thenReturn(Lists.newArrayList(NumericType.TYPE));
    NODE = mock(TimeShift.class);
    FACTORY = mock(TimeShiftFactory.class);
    when(NODE.factory()).thenReturn(FACTORY);
    when(FACTORY.newTypedIterator(eq(NumericType.TYPE), eq(NODE), 
        any(QueryResult.class), any(Collection.class)))
        .thenReturn(mock(TimeShiftNumericIterator.class));
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(SERIES));
  }
  
  @Test
  public void noTimeSpec() throws Exception {
    TimeShiftResult shift = new TimeShiftResult(NODE, result, true, 
        DateTime.parseDuration2("1h"));
    assertNull(shift.timeSpecification());
    assertEquals(DateTime.parseDuration2("1h"), shift.amount());
    assertTrue(shift.isPrevious());
    
    TimeSeries ts = shift.timeSeries().iterator().next();
    assertTrue(ts instanceof TimeShiftTimeSeries);
    assertFalse(ts.iterator(NumericArrayType.TYPE).isPresent());
    assertTrue(ts.iterator(NumericType.TYPE).isPresent());
    assertEquals(1, ts.iterators().size());
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericType.TYPE));
  }
  
  @Test
  public void timeSpec() throws Exception {
    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.start()).thenReturn(new SecondTimeStamp(1546214400L));
    when(spec.end()).thenReturn(new SecondTimeStamp(1546218000L));
    when(result.timeSpecification()).thenReturn(spec);
    
    TimeShiftResult shift = new TimeShiftResult(NODE, result, true, 
        DateTime.parseDuration2("1h"));
    assertEquals(1546218000L, shift.timeSpecification().start().epoch());
    assertEquals(1546221600L, shift.timeSpecification().end().epoch());
    TimeSeries ts = shift.timeSeries().iterator().next();
    assertSame(SERIES, ts);
  }
}
