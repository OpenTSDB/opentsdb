// This file is part of OpenTSDB.
// Copyright (C) 2015-2019  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.reflect.TypeToken;
import java.util.Iterator;
import java.util.Optional;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimeShiftNumericArrayIterator {


  private static TimeSeries SERIES;
  private TimeShiftResult result;

  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
            .setMetric("a")
            .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) SERIES).add(1);
    ((NumericArrayTimeSeries) SERIES).add(5);
    ((NumericArrayTimeSeries) SERIES).add(2);
    ((NumericArrayTimeSeries) SERIES).add(1);
  }

  @Before
  public void before() throws Exception {
    result = mock(TimeShiftResult.class);
    when(result.isPrevious()).thenReturn(true);
    when(result.amount()).thenReturn(DateTime.parseDuration2("1h"));
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new MillisecondTimeStamp(2000));
  }

  @Test
  public void ctor() throws Exception {
    TimeShiftNumericArrayIterator iterator =
        new TimeShiftNumericArrayIterator(result, SERIES);
    assertTrue(iterator.hasNext());

    TimeSeries ts = mock(TimeSeries.class);
    when(ts.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    iterator = new TimeShiftNumericArrayIterator(result, ts);
    assertFalse(iterator.hasNext());

    assertEquals(NumericArrayType.TYPE, iterator.getType());
  }

  @Test
  public void next() {
    TimeShiftNumericArrayIterator iterator =
        new TimeShiftNumericArrayIterator(result, SERIES);
    assertTrue(iterator.next() instanceof TimeShiftNumericArrayIterator);
    TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator1 =  SERIES.iterator(NumericArrayType.TYPE).get();
    assertEquals(iterator1.next().value().type(), ((TimeShiftNumericArrayIterator) iterator.next()).value().type());
  }

  @Test
  public void timestamp() {
    TimeShiftNumericArrayIterator iterator =
        new TimeShiftNumericArrayIterator(result, SERIES);
    assertEquals(2000, iterator.timestamp().msEpoch());
  }
}
