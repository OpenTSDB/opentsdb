// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;

/**
 * A class that implements a mock of the DataPoints and DataPoint interfaces
 * for use in serializing data out to the user.
 */
@Ignore
public class MockDataPoints {

  private final DataPoints dps = mock(DataPoints.class);
  private final DataPoint dp = mock(DataPoint.class);
  private final SeekableView it = mock(SeekableView.class);
  
  private long timestamp = 1356998400000L;
  private int interval = 300000; // in ms
  private long value = 0;
  private int limit = 400; // only checks timeout every 100 dps
  
  private final String metric = "system.cpu.user";
  private final Map<String, String> tags = new HashMap<String, String>(1);
  private final List<String> agg_tags = new ArrayList<String>(1);
  private final List<String> tsuids = new ArrayList<String>(2);
  private final List<Annotation> annotations = new ArrayList<Annotation>(1);
  
  /**
   * Default Ctor that stores some values in the tags and tsuid as well as a 
   * single annotation.
   */
  public MockDataPoints() {
    tags.put("dc", "lga");
    agg_tags.add("host");
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    
    final Annotation note = new Annotation();
    note.setTSUID("000001000001000001");
    note.setStartTime(1356998401000L);
    note.setDescription("Just a simple note");
    annotations.add(note);
  }
  
  /**
   * Retrieves the DataPoints object and sets up the mock calls
   * @return A DataPoints object for iteration
   */
  public DataPoints getMock() {
    when(dps.metricName()).thenReturn(metric);
    when(dps.metricNameAsync()).thenReturn(Deferred.fromResult(metric));
    when(dps.getTags()).thenReturn(tags);
    when(dps.getTagsAsync()).thenReturn(Deferred.fromResult(tags));
    when(dps.getAggregatedTags()).thenReturn(agg_tags);
    when(dps.getAggregatedTagsAsync()).thenReturn(Deferred.fromResult(agg_tags));
    when(dps.getTSUIDs()).thenReturn(tsuids);
    when(dps.getAnnotations()).thenReturn(annotations);
    when(dps.size()).thenReturn(limit);
    when(dps.aggregatedSize()).thenReturn(limit * 2);
    when(dps.iterator()).thenReturn(it);
    when(dps.getQueryIndex()).thenReturn(0);
    
    // iterator mocking
    when(it.hasNext()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(final InvocationOnMock args) throws Throwable {
        if (value > limit) {
          return false;
        }
        return true;
      }
    });
    when(it.next()).thenAnswer(new Answer<DataPoint>() {
      @Override
      public DataPoint answer(final InvocationOnMock args) throws Throwable {
        value++;
        timestamp += interval;
        return dp;
      }
    });
    doThrow(new RuntimeException("OpenTSDB doesn't support remove"))
      .when(it).remove();

    // data point mocking
    when(dp.timestamp()).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(final InvocationOnMock args) throws Throwable {
        return timestamp;
      }
    });
    when(dp.isInteger()).thenReturn(true);
    when(dp.longValue()).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(final InvocationOnMock args) throws Throwable {
        return value;
      }
    });
    return dps;
  }
  
  /**
   * Returns the DataPoint object so that the test can override the mocks.
   * @return A DataPoint in the DataPoints class 
   */
  public DataPoint getMockDP() {
    return dp;
  }
}
