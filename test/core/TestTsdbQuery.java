// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * This class is for unit testing the TsdbQuery class. Pretty much making sure
 * the various ctors and methods function as expected. For actually running the
 * queries and validating the group by and aggregation logic, see 
 * {@link TestTsdbQueryQueries}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public final class TestTsdbQuery extends BaseTsdbTest {
  private TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    query = new TsdbQuery(tsdb);
  }
  
  @Test
  public void setStartTime() throws Exception {
    query.setStartTime(1356998400L);
    assertEquals(1356998400L, query.getStartTime());
  }
  
  @Test
  public void setStartTimeZero() throws Exception {
    query.setStartTime(0L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidNegative() throws Exception {
    query.setStartTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidTooBig() throws Exception {
    query.setStartTime(17592186044416L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeEqualtoEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeGreaterThanEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998460L);
  }
  
  @Test
  public void setEndTime() throws Exception {
    query.setEndTime(1356998400L);
    assertEquals(1356998400L, query.getEndTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void getStartTimeNotSet() throws Exception {
    query.getStartTime();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidNegative() throws Exception {
    query.setEndTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidTooBig() throws Exception {
    query.setEndTime(17592186044416L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeEqualtoEndTime() throws Exception {
    query.setStartTime(1356998400L);
    query.setEndTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeGreaterThanEndTime() throws Exception {
    query.setStartTime(1356998460L);
    query.setEndTime(1356998400L);
  }
  
  @Test
  public void getEndTimeNotSet() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357300800000L);
    assertEquals(1357300800000L, query.getEndTime());
  }
  
  @Test
  public void setTimeSeries() throws Exception {
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NullPointerException.class)
  public void setTimeSeriesNullTags() throws Exception {
    query.setTimeSeries(METRIC_STRING, null, Aggregators.SUM, false);
  }
  
  @Test
  public void setTimeSeriesEmptyTags() throws Exception {
    tags.clear();
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchMetric() throws Exception {
    query.setTimeSeries(NSUN_METRIC, tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagk() throws Exception {
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagv() throws Exception {
    tags.put(TAGK_STRING, NSUN_TAGV);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
  }

  @Test
  public void setTimeSeriesTS() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSNullList() throws Exception {
    query.setTimeSeries(null, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSEmptyList() throws Exception {
    final List<String> tsuids = new ArrayList<String>();
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSDifferentMetrics() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000002000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
}
