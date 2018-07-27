//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import net.opentsdb.query.filter.MetricLiteralFilter;

public class TestQuerySourceConfig {

  @Test
  public void builder() throws Exception {
    final TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    
    QuerySourceConfig qsc = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setStart("1h-ago")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setId("UT")
        .build();
    assertSame(query, qsc.getQuery());
    assertEquals("1h-ago", qsc.getStart());
    assertTrue(qsc.startTime().epoch() > 0);
    assertEquals("system.cpu.user", qsc.getMetric().metric());
    assertEquals("UT", qsc.getId());
    
    try {
      QuerySourceConfig.newBuilder()
        .setQuery(query)
        //.setStart("1h-ago")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setId("UT")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setStart("1h-ago")
        //.setMetric("system.cpu.user")
        .setId("UT")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
