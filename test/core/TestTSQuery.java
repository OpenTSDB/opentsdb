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
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSQuery.class })
public final class TestTSQuery {

  @Test
  public void constructor() {
    assertNotNull(new TSQuery());
  }

  @Test
  public void validate() {
    TSQuery q = this.getMetricForValidate();
    q.validateAndSetQuery();
    assertEquals(1356998400000L, q.startTime());
    assertEquals(1356998460000L, q.endTime());
    assertEquals("sys.cpu.0", q.getQueries().get(0).getMetric());
    assertEquals("*", q.getQueries().get(0).getTags().get("host"));
    assertEquals("lga", q.getQueries().get(0).getTags().get("dc"));
    assertEquals(Aggregators.SUM, q.getQueries().get(0).aggregator());
    assertEquals(Aggregators.AVG, q.getQueries().get(0).downsampler());
    assertEquals(300000, q.getQueries().get(0).downsampleInterval());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart(null);
    q.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart("");
    q.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateInvalidStart() {
    TSQuery q = this.getMetricForValidate();
    q.setStart("Not a timestamp at all");
    q.validateAndSetQuery();
  }
  
  @Test
  public void validateNullEnd() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    TSQuery q = this.getMetricForValidate();
    q.setEnd(null);
    q.validateAndSetQuery();
    assertEquals(1357300800000L, q.endTime());
  }
  
  @Test
  public void validateEmptyEnd() {    
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    TSQuery q = this.getMetricForValidate();
    q.setEnd("");
    q.validateAndSetQuery();
    assertEquals(1357300800000L, q.endTime());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullQueries() {
    TSQuery q = this.getMetricForValidate();
    q.setQueries(null);
    q.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyQueries() {
    TSQuery q = this.getMetricForValidate();
    q.setQueries(new ArrayList<TSSubQuery>());
    q.validateAndSetQuery();
  }
  
  private TSQuery getMetricForValidate() {
    final TSQuery query = new TSQuery();
    query.setStart("1356998400");
    query.setEnd("1356998460");
    final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
    subs.add(TestTSSubQuery.getMetricForValidate());
    query.setQueries(subs);
    return query;
  }
}
