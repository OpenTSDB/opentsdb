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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestTSQuery {

  @Test
  public void convertQuery() throws Exception {
    TSQuery ts_query = new TSQuery();
    ts_query.setStart("1h-ago");
    ts_query.setEnd("5m-ago");
    
    TSSubQuery sub = new TSSubQuery();
    sub.setMetric("sys.cpu.idle");
    sub.setDownsample("60m-max");
    sub.setAggregator("sum");
    sub.setRate(true);
    sub.setRateOptions(RateOptions.newBuilder()
        .setCounter(true)
        .setCounterMax(1024)
        .build());
    sub.setFilters(Lists.newArrayList(new TagVFilter.Builder()
        .setFilter("*")
        .setType("wildcard")
        .setTagk("host")
        .setGroupBy(true)
        .build(),
        new TagVFilter.Builder()
        .setFilter("lga")
        .setType("literal_or")
        .setTagk("colo")
        .build()));
    sub.setExplicitTags(true);
    
    TSSubQuery sub2 = new TSSubQuery();
    sub2.setMetric("sys.cpu.busy");
    sub2.setAggregator("avg");
    sub2.setFilters(Lists.newArrayList(new TagVFilter.Builder()
        .setFilter("phx")
        .setType("literal_or")
        .setTagk("colo")
        .build()));
    sub2.setRate(false);
    ts_query.setQueries(Lists.newArrayList(sub, sub2));
    ts_query.validateAndSetQuery();
    
    TimeSeriesQuery query = TSQuery.convertQuery(ts_query);
    query.validate();
    assertEquals("1h-ago", query.getTime().getStart());
    assertEquals("5m-ago", query.getTime().getEnd());
    assertFalse(query.getTime().isRate());
    
    assertEquals(2, query.getFilters().size());
    assertEquals("f1", query.getFilters().get(0).getId());
    assertEquals(2, query.getFilters().get(0).getTags().size());
    assertEquals("host", query.getFilters().get(0).getTags().get(0).getTagk());
    assertEquals("*", query.getFilters().get(0).getTags().get(0).getFilter());
    assertEquals("wildcard", query.getFilters().get(0).getTags().get(0).getType());
    assertTrue(query.getFilters().get(0).getTags().get(0).isGroupBy());
    assertEquals("colo", query.getFilters().get(0).getTags().get(1).getTagk());
    assertEquals("lga", query.getFilters().get(0).getTags().get(1).getFilter());
    assertEquals("literal_or", query.getFilters().get(0).getTags().get(1).getType());
    assertFalse(query.getFilters().get(0).getTags().get(1).isGroupBy());
    assertTrue(query.getFilters().get(0).getExplicitTags());
    
    assertEquals("f2", query.getFilters().get(1).getId());
    assertEquals(1, query.getFilters().get(1).getTags().size());
    assertEquals("colo", query.getFilters().get(1).getTags().get(0).getTagk());
    assertEquals("phx", query.getFilters().get(1).getTags().get(0).getFilter());
    assertEquals("literal_or", query.getFilters().get(1).getTags().get(0).getType());
    assertFalse(query.getFilters().get(1).getTags().get(0).isGroupBy());
    assertFalse(query.getFilters().get(1).getExplicitTags());
    
    assertEquals(2, query.getMetrics().size());
    assertEquals("m1", query.getMetrics().get(0).getId());
    assertEquals("f1", query.getMetrics().get(0).getFilter());
    assertEquals("sys.cpu.idle", query.getMetrics().get(0).getMetric());
    assertEquals("sum", query.getMetrics().get(0).getAggregator());
    assertEquals("60m", query.getMetrics().get(0).getDownsampler().getInterval());
    assertEquals("max", query.getMetrics().get(0).getDownsampler().getAggregator());
    assertTrue(query.getMetrics().get(0).isRate());
    assertTrue(query.getMetrics().get(0).getRateOptions().isCounter());
    assertEquals(1024, query.getMetrics().get(0).getRateOptions().getCounterMax());
    
    assertEquals("m2", query.getMetrics().get(1).getId());
    assertEquals("f2", query.getMetrics().get(1).getFilter());
    assertEquals("sys.cpu.busy", query.getMetrics().get(1).getMetric());
    assertEquals("avg", query.getMetrics().get(1).getAggregator());
    assertNull(query.getMetrics().get(1).getDownsampler());
    assertFalse(query.getMetrics().get(1).isRate());
    assertNull(query.getMetrics().get(1).getRateOptions());
  }
}
