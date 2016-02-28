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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;

import org.junit.Test;

public final class TestTSSubQuery {

  @Test
  public void constructor() {
    assertNotNull(new TSSubQuery());
  }

  @Test
  public void validate() {
    TSSubQuery sub = getMetricForValidate();
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("wildcard(*)", sub.getTags().get("host"));
    assertEquals("literal_or(lga)", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test
  public void validateTS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("ABCD");
    sub.setTsuids(tsuids);
    sub.validateAndSetQuery();
    assertNotNull(sub.getTsuids());
    assertEquals("wildcard(*)", sub.getTags().get("host"));
    assertEquals("literal_or(lga)", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test
  public void validateNoDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample(null);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("wildcard(*)", sub.getTags().get("host"));
    assertEquals("literal_or(lga)", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertNull(sub.downsampler());
    assertEquals(0, sub.downsampleInterval());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("Notanagg");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrEmptyTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(new ArrayList<String>());
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample("bad");
    sub.validateAndSetQuery();
  }
  
  @Test
  public void validateWithFilter() {
    TSSubQuery sub = getMetricForValidate();
    sub.setFilters(Arrays.asList(TagVFilter.Builder()
        .setFilter("*nari").setType("wildcard").setTagk("host").build()));
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertEquals(0, sub.getTags().size());
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test
  public void validateWithFilterViaTags() {
    TSSubQuery sub = getMetricForValidate();
    
    final Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", TagVWildcardFilter.FILTER_NAME + "(*nari)");
    sub.setTags(tags);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertEquals("wildcard(*nari)", sub.getTags().get("host"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test
  public void validateGroupByFilterMissingParensViaTags() {
    TSSubQuery sub = getMetricForValidate();
    final Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", TagVWildcardFilter.FILTER_NAME);
    sub.setTags(tags);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVLiteralOrFilter);
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }

  @Test
  public void validateWithGroupByFilter() {
    TSSubQuery sub = getMetricForValidate();
    sub.setFilters(Arrays.asList(TagVFilter.Builder()
        .setFilter("*nari").setType("wildcard").setTagk("host")
        .setGroupBy(true).build()));
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("wildcard(*nari)", sub.getTags().get("host"));
    assertEquals(1, sub.getFilters().size());
    assertTrue(sub.getFilters().get(0) instanceof TagVWildcardFilter);
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }

  @Test
  public void validateWithFilterAndGroupByFilter() {
    TSSubQuery sub = getMetricForValidate();
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("colo", "lga*"));
    sub.setFilters(filters);
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", TagVWildcardFilter.FILTER_NAME + "(*nari)");
    sub.setTags(tags);
    
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals(TagVWildcardFilter.FILTER_NAME + "(*nari)", 
        sub.getTags().get("host"));
    assertEquals(1, sub.getFilters().size());
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test
  public void validateWithFilterAndGroupByFilterSameTag() {
    TSSubQuery sub = getMetricForValidate();
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "veti*"));
    sub.setFilters(filters);
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", TagVWildcardFilter.FILTER_NAME + "(*nari)");
    sub.setTags(tags);
    
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals(TagVWildcardFilter.FILTER_NAME + "(*nari)", 
        sub.getTags().get("host"));
    assertEquals(1, sub.getFilters().size());
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300000, sub.downsampleInterval());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateWithDownsampleNone() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample("1m-none");
    sub.validateAndSetQuery();
  }
  
  // NOTE: Each of the hash and equals  tests should make sure that we the code
  // doesn't change after validation.
  
  @Test
  public void testHashCodeandEqualsAggregator() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setAggregator("max");
    final int has_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(has_b, sub1.hashCode());
    
    final TSSubQuery sub2 = getBaseQuery();
    sub2.setAggregator("max");
    
    assertEquals(has_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsAggregatorNull() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setAggregator(null);
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsAggregatorNonExistant() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setAggregator("nosuchagg");
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testHashCodeandEqualsMetric() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setMetric("foo");
    assertEquals(hash_a, sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_a, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setMetric("foo");
    
    assertEquals(hash_a, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsMetricNull() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();
    
    sub1.setMetric(null);
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testHashCodeandEqualsTSUIDs() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("01010101");
    tsuids.add("01010102");
    sub1.setTsuids(tsuids);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    List<String> tsuids2 = new ArrayList<String>(2);
    tsuids2.add("01010101");
    tsuids2.add("01010102");
    sub2.setTsuids(tsuids2);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsTSUIDsChange() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();
    List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("01010101");
    tsuids.add("01010102");
    sub1.setTsuids(tsuids);
    
    tsuids.set(1, "01010103");
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    List<String> tsuids2 = new ArrayList<String>(2);
    tsuids2.add("01010101");
    tsuids2.add("01010103");
    sub2.setTsuids(tsuids2);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsTag() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web02");
    sub1.setTags(tags);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    tags = new HashMap<String, String>();
    tags.put("host", "web02");
    sub2.setTags(tags);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsTags() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web02");
    tags.put("foo", "bar");
    sub1.setTags(tags);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    tags = new HashMap<String, String>();
    tags.put("host", "web02");
    tags.put("foo", "bar");
    sub2.setTags(tags);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsTagsNull() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setTags(null);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setTags(null);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsDownsampler() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setDownsample("1h-avg");
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setDownsample("1h-avg");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsDownsamplerInvalid() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setDownsample("bad ds");
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testHashCodeandEqualsRate() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRate(false);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setRate(false);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsRateOptionsSameNew() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRateOptions(new RateOptions(true, 1024, 16));
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
 
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsRateOptionsNotCounter() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRateOptions(new RateOptions(false, 1024, 16));
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setRateOptions(new RateOptions(false, 1024, 16));
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsRateOptionsNewMax() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRateOptions(new RateOptions(true, 768, 16));
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setRateOptions(new RateOptions(true, 768, 16));
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsRateOptionsNewReset() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRateOptions(new RateOptions(true, 1024, 32));
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setRateOptions(new RateOptions(true, 1024, 32));
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsRateOptionsNull() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setRateOptions(null);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setRateOptions(null);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }

  @Test
  public void testHashCodeandEqualsExplicitTags() {
    final TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();

    sub1.setExplicitTags(true);
    final int hash_b = sub1.hashCode();
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setExplicitTags(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testEqualsNull() {
    final TSSubQuery sub1 = getBaseQuery();
    assertFalse(sub1.equals(null));
  }
  
  @Test
  public void testEqualsWrongType() {
    final TSSubQuery sub1 = getBaseQuery();
    assertFalse(sub1.equals(new String("Foobar")));
  }
  
  @Test
  public void testEqualsSame() {
    final TSSubQuery sub1 = getBaseQuery();
    assertTrue(sub1.equals(sub1));
  }

  @Test
  public void testHashCodeandEqualsFilter() {
    TSSubQuery sub1 = getBaseQuery();
    final int hash_a = sub1.hashCode();
    sub1.setFilters(Arrays.asList(TagVFilter.Builder()
        .setFilter("*nari").setType("wildcard").setTagk("host")
        .setGroupBy(true).build()));    
    assertFalse(hash_a == sub1.hashCode());
    sub1.validateAndSetQuery();
    final int has_b = sub1.hashCode();
    assertEquals(has_b, sub1.hashCode());
    
    TSSubQuery sub2 = getBaseQuery();
    sub2.setFilters(Arrays.asList(TagVFilter.Builder()
        .setFilter("*nari").setType("wildcard").setTagk("host")
        .setGroupBy(true).build()));    
    sub2.validateAndSetQuery();
    
    assertEquals(has_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  /** @return a sub query object with some defaults set for testing */
  public static TSSubQuery getBaseQuery() {
    TSSubQuery query = new TSSubQuery();
    query.setAggregator("sum");
    query.setMetric("foo");
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("host", "web01");
    tags.put("dc", "lax");
    query.setTags(tags);
    query.setRate(true);
    query.setRateOptions(new RateOptions(true, 1024, 16));
    return query;
  }
  
  /**
   * Sets up an object with good, common values for testing the validation
   * function with an "m" type query (no tsuids). Each test can "set" the 
   * method it wants to fool with and call .validateAndSetQuery()
   * <b>Warning:</b> This method is also shared by {@link TestTSQuery} so be
   * careful if you change any values
   * @return A sub query object
   */
  public static TSSubQuery getMetricForValidate() {
    final TSSubQuery sub = new TSSubQuery();
    sub.setAggregator("sum");
    sub.setDownsample("5m-avg");
    sub.setMetric("sys.cpu.0");
    sub.setRate(false);
    final HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "*");
    tags.put("dc", "lga");
    sub.setTags(tags);
    return sub;
  }
}
