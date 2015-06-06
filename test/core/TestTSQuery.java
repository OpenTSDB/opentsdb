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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;

import net.opentsdb.utils.DateTime;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSQuery.class, DateTime.class })
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
    assertEquals("wildcard(*)", q.getQueries().get(0).getTags().get("host"));
    assertEquals("literal_or(lga)", q.getQueries().get(0).getTags().get("dc"));
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
  
  // NOTE: Each of the hash and equals  tests should make sure that we the code
  // doesn't change after validation.
  
  @Test
  public void testHashCodeandEqualsStart() {
    TSQuery sub1 = getMetricForValidate();
    final int hash_a = sub1.hashCode();
    sub1.setStart("1356998300");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setStart("1356998300");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsStartNull() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setStart(null);
    assertTrue(hash_a != sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsStartInvalid() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setStart("1h-ago");
    assertTrue(hash_a != sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testHashCodeandEqualsEnd() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setEnd("1356998490");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setEnd("1356998490");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsEndNull() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setEnd(null);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    // this is ok since we assume "now" if end is missing
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setEnd(null);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsEndInvalid() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setEnd("1356998300");
    assertTrue(hash_a != sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testHashCodeandEqualsTimezone() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setTimezone("America/New_York");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setTimezone("America/New_York");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsTimezoneInvalid() throws Exception {
    // silly test isn't calling into the real method, mockit!
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString()))
      .thenThrow(new IllegalArgumentException("Invalid timezone"));
    
    TSQuery sub1 = getMetricForValidate();
  
    final int hash_a = sub1.hashCode();
    sub1.setTimezone("Not a timezone");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setTimezone("Not a timezone");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsOptions() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    HashMap<String, ArrayList<String>> options = 
        new HashMap<String, ArrayList<String>>(2);
    ArrayList<String> params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    sub1.setOptions(options);
    
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();

    options = new HashMap<String, ArrayList<String>>(2);
    params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    sub2.setOptions(options);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsOptionsNewPut() {
    TSQuery sub1 = getMetricForValidate();
    HashMap<String, ArrayList<String>> options = 
        new HashMap<String, ArrayList<String>>(3);
    ArrayList<String> params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    sub1.setOptions(options);
    
    final int hash_a = sub1.hashCode();

    params = new ArrayList<String>(1);
    params.add("top");
    options.put("key", params);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();

    options = new HashMap<String, ArrayList<String>>(2);
    params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    params = new ArrayList<String>(1);
    params.add("top");
    options.put("key", params);
    sub2.setOptions(options);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsOptionsNewParam() {
    TSQuery sub1 = getMetricForValidate();
    HashMap<String, ArrayList<String>> options = 
        new HashMap<String, ArrayList<String>>(3);
    ArrayList<String> params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    sub1.setOptions(options);
    
    final int hash_a = sub1.hashCode();

    params = new ArrayList<String>(1);
    params.add("cycles");
    options.put("label", params);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();

    options = new HashMap<String, ArrayList<String>>(2);
    params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("cycles");
    options.put("label", params);
    params = new ArrayList<String>(1);;
    sub2.setOptions(options);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsOptionsExtraParam() {
    TSQuery sub1 = getMetricForValidate();
    HashMap<String, ArrayList<String>> options = 
        new HashMap<String, ArrayList<String>>(3);
    ArrayList<String> params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    options.put("label", params);
    sub1.setOptions(options);
    
    final int hash_a = sub1.hashCode();

    options.get("label").add("extra");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();

    options = new HashMap<String, ArrayList<String>>(2);
    params = new ArrayList<String>(1);
    params.add("1419x576");
    options.put("wxh", params);
    params = new ArrayList<String>(1);
    params.add("latency");
    params.add("extra");
    options.put("label", params);
    params = new ArrayList<String>(1);;
    sub2.setOptions(options);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsPadding() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setPadding(true);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setPadding(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsNoAnnotations() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setNoAnnotations(true);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setNoAnnotations(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsWithGlobalAnnotations() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setGlobalAnnotations(true);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setGlobalAnnotations(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsShowTSUIDs() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setShowTSUIDs(true);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setShowTSUIDs(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsMSResolution() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setMsResolution(true);
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.setMsResolution(true);
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
   
  @Test
  public void testHashCodeandEqualsNewSubQuery() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.getQueries().add(TestTSSubQuery.getBaseQuery());
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.getQueries().add(TestTSSubQuery.getBaseQuery());
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test
  public void testHashCodeandEqualsChangeSubQuery() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.getQueries().get(0).setMetric("foo");
    final int hash_b = sub1.hashCode();
    assertTrue(hash_a != hash_b);
    sub1.validateAndSetQuery();
    assertEquals(hash_b, sub1.hashCode());
    
    TSQuery sub2 = getMetricForValidate();
    sub2.getQueries().get(0).setMetric("foo");
    
    assertEquals(hash_b, sub2.hashCode());
    assertEquals(sub1, sub2);
    assertFalse(sub1 == sub2);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsEmptySubQueries() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setQueries(new ArrayList<TSSubQuery>(0));
    assertTrue(hash_a != sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testHashCodeandEqualsNullSubQueries() {
    TSQuery sub1 = getMetricForValidate();
    
    final int hash_a = sub1.hashCode();
    sub1.setQueries(null);
    assertTrue(hash_a != sub1.hashCode());
    sub1.validateAndSetQuery();
  }
  
  @Test
  public void testEqualsNull() {
    TSQuery sub1 = getMetricForValidate();
    assertFalse(sub1.equals(null));
  }
  
  @Test
  public void testEqualsWrongType() {
    TSQuery sub1 = getMetricForValidate();
    assertFalse(sub1.equals(new String("Foobar")));
  }
  
  @Test
  public void testEqualsSame() {
    TSQuery sub1 = getMetricForValidate();
    assertTrue(sub1.equals(sub1));
  }
  
  /**
   * Sets up an object with good, common values for testing the validation
   * function with an query string query. Each test can "set" the 
   * method it wants to fool with and call .validateAndSetQuery()
   * <b>Warning:</b> This method calls into {@link TestTSQuery}
   * @return A query object
   */
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
