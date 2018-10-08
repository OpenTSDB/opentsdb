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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Bytes;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestTSUID extends SchemaBase {

  private Schema schema;
  
  @Before
  public void before() throws Exception {
    schema = schema();
  }
  
  @Test
  public void ctorIllegals() throws Exception {
    try {
      new TSUID(null, schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TSUID(new byte[0], schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TSUID(new byte[2], schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TSUID(new byte[9], null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void badTSUIDs() throws Exception {
    // missing a tag value
    try {
      new TSUID(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES, 
          TAGK_B_BYTES /*, TAGV_B_BYTES */), schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // just a metric
    try {
      new TSUID(getTSUID(schema, METRIC_STRING), schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // truncation
    try {
      new TSUID(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES, 
          TAGK_B_BYTES, new byte[2]), schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorAndGetters() throws Exception {
    final byte[] key = getTSUID(schema, METRIC_STRING, TAGK_STRING, 
        TAGV_STRING, TAGK_B_STRING, TAGV_B_STRING);
    final TSUID tsuid = new TSUID(key, schema);
    assertTrue(tsuid.encoded());
    assertArrayEquals(key, tsuid.tsuid());
    assertNull(tsuid.alias());
    assertNull(tsuid.namespace());
    assertArrayEquals(METRIC_BYTES, tsuid.metric());
    assertTrue(tsuid.aggregatedTags().isEmpty());
    assertTrue(tsuid.disjointTags().isEmpty());
    assertEquals(Const.TS_BYTE_ID, tsuid.type());
    assertSame(schema, tsuid.dataStore());
    
    assertNull(tsuid.uids);
    ByteSet uids = tsuid.uniqueIds();
    assertEquals(1, uids.size());
    assertTrue(uids.contains(key));
    assertSame(uids, tsuid.uids);
    
    assertNull(tsuid.tags);
    ByteMap<byte[]> tags = tsuid.tags();
    assertEquals(2, tags.size());
    assertArrayEquals(TAGV_BYTES, tags.get(TAGK_BYTES));
    assertArrayEquals(TAGV_B_BYTES, tags.get(TAGK_B_BYTES));
    assertSame(tags, tsuid.tags);
  }

  @Test
  public void decode() throws Exception {
    TSUID tsuid = new TSUID(getTSUID(schema, METRIC_STRING, TAGK_STRING, 
        TAGV_STRING), schema());
    
    assertNull(tsuid.string_id);
    TimeSeriesStringId id = tsuid.decode(false, null).join();
    assertEquals(METRIC_STRING, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals(TAGV_STRING, id.tags().get(TAGK_STRING));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    assertNull(tsuid.string_id);
    
    tsuid = new TSUID(getTSUID(schema, METRIC_STRING, TAGK_STRING, 
        TAGV_STRING, TAGK_B_STRING, TAGV_B_STRING), schema());
    
    id = tsuid.decode(true, null).join();
    assertEquals(METRIC_STRING, id.metric());
    assertEquals(2, id.tags().size());
    assertEquals(TAGV_STRING, id.tags().get(TAGK_STRING));
    assertEquals(TAGV_B_STRING, id.tags().get(TAGK_B_STRING));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    assertSame(id, tsuid.string_id);
    
    trace = new MockTrace(true);
    tsuid = new TSUID(getTSUID(schema, METRIC_STRING, TAGK_STRING, 
        TAGV_STRING, TAGK_B_STRING, TAGV_B_STRING), schema());
    id = tsuid.decode(false, trace.newSpan("UT").start()).join();
    assertEquals(METRIC_STRING, id.metric());
    assertEquals(2, id.tags().size());
    assertEquals(TAGV_STRING, id.tags().get(TAGK_STRING));
    assertEquals(TAGV_B_STRING, id.tags().get(TAGK_B_STRING));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    verifySpan(TSUID.class.getName() + ".decode", 4);
  }
  
  @Test
  public void decodeNoSuchMetric() throws Exception {
    TSUID tsuid = new TSUID(Bytes.concat(NSUI_METRIC, TAGK_BYTES, 
        TAGV_BYTES, TAGK_B_BYTES, TAGV_B_BYTES), schema());
    
    Deferred<TimeSeriesStringId> deferred = tsuid.decode(false, null);
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { 
      assertEquals(Schema.METRIC_TYPE, e.kind());
      assertArrayEquals(NSUI_METRIC, e.id());
    }
    
    trace = new MockTrace(true);
    deferred = tsuid.decode(false, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { }
    verifySpan(TSUID.class.getName() + ".decode", NoSuchUniqueId.class, 4);
  }
  
  @Test
  public void decodeNoSuchTagK() throws Exception {
    TSUID tsuid = new TSUID(Bytes.concat(METRIC_BYTES, TAGK_BYTES, 
        TAGV_BYTES, NSUI_TAGK, TAGV_B_BYTES), schema());
    
    Deferred<TimeSeriesStringId> deferred = tsuid.decode(false, null);
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { 
      assertEquals(Schema.TAGK_TYPE, e.kind());
      assertArrayEquals(NSUI_TAGK, e.id());
    }
    
    trace = new MockTrace(true);
    deferred = tsuid.decode(false, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { }
    verifySpan(TSUID.class.getName() + ".decode", NoSuchUniqueId.class, 4);
  }
  
  @Test
  public void decodeNoSuchTagV() throws Exception {
    TSUID tsuid = new TSUID(Bytes.concat(METRIC_BYTES, TAGK_BYTES, 
        TAGV_BYTES, TAGK_B_BYTES, NSUI_TAGV), schema());
    
    Deferred<TimeSeriesStringId> deferred = tsuid.decode(false, null);
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { 
      assertEquals(Schema.TAGV_TYPE, e.kind());
      assertArrayEquals(NSUI_TAGV, e.id());
    }
    
    trace = new MockTrace(true);
    deferred = tsuid.decode(false, trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected NoSuchUniqueId");
    } catch (NoSuchUniqueId e) { }
    verifySpan(TSUID.class.getName() + ".decode", NoSuchUniqueId.class, 4);
  }
  
  @Test
  public void buildHashCode() throws Exception {
    TSUID tsuid = new TSUID(Bytes.concat(METRIC_BYTES, TAGK_BYTES, 
        TAGV_BYTES, TAGK_B_BYTES, NSUI_TAGV), schema());
    assertEquals(-5870990467325021724L, tsuid.buildHashCode());
  }
}
