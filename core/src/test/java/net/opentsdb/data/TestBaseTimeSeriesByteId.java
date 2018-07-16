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
package net.opentsdb.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestBaseTimeSeriesByteId {

  private static final byte[] ARRAY = new byte[] { 'f', 'o', 'o' };
  private static final byte[] EMPTY_ARRAY = new byte[] { };
  private static final ByteMap<byte[]> TAGS = new ByteMap<byte[]>();
  {
    TAGS.put(new byte[] { 'k', '1' }, new byte[] { 'v', '1' });
    TAGS.put(new byte[] { 'k', '2' }, new byte[] { 'v', '2' });
  }
  private static final List<byte[]> LIST = Lists.newArrayList(
      new byte[] { 'l', '1' },
      new byte[] { 'l', '2' });
  private static final ByteSet SET = new ByteSet();
  {
    SET.add(new byte[] { 's', '1' });
    SET.add(new byte[] { 's', '2' });
  }
  private ReadableTimeSeriesDataStore data_store;
  
  @Before
  public void before() throws Exception {
    data_store = mock(ReadableTimeSeriesDataStore.class);
  }
  
  @Test
  public void builder() throws Exception {
    try {
      BaseTimeSeriesByteId.newBuilder(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    BaseTimeSeriesByteId.Builder builder = 
        BaseTimeSeriesByteId.newBuilder(data_store);
    
    assertFalse(builder.encoded);
    builder.setEncoded(true);
    assertTrue(builder.encoded);
    
    assertNull(builder.alias);
    builder.setAlias(ARRAY);
    assertArrayEquals(ARRAY, builder.alias);
    builder.setAlias(EMPTY_ARRAY);
    assertArrayEquals(EMPTY_ARRAY, builder.alias);
    builder.setAlias(null);
    assertNull(builder.alias);
    
    assertNull(builder.namespace);
    builder.setNamespace(ARRAY);
    assertArrayEquals(ARRAY, builder.namespace);
    builder.setNamespace(EMPTY_ARRAY);
    assertArrayEquals(EMPTY_ARRAY, builder.namespace);
    builder.setNamespace(null);
    assertNull(builder.namespace);
    
    assertNull(builder.metric);
    builder.setMetric(ARRAY);
    assertArrayEquals(ARRAY, builder.metric);
    builder.setMetric(EMPTY_ARRAY);
    assertArrayEquals(EMPTY_ARRAY, builder.metric);
    builder.setMetric(null);
    assertNull(builder.metric);
    
    assertNull(builder.tags);
    builder.setTags(TAGS);
    assertSame(TAGS, builder.tags);
    builder.setTags(null);
    assertNull(builder.tags);
    builder.addTags(new byte[] { 'k', '1' }, new byte[] { 'v', '1' });
    assertEquals(1, builder.tags.size());
    assertNotSame(TAGS, builder.tags);
    builder.setTags(TAGS);
    assertSame(TAGS, builder.tags);
    
    assertNull(builder.aggregated_tags);
    builder.setAggregatedTags(LIST);
    assertSame(LIST, builder.aggregated_tags);
    builder.setAggregatedTags(null);
    assertNull(builder.aggregated_tags);
    builder.addAggregatedTag(new byte[] { 'l', '1' });
    assertEquals(1, builder.aggregated_tags.size());
    
    assertNull(builder.disjoint_tags);
    builder.setDisjointTags(LIST);
    assertSame(LIST, builder.disjoint_tags);
    builder.setDisjointTags(null);
    assertNull(builder.disjoint_tags);
    builder.addDisjointTag(new byte[] { 'l', '1' });
    assertEquals(1, builder.disjoint_tags.size());
    
    assertNull(builder.unique_ids);
    builder.setUniqueId(SET);
    assertSame(SET, builder.unique_ids);
    builder.setUniqueId(null);
    assertNull(builder.unique_ids);
    builder.addUniqueId(new byte[] { 's', '1' });
    assertEquals(1, builder.unique_ids.size());
  }

  @Test
  public void build() throws Exception {
    BaseTimeSeriesByteId id = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertArrayEquals(ARRAY, id.alias());
    assertArrayEquals(ARRAY, id.namespace());
    assertArrayEquals(ARRAY, id.metric());
    assertEquals(TAGS, id.tags());
    assertEquals(LIST, id.aggregatedTags());
    assertEquals(LIST, id.disjointTags());
    assertEquals(SET, id.uniqueIds());
    
    try {
      id = BaseTimeSeriesByteId.newBuilder(data_store)
          .setAlias(ARRAY)
          .setNamespace(ARRAY)
          //.setMetric(ARRAY) no metric
          .setTags(TAGS)
          .setAggregatedTags(LIST)
          .setDisjointTags(LIST)
          .setUniqueId(SET)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesByteId.newBuilder(data_store)
          .setAlias(ARRAY)
          .setNamespace(ARRAY)
          .setMetric(ARRAY)
          .addTags(ARRAY, null) // null tag value
          .setAggregatedTags(LIST)
          .setDisjointTags(LIST)
          .setUniqueId(SET)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      id = BaseTimeSeriesByteId.newBuilder(data_store)
          .setAlias(ARRAY)
          .setNamespace(ARRAY)
          .setMetric(ARRAY)
          .setTags(TAGS)
          .setAggregatedTags(Lists.newArrayList(new byte[] { 'h', 'i' }, null))
          .setDisjointTags(LIST)
          .setUniqueId(SET)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      id = BaseTimeSeriesByteId.newBuilder(data_store)
          .setAlias(ARRAY)
          .setNamespace(ARRAY)
          .setMetric(ARRAY)
          .setTags(TAGS)
          .setAggregatedTags(LIST)
          .setDisjointTags(Lists.newArrayList(new byte[] { 'h', 'i' }, null))
          .setUniqueId(SET)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final BaseTimeSeriesByteId id1 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    BaseTimeSeriesByteId id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    final byte[] array2 = new byte[] { 'm', 'e', 'h' };
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(array2)  // <-- diff
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        //.setAlias(ARRAY)  // <-- diff
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(array2)  // <-- diff
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        //.setNamespace(ARRAY)  // <-- diff
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(array2)  // <-- diff
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    ByteMap<byte[]> tags2 = new ByteMap<byte[]>();
    tags2.put(new byte[] { 'k', '2' }, new byte[] { 'v', '2' });
    tags2.put(new byte[] { 'k', '1' }, new byte[] { 'v', '1' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(tags2) // <-- diff order kinda sorta
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    tags2.put(new byte[] { 'k', '3' }, new byte[] { 'v', '3' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(tags2)  // <-- diff size
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        //.setTags(TAGS)  // <-- diff
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    List<byte[]> list2 = Lists.newArrayList(
        new byte[] { 'l', '2' },
        new byte[] { 'l', '1' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(list2)  // <-- diff order
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    list2.add(new byte[] { 'l', '3' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(list2)  // <-- diff
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        //.setAggregatedTags(LIST)  // <-- diff
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    list2.remove(2);
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(list2)  // <-- diff order
        .setUniqueId(SET)
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    list2.add(new byte[] { 'l', '3' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(list2)  // <-- diff
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        //.setDisjointTags(LIST)  // <-- diff
        .setUniqueId(SET)
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    ByteSet set2 = new ByteSet();
    set2.add(new byte[] { 's', '2' });
    set2.add(new byte[] { 's', '1' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(set2)  // <-- diff order sorta
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    set2.add(new byte[] { 's', '3' });
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(set2)  // <-- diff size
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        //.setUniqueId(SET)  // <-- diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
  }

  @Test
  public void decode() throws Exception {
    when(data_store.resolveByteId(any(TimeSeriesByteId.class), any(Span.class)))
      .thenReturn(Deferred.fromResult(null));
    final BaseTimeSeriesByteId id1 = BaseTimeSeriesByteId.newBuilder(data_store)
        .setAlias(ARRAY)
        .setNamespace(ARRAY)
        .setMetric(ARRAY)
        .setTags(TAGS)
        .setAggregatedTags(LIST)
        .setDisjointTags(LIST)
        .setUniqueId(SET)
        .build();
    assertNull(id1.decode(false, null).join());
  }
}
