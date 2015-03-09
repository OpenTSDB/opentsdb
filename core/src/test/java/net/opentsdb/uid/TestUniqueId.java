// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import java.io.IOException;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class TestUniqueId {
  private TsdbStore client;
  private UniqueId uid;
  private MetricRegistry metrics;
  private EventBus idEventBus;

  @Before
  public void setUp() throws IOException{
    ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore());
    client = objectGraph.get(TsdbStore.class);

    metrics = new MetricRegistry();
    idEventBus = mock(EventBus.class);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTsdbStore() {
    uid = new UniqueId(null, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTable() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoType() {
    uid = new UniqueId(client, null, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoEventbus() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, null);
  }
  
  @Test
  public void getNameSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected=NoSuchUniqueId.class)
  public void getNameForNonexistentId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(new byte[]{1, 2, 3}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test(expected=IllegalArgumentException.class)
  public void getNameWithInvalidId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(new byte[]{1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getIdSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit too ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  // The table contains IDs encoded on 2 bytes but the instance wants 3.
  @Test(expected=IllegalStateException.class)
  public void getIdMisconfiguredWidth() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 'a', 0x42 };
    client.allocateUID("foo", id, UniqueIdType.METRIC);

    uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test(expected=NoSuchUniqueName.class)
  public void getIdForNonexistentName() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void createIdWithExistingId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final byte[] id = { 0, 0, 1};
    uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    try {
      uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch(Exception e) {
      assertEquals("A UID with name foo already exists", e.getMessage());
    }
    // Should be a cache hit ...
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test  // Test the creation of an ID with no problem.
  public void createIdIdWithSuccess() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    // Due to the implementation in the memoryStore used for testing the first
    // call will always return 1
    final byte[] id = { 0, 0, 1 };

    assertArrayEquals(id, uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    // Should be a cache hit since we created that entry.
    assertArrayEquals(id, uid.getId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
    // Should be a cache hit too for the same reason.
    assertEquals("foo", uid.getName(id).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(0, counters.get("uid.cache-miss:kind=metrics").getCount());
  }

  @Test
  public void createIdPublishesEventOnSuccess() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    verify(idEventBus).post(any(IdCreatedEvent.class));
  }

  @Test
  public void testResolveIdWildcardNull() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    assertNull(uid.resolveId(null).joinUninterruptibly());
  }

  @Test
  public void testResolveIdWildcardEmpty() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    assertNull(uid.resolveId("").joinUninterruptibly());
  }

  @Test
  public void testResolveIdWildcardStar() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    assertNull(uid.resolveId("*").joinUninterruptibly());
  }

  @Test
  public void testResolveIdGetsId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    byte[] id = client.allocateUID("nameexists", UniqueIdType.METRIC)
        .joinUninterruptibly();
    assertArrayEquals(id, uid.resolveId("*").joinUninterruptibly());
  }

  @Test(expected = NoSuchUniqueName.class)
  public void testResolveIdGetsMissingId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.resolveId("nosuchname").joinUninterruptibly();
  }
}
