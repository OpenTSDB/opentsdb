// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.util.List;

import com.google.common.collect.ImmutableSortedSet;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.utils.ByteArrayPair;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.uid.UniqueIdType;

import javax.inject.Inject;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class TsdbStoreTimeSeriesQueryTest {
  private static final byte[] SYS_CPU_USER_ID = new byte[]{0, 0, 1};
  private static final byte[] SYS_CPU_NICE_ID = new byte[]{0, 0, 2};
  private static final byte[] SYS_CPU_IDLE_ID = new byte[]{0, 0, 3};
  private static final byte[] NO_VALUES_ID = new byte[]{0, 0, 11};

  private static final byte[] TAGK_HOST_ID = new byte[]{0, 0, 1};
  private static final byte[] TAGK_OWNER_ID = new byte[]{0, 0, 4};

  private static final byte[] TAGV_WEB01_ID = new byte[]{0, 0, 1};
  private static final byte[] TAGV_WEB02_ID = new byte[]{0, 0, 2};

  // tsuids
  private static final byte[][] test_tsuids = {
      new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
      new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 2},
      new byte[]{0, 0, 2, 0, 0, 1, 0, 0, 1},
      new byte[]{0, 0, 3, 0, 0, 1, 0, 0, 1, 0, 0, 4, 0, 0, 5},
      new byte[]{0, 0, 3, 0, 0, 1, 0, 0, 2, 0, 0, 4, 0, 0, 5},
      new byte[]{0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 1, 0, 0, 9, 0, 0, 3},
      new byte[]{0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 10, 0, 0, 9, 0, 0, 3}
  };

  @Inject TsdbStore tsdb_store;

  @Before
  public void setUp() throws Exception {
    tsdb_store.allocateUID("sys.cpu.user", SYS_CPU_USER_ID, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.nice", SYS_CPU_NICE_ID, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.idle", SYS_CPU_IDLE_ID, UniqueIdType.METRIC);
    tsdb_store.allocateUID("no.values", NO_VALUES_ID, UniqueIdType.METRIC);

    tsdb_store.allocateUID("host", TAGK_HOST_ID, UniqueIdType.TAGK);
    tsdb_store.allocateUID("owner", TAGK_OWNER_ID, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", TAGV_WEB01_ID, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", TAGV_WEB02_ID, UniqueIdType.TAGV);
  }

  @Test
  public void metricOnlyMeta() throws Exception {
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(SYS_CPU_USER_ID, ImmutableSortedSet.<ByteArrayPair>of());
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[1], tsuids.get(1));
  }
  
  // returns everything
  @Test
  public void metricOnlyMetaStar() throws Exception {
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(null, ImmutableSortedSet.<ByteArrayPair>of());
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(7, tsuids.size());
  }
  
  @Test
  public void metricOnly2Meta() throws Exception {
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(SYS_CPU_NICE_ID, ImmutableSortedSet.<ByteArrayPair>of());
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[2], tsuids.get(0));
  }
  
  @Test
  public void metricOnlyNoValuesMeta() throws Exception {
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(NO_VALUES_ID, ImmutableSortedSet.<ByteArrayPair>of());
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void tagkOnlyMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(TAGK_HOST_ID, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids[i], tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnly2Meta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(TAGK_OWNER_ID, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[3], tsuids.get(0));
    assertArrayEquals(test_tsuids[4], tsuids.get(1));
  }
  
  @Test
  public void tagvOnlyMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(null, TAGV_WEB01_ID));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[2], tsuids.get(1));
    assertArrayEquals(test_tsuids[3], tsuids.get(2));
    assertArrayEquals(test_tsuids[5], tsuids.get(3));
  }
  
  @Test
  public void tagvOnly2Meta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(null, TAGV_WEB02_ID));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[1], tsuids.get(0));
    assertArrayEquals(test_tsuids[4], tsuids.get(1));
  }
  
  @Test
  public void metricAndTagkMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(TAGK_HOST_ID, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(SYS_CPU_NICE_ID, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[2], tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(null, TAGV_WEB02_ID));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(SYS_CPU_IDLE_ID, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[4], tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(TAGK_HOST_ID, TAGV_WEB01_ID));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(SYS_CPU_IDLE_ID, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[3], tsuids.get(0));
  }
  
  @Test
  public void tagPairOnlyMeta() throws Exception {
    ImmutableSortedSet<ByteArrayPair> tags =
        ImmutableSortedSet.of(new ByteArrayPair(TAGK_HOST_ID, TAGV_WEB01_ID));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = tsdb_store.executeTimeSeriesQuery(query)
        .joinUninterruptibly();

    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[2], tsuids.get(1));
    assertArrayEquals(test_tsuids[3], tsuids.get(2));
  }
}
