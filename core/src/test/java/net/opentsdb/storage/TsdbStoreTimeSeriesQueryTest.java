package net.opentsdb.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Pair;

import com.google.common.collect.ImmutableSortedSet;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import javax.inject.Inject;

public abstract class TsdbStoreTimeSeriesQueryTest {
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
  @Inject TsdbStore store;
  private LabelId sysCpuUserId;
  private LabelId sysCpuNiceId;
  private LabelId sysCpuIdleId;
  private LabelId noValuesId;
  private LabelId tagkHostId;
  private LabelId tagkOwnerId;
  private LabelId tagvWeb01Id;
  private LabelId tagvWeb02Id;

  @Before
  public void setUp() throws Exception {
    sysCpuUserId = store.allocateUID("sys.cpu.user", UniqueIdType.METRIC).get();
    sysCpuNiceId = store.allocateUID("sys.cpu.nice", UniqueIdType.METRIC).get();
    sysCpuIdleId = store.allocateUID("sys.cpu.idle", UniqueIdType.METRIC).get();
    noValuesId = store.allocateUID("no.values", UniqueIdType.METRIC).get();

    tagkHostId = store.allocateUID("host", UniqueIdType.TAGK).get();
    tagkOwnerId = store.allocateUID("owner", UniqueIdType.TAGK).get();

    tagvWeb01Id = store.allocateUID("web01", UniqueIdType.TAGV).get();
    tagvWeb02Id = store.allocateUID("web02", UniqueIdType.TAGV).get();
  }

  @Test
  public void metricOnlyMeta() throws Exception {
    final ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of();
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(sysCpuUserId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[1], tsuids.get(1));
  }

  // returns everything
  @Test
  public void metricOnlyMetaStar() throws Exception {
    final ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of();
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(7, tsuids.size());
  }

  @Test
  public void metricOnly2Meta() throws Exception {
    final ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of();
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(sysCpuNiceId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[2], tsuids.get(0));
  }

  @Test
  public void metricOnlyNoValuesMeta() throws Exception {
    final ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of();
    final ResolvedSearchQuery query =
        new ResolvedSearchQuery(noValuesId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }

  @Test
  public void tagkOnlyMeta() throws Exception {
    final ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(tagkHostId, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids[i], tsuids.get(i));
    }
  }

  @Test
  public void tagkOnly2Meta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(tagkOwnerId, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[3], tsuids.get(0));
    assertArrayEquals(test_tsuids[4], tsuids.get(1));
  }

  @Test
  public void tagvOnlyMeta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(null, tagvWeb01Id));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[2], tsuids.get(1));
    assertArrayEquals(test_tsuids[3], tsuids.get(2));
    assertArrayEquals(test_tsuids[5], tsuids.get(3));
  }

  @Test
  public void tagvOnly2Meta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(null, tagvWeb02Id));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids[1], tsuids.get(0));
    assertArrayEquals(test_tsuids[4], tsuids.get(1));
  }

  @Test
  public void metricAndTagkMeta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(tagkHostId, null));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(sysCpuNiceId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[2], tsuids.get(0));
  }

  @Test
  public void metricAndTagvMeta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.<LabelId, LabelId>create(null, tagvWeb02Id));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(sysCpuIdleId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[4], tsuids.get(0));
  }

  @Test
  public void metricAndTagPairMeta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.create(tagkHostId, tagvWeb01Id));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(sysCpuIdleId, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids[3], tsuids.get(0));
  }

  @Test
  public void tagPairOnlyMeta() throws Exception {
    ImmutableSortedSet<Pair<LabelId, LabelId>> tags =
        ImmutableSortedSet.of(Pair.create(tagkHostId, tagvWeb01Id));

    final ResolvedSearchQuery query = new ResolvedSearchQuery(null, tags);
    final List<byte[]> tsuids = store.executeTimeSeriesQuery(query).get();

    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids[0], tsuids.get(0));
    assertArrayEquals(test_tsuids[2], tsuids.get(1));
    assertArrayEquals(test_tsuids[3], tsuids.get(2));
  }
}
