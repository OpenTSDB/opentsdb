package net.opentsdb.core;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.opentsdb.TestModule;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.utils.TestUtil;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;

import autovalue.shaded.com.google.common.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import javax.inject.Inject;

public class IdClientTest {
  @Inject TsdbStore store;
  @Inject IdClient idClient;

  private LabelId sysCpu0;
  private LabelId host;
  private LabelId web01;
  private LabelId sysCpu1;
  private LabelId datacenter;
  private LabelId localhost;
  private LabelId myserver;
  private LabelId pop;
  private LabelId doesnotexist;
  private LabelId web011;
  private LabelId web02;
  private LabelId nohost;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);

    sysCpu0 = store.allocateUID("sys.cpu.0", METRIC).get();
    sysCpu1 = store.allocateUID("sys.cpu.1", METRIC).get();

    host = store.allocateUID("host", TAGK).get();
    datacenter = store.allocateUID("datacenter", TAGK).get();
    pop = store.allocateUID("pop", TAGK).get();
    doesnotexist = store.allocateUID("doesnotexist", TAGK).get();

    localhost = store.allocateUID("localhost", TAGV).get();
    myserver = store.allocateUID("myserver", TAGV).get();
    web011 = store.allocateUID("web01", TAGV).get();
    web02 = store.allocateUID("web02", TAGV).get();
    nohost = store.allocateUID("nohost", TAGV).get();
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUidNameMetric() throws Exception {
    assertEquals("sys.cpu.0", idClient.getUidName(METRIC, sysCpu0).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUidNameTagk() throws Exception {
    assertEquals("host", idClient.getUidName(TAGK, host).get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUidNameTagv() throws Exception {
    assertEquals("web01", idClient.getUidName(TAGV, web01).get());
  }

  @Test(expected = NoSuchUniqueId.class, timeout = TestUtil.TIMEOUT)
  public void getUidNameMetricNSU() throws Exception {
    idClient.getUidName(METRIC, mock(LabelId.class)).get();
  }

  @Test(expected = NoSuchUniqueId.class, timeout = TestUtil.TIMEOUT)
  public void getUidNameTagkNSU() throws Exception {
    idClient.getUidName(TAGK, mock(LabelId.class)).get();
  }

  @Test(expected = NoSuchUniqueId.class, timeout = TestUtil.TIMEOUT)
  public void getUidNameTagvNSU() throws Exception {
    idClient.getUidName(TAGV, mock(LabelId.class)).get();
  }

  @Test(expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    idClient.getUidName(null, mock(LabelId.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUidNameNullUID() throws Exception {
    idClient.getUidName(TAGV, null);
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUIDMetric() throws Exception {
    assertEquals(sysCpu0, idClient.getUID(METRIC, "sys.cpu.0").get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUIDTagk() throws Exception {
    assertEquals(host, idClient.getUID(TAGK, "host").get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getUIDTagv() throws Exception {
    assertEquals(host, idClient.getUID(TAGV, "localhost").get());
  }

  @Test(expected = NoSuchUniqueName.class, timeout = TestUtil.TIMEOUT)
  public void getUIDMetricNSU() throws Exception {
    idClient.getUID(METRIC, "sys.cpu.2").get();
  }

  @Test(expected = NoSuchUniqueName.class, timeout = TestUtil.TIMEOUT)
  public void getUIDTagkNSU() throws Exception {
    idClient.getUID(TAGK, "region").get();
  }

  @Test(expected = NoSuchUniqueName.class, timeout = TestUtil.TIMEOUT)
  public void getUIDTagvNSU() throws Exception {
    idClient.getUID(TAGV, "yourserver").get();
  }

  @Test(expected = NullPointerException.class)
  public void getUIDNullType() {
    idClient.getUID(null, "sys.cpu.1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    idClient.getUID(TAGV, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    idClient.getUID(TAGV, "");
  }

  @Test
  public void assignUidMetric() {
    final LabelId id = mock(LabelId.class);
    when(store.allocateUID("sys.cpu.2", METRIC))
        .thenReturn(Futures.immediateFuture(id));
    assertSame(id, idClient.assignUid(METRIC, "sys.cpu.2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    idClient.assignUid(METRIC, "sys.cpu.0");
  }

  @Test
  public void assignUidTagk() {
    final LabelId id = mock(LabelId.class);
    when(store.allocateUID("region", TAGK))
        .thenReturn(Futures.immediateFuture(id));
    assertSame(id, idClient.assignUid(TAGK, "region"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    idClient.assignUid(TAGK, "host");
  }

  @Test
  public void assignUidTagv() {
    final LabelId id = mock(LabelId.class);
    when(store.allocateUID("yourserver", TAGV))
        .thenReturn(Futures.immediateFuture(id));
    assertSame(id, idClient.assignUid(TAGV, "yourserver"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    idClient.assignUid(TAGV, "localhost");
  }

  @Test(expected = NullPointerException.class)
  public void assignUidNullType() {
    idClient.assignUid(null, "localhost");
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    idClient.assignUid(METRIC, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    idClient.assignUid(METRIC, "Not!A:Valid@Name");
  }

  @Test
  public void validateGoodString() {
    IdClient.validateUidName("test", "omg-TSDB/42._foo_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateNullString() {
    IdClient.validateUidName("test", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateBadString() {
    IdClient.validateUidName("test", "this is a test!");
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getTagNames() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(host, web01);
    final Map<String, String> tags = idClient.getTagNames(ids).get();
    assertEquals("web01", tags.get("host"));
  }

  @Test(expected = NoSuchUniqueId.class, timeout = TestUtil.TIMEOUT)
  public void getTagNamesNSUI() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(mock(LabelId.class), mock(LabelId.class));
    idClient.getTagNames(ids).get();
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getTagNamesEmptyList() throws Exception {
    final Map<String, String> tags = idClient.getTagNames(ImmutableList.<LabelId>of()).get();
    assertNotNull(tags);
    assertEquals(0, tags.size());
  }

  @Test(expected = NoSuchUniqueName.class)
  public void executeTimeSeriesQueryMissingName() throws Exception {
    final SearchQuery query = new SearchQuery("nosuchname");
    idClient.executeTimeSeriesQuery(query).get();
  }
}
