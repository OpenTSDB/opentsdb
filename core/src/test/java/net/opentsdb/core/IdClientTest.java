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
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.TestUtil;

import autovalue.shaded.com.google.common.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import javax.inject.Inject;

public class IdClientTest {
  @Rule
  public final Timeout timeout = Timeout.millis(TestUtil.TIMEOUT);

  @Inject TsdbStore store;
  @Inject IdClient idClient;

  private LabelId sysCpu0;
  private LabelId host;
  private LabelId web01;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);

    sysCpu0 = store.allocateLabel("sys.cpu.0", METRIC).get();
    host = store.allocateLabel("host", TAGK).get();
    web01 = store.allocateLabel("web01", TAGV).get();
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    idClient.assignUid(METRIC, "Not!A:Valid@Name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    idClient.assignUid(METRIC, null);
  }

  @Test(expected = NullPointerException.class)
  public void assignUidNullType() {
    idClient.assignUid(null, "localhost");
  }

  @Test
  public void assignUidTagKey() {
    final LabelId id = mock(LabelId.class);
    when(store.allocateLabel("region", TAGK))
        .thenReturn(Futures.immediateFuture(id));
    assertSame(id, idClient.assignUid(TAGK, "region"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidTagKeyExists() {
    idClient.assignUid(TAGK, "host");
  }

  @Test(expected = NoSuchUniqueName.class)
  public void executeTimeSeriesQueryMissingName() throws Exception {
    final SearchQuery query = new SearchQuery("nosuchname");
    idClient.executeTimeSeriesQuery(query).get();
  }

  @Test
  public void getTagNames() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(host, web01);
    final Map<String, String> tags = idClient.getTagNames(ids).get();
    assertEquals("web01", tags.get("host"));
  }

  @Test
  public void getTagNamesEmptyList() throws Exception {
    final Map<String, String> tags = idClient.getTagNames(ImmutableList.<LabelId>of()).get();
    assertNotNull(tags);
    assertEquals(0L, tags.size());
  }

  @Test(expected = NoSuchUniqueId.class)
  public void getTagNamesNSUI() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(mock(LabelId.class), mock(LabelId.class));
    idClient.getTagNames(ids).get();
  }

  @Test
  public void getUID() throws Exception {
    assertEquals(sysCpu0, idClient.getLabelId(METRIC, "sys.cpu.0").get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    idClient.getLabelId(TAGV, "");
  }

  @Test(expected = NoSuchUniqueName.class)
  public void getUIDNSU() throws Exception {
    idClient.getLabelId(METRIC, "sys.cpu.2").get();
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    idClient.getLabelId(TAGV, null);
  }

  @Test(expected = NullPointerException.class)
  public void getUIDNullType() {
    idClient.getLabelId(null, "sys.cpu.1");
  }

  @Test
  public void getUidName() throws Exception {
    assertEquals("web01", idClient.getLabelName(TAGV, web01).get());
  }

  @Test(expected = NoSuchUniqueId.class)
  public void getUidNameNSU() throws Exception {
    idClient.getLabelName(TAGV, mock(LabelId.class)).get();
  }

  @Test(expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    idClient.getLabelName(null, mock(LabelId.class));
  }

  @Test(expected = NullPointerException.class)
  public void getUidNameNullUID() throws Exception {
    idClient.getLabelName(TAGV, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateBadString() {
    IdClient.validateLabelName("test", "this is a test!");
  }

  @Test
  public void validateGoodString() {
    IdClient.validateLabelName("test", "omg-TSDB/42._foo_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateNullString() {
    IdClient.validateLabelName("test", null);
  }
}
