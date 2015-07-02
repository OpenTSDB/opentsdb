package net.opentsdb.core;

import static net.opentsdb.uid.LabelType.METRIC;
import static net.opentsdb.uid.LabelType.TAGK;
import static net.opentsdb.uid.LabelType.TAGV;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.opentsdb.DaggerTestComponent;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import javax.inject.Inject;

public class LabelClientTest {
  @Rule
  public final Timeout timeout = Timeout.millis(TestUtil.TIMEOUT);

  @Inject TsdbStore store;
  @Inject LabelClient labelClient;

  private LabelId sysCpu0;
  private LabelId host;
  private LabelId web01;

  @Before
  public void setUp() throws Exception {
    DaggerTestComponent.create().inject(this);

    sysCpu0 = store.allocateLabel("sys.cpu.0", METRIC).get();
    host = store.allocateLabel("host", TAGK).get();
    web01 = store.allocateLabel("web01", TAGV).get();
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    labelClient.assignUid(METRIC, "Not!A:Valid@Name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    labelClient.assignUid(METRIC, null);
  }

  @Test(expected = NullPointerException.class)
  public void assignUidNullType() {
    labelClient.assignUid(null, "localhost");
  }

  @Test
  public void assignUidTagKey() {
    final LabelId id = mock(LabelId.class);
    when(store.allocateLabel("region", TAGK))
        .thenReturn(Futures.immediateFuture(id));
    assertSame(id, labelClient.assignUid(TAGK, "region"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignUidTagKeyExists() {
    labelClient.assignUid(TAGK, "host");
  }

  @Test(expected = NoSuchUniqueName.class)
  public void executeTimeSeriesQueryMissingName() throws Exception {
    final SearchQuery query = new SearchQuery("nosuchname");
    labelClient.executeTimeSeriesQuery(query).get();
  }

  @Test
  public void getTagNames() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(host, web01);
    final Map<String, String> tags = labelClient.getTagNames(ids).get();
    assertEquals("web01", tags.get("host"));
  }

  @Test
  public void getTagNamesEmptyList() throws Exception {
    final Map<String, String> tags = labelClient.getTagNames(ImmutableList.<LabelId>of()).get();
    assertNotNull(tags);
    assertEquals(0L, tags.size());
  }

  @Test(expected = NoSuchUniqueId.class)
  public void getTagNamesNoSuchId() throws Exception {
    ImmutableList<LabelId> ids = ImmutableList.of(mock(LabelId.class), mock(LabelId.class));
    labelClient.getTagNames(ids).get();
  }

  @Test
  public void getLabelId() throws Exception {
    assertEquals(sysCpu0, labelClient.getLabelId(METRIC, "sys.cpu.0").get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getLabelIdEmptyName() {
    labelClient.getLabelId(TAGV, "");
  }

  @Test(expected = NoSuchUniqueName.class)
  public void getLabelIdNoSuchName() throws Exception {
    labelClient.getLabelId(METRIC, "sys.cpu.2").get();
  }

  @Test(expected = IllegalArgumentException.class)
  public void getLabelIdNullName() {
    labelClient.getLabelId(TAGV, null);
  }

  @Test(expected = NullPointerException.class)
  public void getLabelIdNullType() {
    labelClient.getLabelId(null, "sys.cpu.1");
  }

  @Test
  public void getUidName() throws Exception {
    assertEquals("web01", labelClient.getLabelName(TAGV, web01).get());
  }

  @Test(expected = NoSuchUniqueId.class)
  public void getLabelNameNoSuchId() throws Exception {
    labelClient.getLabelName(TAGV, mock(LabelId.class)).get();
  }

  @Test(expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    labelClient.getLabelName(null, mock(LabelId.class));
  }

  @Test(expected = NullPointerException.class)
  public void getLabelNameNullId() throws Exception {
    labelClient.getLabelName(TAGV, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateBadString() {
    LabelClient.validateLabelName("test", "this is a test!");
  }

  @Test
  public void validateGoodString() {
    LabelClient.validateLabelName("test", "omg-TSDB/42._foo_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateNullString() {
    LabelClient.validateLabelName("test", null);
  }
}
