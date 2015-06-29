package net.opentsdb.storage.cassandra;

import static net.opentsdb.storage.cassandra.CassandraLabelId.toLong;
import static net.opentsdb.storage.cassandra.CassandraTestHelpers.TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.typesafe.config.Config;
import dagger.ObjectGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;

public class TestCassandraStore {
  private static final String METRIC_NAME_ONE = "sys";
  private static final String METRIC_NAME_TWO = "cpu0";
  private static final String METRIC_NAME_THREE = "cpu1";
  private static final String TAGK_NAME_ONE = "host";
  private static final String TAGV_NAME_ONE = "127.0.0.1";
  private static LabelId TAGK_UID_ONE;
  private static LabelId TAGV_UID_ONE;
  @Inject Config config;
  @Inject CassandraStoreDescriptor storeDescriptor;
  private CassandraStore store;
  private Map<String, LabelId> nameUid = new HashMap<>();

  @Rule
  public final Timeout timeout = Timeout.millis(TIMEOUT);

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new CassandraTestModule()).inject(this);

    store = new CassandraStoreDescriptor().createStore(config, new MetricRegistry());

    nameUid.put(METRIC_NAME_ONE, store.allocateLabel(
        METRIC_NAME_ONE, UniqueIdType.METRIC).get());

    nameUid.put(METRIC_NAME_TWO, store.allocateLabel(
        METRIC_NAME_TWO, UniqueIdType.METRIC).get());

    nameUid.put(METRIC_NAME_THREE, store.allocateLabel(
        METRIC_NAME_THREE, UniqueIdType.METRIC).get());

    TAGK_UID_ONE = store.allocateLabel(TAGK_NAME_ONE, UniqueIdType.TAGK).get();
    TAGV_UID_ONE = store.allocateLabel(TAGV_NAME_ONE, UniqueIdType.TAGV).get();


    /*
    store.addPoint(TSUID_ONE, new byte[]{'d', '1'}, 1356998400, (short) 'a');
    store.addPoint(TSUID_ONE, new byte[]{'d', '2'}, 1356998401, (short) 'b');
    store.addPoint(TSUID_ONE, new byte[]{'d', '2'}, 1357002078, (short) 'b');

    store.addPoint(TSUID_TWO, new byte[]{'d', '3'}, 1356998400, (short) 'b');
    */
  }

  @After
  public void tearDown() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
  }

  @Test
  public void constructor() throws IOException {
    final Cluster cluster = storeDescriptor.createCluster(config);
    final Session session = storeDescriptor.connectTo(cluster);
    assertNotNull(new CassandraStore(cluster, session));
  }

  @Test(expected = NullPointerException.class)
  public void constructorNullSession() throws IOException {
    final Cluster cluster = storeDescriptor.createCluster(config);
    new CassandraStore(cluster, null);
  }

  @Test(expected = NullPointerException.class)
  public void constructorNullCluster() throws IOException {
    final Cluster cluster = storeDescriptor.createCluster(config);
    final Session session = storeDescriptor.connectTo(cluster);
    new CassandraStore(null, session);
  }

  /*
  @Test
  public void addPoint() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    //'001001001', 1356998400, 1356998400, 'a', 'data1'
    store.addPoint(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).get(TIMEOUT);
  }

  @Test(expected = NullPointerException.class)
  public void addPointNull() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            null,
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).get(TIMEOUT);

  }

  @Test(expected = IllegalArgumentException.class)
  public void addPointEmpty() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            new byte[]{},
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).get(TIMEOUT);

  }

  @Test(expected = IllegalArgumentException.class)
  public void addPointTooShort() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0},
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).get(TIMEOUT);
  }
  */

  @Test
  public void buildBaseTimeNegativeTime() {
    assertEquals(1434545280000L, CassandraStore.buildBaseTime(1434545416154L));
  }

  @Test
  public void allocateLabel() throws Exception {
    LabelId newMetricUid = store.allocateLabel("new", UniqueIdType.METRIC).get();
    long maxUid = 0;
    for (LabelId uid : nameUid.values()) {
      maxUid = Math.max(toLong(uid), maxUid);
    }
    assertEquals(maxUid + 1, toLong(newMetricUid));
  }

  @Test
  public void renameId() {
    fail();
    //store.allocateLabel("renamed", new byte[]{0, 0, 4}, UniqueIdType.METRIC);
  }

  @Test
  public void getName() throws Exception {
    validateValidName(METRIC_NAME_ONE, UniqueIdType.METRIC);
    validateValidName(METRIC_NAME_TWO, UniqueIdType.METRIC);
    validateValidName(METRIC_NAME_THREE, UniqueIdType.METRIC);
    validateInvalidName(mock(LabelId.class), UniqueIdType.METRIC);
    validateValidName(TAGK_NAME_ONE, UniqueIdType.TAGK);
    validateValidName(TAGV_NAME_ONE, UniqueIdType.TAGK);
  }

  @Test
  public void getId() throws Exception {
    validateValidId(METRIC_NAME_ONE, UniqueIdType.METRIC);
    validateValidId(METRIC_NAME_TWO, UniqueIdType.METRIC);
    validateValidId(METRIC_NAME_THREE, UniqueIdType.METRIC);
    validateInvalidId("Missing", UniqueIdType.METRIC);
    validateValidId(TAGK_NAME_ONE, UniqueIdType.TAGK);
    validateValidId(TAGV_NAME_ONE, UniqueIdType.TAGV);
  }

  private void validateValidId(final String name, final UniqueIdType type)
      throws Exception {
    Optional<LabelId> value = store.getId(name, type).get();

    assertTrue(value.isPresent());
    assertEquals(nameUid.get(name), value.get());
  }

  private void validateInvalidId(final String name, final UniqueIdType type)
      throws Exception {
    Optional<LabelId> value = store.getId(name, type).get();
    assertFalse(value.isPresent());
  }

  private void validateValidName(final String name, final UniqueIdType type)
      throws Exception {
    Optional<String> value = store.getName(nameUid.get(name), type).get();

    assertTrue(value.isPresent());
    assertEquals(name, value.get());
  }

  private void validateInvalidName(final LabelId uid, final UniqueIdType type)
      throws Exception {
    Optional<String> value = store.getName(uid, type).get();
    assertFalse(value.isPresent());
  }
}
