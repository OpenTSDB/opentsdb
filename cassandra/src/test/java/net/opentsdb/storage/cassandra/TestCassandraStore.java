package net.opentsdb.storage.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import dagger.ObjectGraph;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static net.opentsdb.storage.cassandra.CassandraLabelId.toLong;
import static net.opentsdb.storage.cassandra.CassandraTestHelpers.TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestCassandraStore {
  private static final String METRIC_NAME_ONE = "sys";
  private static final String METRIC_NAME_TWO = "cpu0";
  private static final String METRIC_NAME_THREE = "cpu1";
  private static final String TAGK_NAME_ONE = "host";
  private static LabelId TAGK_UID_ONE;
  private static final String TAGV_NAME_ONE = "127.0.0.1";
  private static LabelId TAGV_UID_ONE;

  private CassandraStore store;

  @Inject Config config;
  @Inject CassandraStoreDescriptor storeDescriptor;

  private Map<String, LabelId> name_uid = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new CassandraTestModule()).inject(this);

    store = new CassandraStoreDescriptor().createStore(config, new MetricRegistry());

    name_uid.put(METRIC_NAME_ONE, store.allocateUID(
        METRIC_NAME_ONE, UniqueIdType.METRIC).join());

    name_uid.put(METRIC_NAME_TWO, store.allocateUID(
        METRIC_NAME_TWO, UniqueIdType.METRIC).join());

    name_uid.put(METRIC_NAME_THREE, store.allocateUID(
        METRIC_NAME_THREE, UniqueIdType.METRIC).join());

    TAGK_UID_ONE = store.allocateUID(TAGK_NAME_ONE, UniqueIdType.TAGK).join();
    TAGV_UID_ONE = store.allocateUID(TAGV_NAME_ONE, UniqueIdType.TAGV).join();


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
            (short) 47).joinUninterruptibly(TIMEOUT);
  }

  @Test(expected = NullPointerException.class)
  public void addPointNull() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            null,
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).joinUninterruptibly(TIMEOUT);

  }

  @Test(expected = IllegalArgumentException.class)
  public void addPointEmpty() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            new byte[]{},
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).joinUninterruptibly(TIMEOUT);

  }

  @Test(expected = IllegalArgumentException.class)
  public void addPointTooShort() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
    store.addPoint(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0},
            new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
            (long) 1356998400,
            (short) 47).joinUninterruptibly(TIMEOUT);
  }
  */

  @Test
  public void allocateUID() throws Exception {
    LabelId new_metric_uid = store.allocateUID("new", UniqueIdType.METRIC)
            .joinUninterruptibly(TIMEOUT);
    long max_uid = 0;
    for (LabelId uid : name_uid.values()) {
      max_uid = Math.max(toLong(uid), max_uid);
    }
    assertEquals(max_uid + 1, toLong(new_metric_uid));
  }

  @Test
  public void renameUID() {
    fail();
    //store.allocateUID("renamed", new byte[]{0, 0, 4}, UniqueIdType.METRIC);
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
    Optional<LabelId> value = store.getId(name, type).join(TIMEOUT);

    assertTrue(value.isPresent());
    assertEquals(name_uid.get(name), value.get());
  }

  private void validateInvalidId(final String name, final UniqueIdType type)
          throws Exception {
    Optional<LabelId> value = store.getId(name, type).join(TIMEOUT);
    assertFalse(value.isPresent());
  }

  private void validateValidName(final String name, final UniqueIdType type)
          throws Exception {
    Optional<String> value = store.getName(name_uid.get(name), type).join(TIMEOUT);

    assertTrue(value.isPresent());
    assertEquals(name, value.get());
  }

  private void validateInvalidName(final LabelId uid, final UniqueIdType type)
          throws Exception {
    Optional<String> value = store.getName(uid, type).join(TIMEOUT);
    assertFalse(value.isPresent());
  }
}
