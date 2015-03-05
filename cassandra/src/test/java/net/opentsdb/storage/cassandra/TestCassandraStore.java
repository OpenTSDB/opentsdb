package net.opentsdb.storage.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import dagger.ObjectGraph;
import net.opentsdb.stats.Metrics;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static net.opentsdb.storage.cassandra.CassandraTestHelpers.TIMEOUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCassandraStore {
  private static final String METRIC_NAME_ONE = "sys";
  private static final String METRIC_NAME_TWO = "cpu0";
  private static final String METRIC_NAME_THREE = "cpu1";
  private static final String TAGK_NAME_ONE = "host";
  private static byte[] TAGK_UID_ONE;
  private static final String TAGV_NAME_ONE = "127.0.0.1";
  private static byte[] TAGV_UID_ONE;
  private static byte[] TSUID_ONE;
  private static byte[] TSUID_TWO;
  private static byte[] TSUID_THREE;

  private CassandraStore store;

  @Inject Config config;
  @Inject CassandraStoreDescriptor storeDescriptor;

  private Map<String, byte[]> name_uid = new HashMap<String, byte[]>();

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new CassandraTestModule()).inject(this);

    store = new CassandraStoreDescriptor().createStore(config, new Metrics(new MetricRegistry()));







    name_uid.put(METRIC_NAME_ONE, store.allocateUID(
        METRIC_NAME_ONE, UniqueIdType.METRIC).joinUninterruptibly());

    name_uid.put(METRIC_NAME_TWO, store.allocateUID(
        METRIC_NAME_TWO, UniqueIdType.METRIC).joinUninterruptibly());

    name_uid.put(METRIC_NAME_THREE, store.allocateUID(
        METRIC_NAME_THREE, UniqueIdType.METRIC).joinUninterruptibly());


    TAGK_UID_ONE = store.allocateUID(TAGK_NAME_ONE, UniqueIdType.TAGK)
        .joinUninterruptibly();

    TAGV_UID_ONE = store.allocateUID(TAGV_NAME_ONE, UniqueIdType.TAGV)
        .joinUninterruptibly();

    TSUID_ONE = createTSUIDFromTreeUID(name_uid.get
        (METRIC_NAME_ONE), TAGK_UID_ONE, TAGV_UID_ONE);
    TSUID_TWO = createTSUIDFromTreeUID(name_uid.get
        (METRIC_NAME_TWO), TAGK_UID_ONE, TAGV_UID_ONE);
    TSUID_THREE = createTSUIDFromTreeUID(name_uid.get
        (METRIC_NAME_THREE), TAGK_UID_ONE, TAGV_UID_ONE);


    store.addPoint(TSUID_ONE, new byte[]{'d', '1'}, 1356998400, (short) 'a');
    store.addPoint(TSUID_ONE, new byte[]{'d', '2'}, 1356998401, (short) 'b');
    store.addPoint(TSUID_ONE, new byte[]{'d', '2'}, 1357002078, (short) 'b');

    store.addPoint(TSUID_TWO, new byte[]{'d', '3'}, 1356998400, (short) 'b');
  }

  @After
  public void tearDown() throws Exception {
    CassandraTestHelpers.truncate(store.getSession());
  }

  private byte[] createTSUIDFromTreeUID(final byte[] metric, final byte[]
          tagk, final byte[] tagv) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    outputStream.write(metric);
    outputStream.write(tagk);
    outputStream.write(tagv);
    return outputStream.toByteArray();
  }

  @Test
  public void constructor() throws IOException {
    assertNotNull(new CassandraStore(storeDescriptor.createCluster(config)));
  }

  @Test(expected = NullPointerException.class)
  public void constructorNull() throws IOException {
    new CassandraStore(null);
  }

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

  @Test
  public void allocateUID() throws Exception {
    byte[] new_metric_uid = store.allocateUID("new", UniqueIdType.METRIC)
            .joinUninterruptibly(TIMEOUT);
    long max_uid = 0;
    for (byte[] uid : name_uid.values()) {
      max_uid = Math.max(IdUtils.uidToLong(uid), max_uid);
    }
    assertEquals(max_uid + 1, IdUtils.uidToLong(new_metric_uid));
  }

  @Test
  public void renameUID() {
    fail();
    store.allocateUID("renamed", new byte[]{0, 0, 4}, UniqueIdType.METRIC);
  }

  @Test
  public void getName() throws Exception {
    validateValidName(METRIC_NAME_ONE, UniqueIdType.METRIC);
    validateValidName(METRIC_NAME_TWO, UniqueIdType.METRIC);
    validateValidName(METRIC_NAME_THREE, UniqueIdType.METRIC);
    validateInvalidName(new byte[]{0, 0, 10}, UniqueIdType.METRIC);
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
    Optional<byte[]> value = store.getId(name, type)
            .joinUninterruptibly(TIMEOUT);

    assertTrue(value.isPresent());
    assertArrayEquals(name_uid.get(name), value.get());
  }

  private void validateInvalidId(final String name, final UniqueIdType type)
          throws Exception {
    Optional<byte[]> value = store.getId(name, type)
            .joinUninterruptibly(TIMEOUT);

    assertFalse(value.isPresent());
  }

  private void validateValidName(final String name, final UniqueIdType type)
          throws Exception {
    Optional<String> value = store.getName(name_uid.get(name), type)
            .joinUninterruptibly(TIMEOUT);

    assertTrue(value.isPresent());
    assertEquals(name, value.get());
  }

  private void validateInvalidName(final byte[] uid, final UniqueIdType type)
          throws Exception {
    Optional<String> value = store.getName(uid, type)
            .joinUninterruptibly(TIMEOUT);
    assertFalse(value.isPresent());
  }
}
