package net.opentsdb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;

import org.junit.Before;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.junit.Assert.*;

public class UniqueIdClientTest {
  private Config config;
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private UniqueIdClient uniqueIdClient;

  @Before
  public void before() throws Exception {
    config = new Config(false);
    config.setFixDuplicates(true); // TODO(jat): test both ways
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(tsdb_store)
            .build();

    uniqueIdClient = new UniqueIdClient(tsdb_store, config, tsdb);
  }

  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setGetUidName() {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }

  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setupAssignUid() {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("sys.cpu.1", new byte[]{0, 0, 2}, METRIC);

    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("datacenter", new byte[]{0, 0, 2}, TAGK);

    tsdb_store.allocateUID("localhost", new byte[]{0, 0, 1}, TAGV);
    tsdb_store.allocateUID("myserver", new byte[]{0, 0, 2}, TAGV);
  }

  @Test
  public void getUidNameMetric() throws Exception {
    setGetUidName();
    assertEquals("sys.cpu.0", uniqueIdClient.getUidName(METRIC,
            new byte[]{0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void getUidNameTagk() throws Exception {
    setGetUidName();
    assertEquals("host", uniqueIdClient.getUidName(TAGK,
            new byte[]{0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void getUidNameTagv() throws Exception {
    setGetUidName();
    assertEquals("web01", uniqueIdClient.getUidName(TAGV,
            new byte[]{0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameMetricNSU() throws Exception {
    setGetUidName();
    uniqueIdClient.getUidName(METRIC, new byte[] { 0, 0, 2 })
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagkNSU() throws Exception {
    setGetUidName();
    uniqueIdClient.getUidName(TAGK, new byte[] { 0, 0, 2 })
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagvNSU() throws Exception {
    setGetUidName();
    uniqueIdClient.getUidName(TAGV, new byte[] { 0, 0, 2 })
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    setGetUidName();
    uniqueIdClient.getUidName(null, new byte[] { 0, 0, 2 }).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void getUidNameNullUID() throws Exception {
    setGetUidName();
    uniqueIdClient.getUidName(TAGV, null).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getUIDMetric() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 },
            uniqueIdClient.getUID(METRIC, "sys.cpu.0").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void getUIDTagk() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 },
            uniqueIdClient.getUID(TAGK, "host").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void getUIDTagv() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 },
            uniqueIdClient.getUID(TAGV, "localhost").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test (expected = NoSuchUniqueName.class)
  public void getUIDMetricNSU() throws Exception {
    setupAssignUid();
    uniqueIdClient.getUID(METRIC, "sys.cpu.2").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagkNSU() throws Exception {
    setupAssignUid();
    uniqueIdClient.getUID(TAGK, "region").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagvNSU() throws Exception {
    setupAssignUid();
    uniqueIdClient.getUID(TAGV, "yourserver").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NullPointerException.class)
  public void getUIDNullType() {
    setupAssignUid();
    uniqueIdClient.getUID(null, "sys.cpu.1");
  }

  @Test (expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    setupAssignUid();
    uniqueIdClient.getUID(TAGV, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    setupAssignUid();
    uniqueIdClient.getUID(TAGV, "");
  }

  @Test
  public void assignUidMetric() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 3 },
            uniqueIdClient.assignUid(METRIC, "sys.cpu.2"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    setupAssignUid();
    uniqueIdClient.assignUid(METRIC, "sys.cpu.0");
  }

  @Test
  public void assignUidTagk() {
    setupAssignUid();
    assertArrayEquals(new byte[] {0, 0, 3},
            uniqueIdClient.assignUid(TAGK, "region"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    setupAssignUid();
    uniqueIdClient.assignUid(TAGK, "host");
  }

  @Test
  public void assignUidTagv() {
    setupAssignUid();
    assertArrayEquals(new byte[] {0, 0, 3},
            uniqueIdClient.assignUid(TAGV, "yourserver"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    setupAssignUid();
    uniqueIdClient.assignUid(TAGV, "localhost");
  }

  @Test (expected = NullPointerException.class)
  public void assignUidNullType() {
    setupAssignUid();
    uniqueIdClient.assignUid(null, "localhost");
  }

  @Test (expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    setupAssignUid();
    uniqueIdClient.assignUid(METRIC, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    setupAssignUid();
    uniqueIdClient.assignUid(METRIC, "Not!A:Valid@Name");
  }

  @Test
  public void validateGoodString() {
    UniqueIdClient.validateUidName("test", "omg-TSDB/42._foo_");
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateNullString() {
    UniqueIdClient.validateUidName("test", null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateBadString() {
    UniqueIdClient.validateUidName("test", "this is a test!");
  }

  @Test
  public void getTagNames() throws Exception {
    setupStorage();
    setupResolveIds();

    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 1 });
    final HashMap<String, String> tags = uniqueIdClient.getTagNames(ids)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("web01", tags.get("host"));
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getTagNamesNSUI() throws Exception {
    setupStorage();
    setupResolveIds();

    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 2 });
    uniqueIdClient.getTagNames(ids).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getTagNamesEmptyList() throws Exception {
    setupStorage();
    setupResolveIds();

    final List<byte[]> ids = new ArrayList<byte[]>(0);
    final HashMap<String, String> tags = uniqueIdClient.getTagNames(ids)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(tags);
    assertEquals(0, tags.size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void getTagNamesWrongLength() throws Exception {
    setupStorage();
    setupResolveIds();

    final List<byte[]> ids = new ArrayList<byte[]>(1);
    ids.add(new byte[] { 0, 0, 1, 0, 0, 0, 2 });
    uniqueIdClient.getTagNames(ids).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getOrCreateAllCreate() throws Exception {
    setupStorage();
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    final List<byte[]> uids = uniqueIdClient.getOrCreateAllTags(tags)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 1}, uids.get(0));
  }

  @Test
  public void getOrCreateTagkAllowed() throws Exception {
    setupStorage();
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("doesnotexist", "web01");
    final List<byte[]> uids = uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 3, 0, 0, 1}, uids.get(0));
  }

  @Test
  public void getOrCreateTagkNotAllowedGood() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagks", "false");
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("pop", "web01");
    final List<byte[]> uids = uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 2, 0, 0, 1}, uids.get(0));
  }

  @Test (expected = NoSuchUniqueName.class)
  public void getOrCreateTagkNotAllowedBlocked() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagks", "false");
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("nonesuch", "web01");
    uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void getOrCreateTagvAllowed() throws Exception {
    setupStorage();
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "nohost");
    final List<byte[]> uids = uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 3}, uids.get(0));
  }

  @Test
  public void getOrCreateTagvNotAllowedGood() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagvs", "false");
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web02");
    final List<byte[]> uids = uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2}, uids.get(0));
  }

  @Test (expected = NoSuchUniqueName.class)
  public void getOrCreateTagvNotAllowedBlocked() throws Exception {
    setupStorage();
    config.overrideConfig("tsd.core.auto_create_tagvs", "false");
    setupResolveAll();

    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "invalidhost");
    uniqueIdClient.getOrCreateAllTags(tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  // PRIVATE helpers to setup unit tests

  private void setupStorage() throws Exception {
    config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(tsdb_store)
            .build();
  }

  private void setupResolveIds() throws Exception {
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
  }

  private void setupResolveAll() throws Exception {
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("pop", new byte[]{0, 0, 2}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("doesnotexist", new byte[]{0, 0, 3}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[]{0, 0, 2}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("nohost", new byte[]{0, 0, 3}, UniqueIdType.TAGV);
  }
}