package net.opentsdb.core;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;
import org.hbase.async.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.junit.Assert.*;

public class DataPointsClientTest {
  private MemoryStore store;
  private DataPointsClient dataPointsClient;

  @Mock private SearchPlugin searchPlugin;
  @Mock private RTPublisher realtimePublisher;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);

    final Config config = new Config(false);
    config.setFixDuplicates(true); // TODO(jat): test both ways

    final ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore(config));
    store = objectGraph.get(MemoryStore.class);
    dataPointsClient = objectGraph.get(DataPointsClient.class);
  }

  @Test
  public void addPointLong1Byte() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointLong1ByteNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, -42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(-42, value[0]);
  }

  @Test
  public void addPointLong2Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 257, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(257, Bytes.getShort(value));
  }

  @Test
  public void addPointLong2BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, -257, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(-257, Bytes.getShort(value));
  }

  @Test
  public void addPointLong4Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 65537, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(65537, Bytes.getInt(value));
  }

  @Test
  public void addPointLong4BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, -65537, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(-65537, Bytes.getInt(value));
  }

  @Test
  public void addPointLong8Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 4294967296L, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(4294967296L, Bytes.getLong(value));
  }

  @Test
  public void addPointLong8BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, -4294967296L, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(-4294967296L, Bytes.getLong(value));
  }

  @Test
  public void addPointLongMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400500L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row,
            new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointLongMany() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 50; i++) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp++, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, store.numColumnsDataTable(row));
  }

  @Test
  public void addPointLongManyMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400500L;
    for (int i = 1; i <= 50; i++) {
      dataPointsClient.addPoint("sys.cpu.user", timestamp++, i, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row,
            new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, store.numColumnsDataTable(row));
  }

  @Test
  public void addPointLongEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1357001999, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xE0,
            (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointLongOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 24, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
  }

  @SuppressWarnings("unchecked")
  @Test (expected = NoSuchUniqueName.class)
  public void addPointNoAutoMetric() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user.0", 1356998400, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void addPointSecondZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 0, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointSecondOne() throws Exception {
    // hey, it's valid *shrug* Thu, 01 Jan 1970 00:00:01 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 16 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointSecond2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 4294967295L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9,
            0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0x69, (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test (expected = IllegalArgumentException.class)
  public void addPointSecondNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", -2147483648, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void addPointMS1970() throws Exception {
    // Since it's just over Integer.MAX_VALUE, OpenTSDB will treat this as
    // a millisecond timestamp since it doesn't fit in 4 bytes.
    // Base time is 4294800 which is Thu, 19 Feb 1970 17:00:00 GMT
    // offset = F0A36000 or 167296 ms
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 4294967296L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0, (byte) 0x41, (byte) 0x88,
            (byte) 0x90, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xF0,
            (byte) 0xA3, 0x60, 0});
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointMS2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15.000 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 4294967295000L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9,
            0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xF6,
            (byte) 0x77, 0x46, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointMS2286() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", Const.MAX_MS_TIMESTAMP, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0x54, (byte) 0x0B, (byte) 0xD9,
            0x10, 0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xFA,
            (byte) 0xAE, 0x5F, (byte) 0xC0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test  (expected = IllegalArgumentException.class)
  public void addPointMSTooLarge() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", Const.MAX_MS_TIMESTAMP+1, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void addPointMSNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", -2147483648000L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void addPointFloat() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointFloatNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, -42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(-42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointFloatMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400500L, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row,
            new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointFloatEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1357001999, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xE0,
            (byte) 0xFB });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointFloatPrecision() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42.5123459999F, tags)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.512345F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointFloatOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 25.4F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    final byte[] value = store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(25.4F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointBothSameTimeIntAndFloat() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs?
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    dataPointsClient.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertEquals(2, store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointBothSameTimeIntAndFloatMs() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs?
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400500L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    dataPointsClient.addPoint("sys.cpu.user", 1356998400500L, 42.5F, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    byte[] value = store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertEquals(2, store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }

  @Test
  public void addPointBothSameTimeSecondAndMs() throws Exception {
    // this can happen if a second and an ms data point are stored for the same
    // timestamp.
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    dataPointsClient.addPoint("sys.cpu.user", 1356998400L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    dataPointsClient.addPoint("sys.cpu.user", 1356998400000L, 42, tags).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0,
            0, 0, 1, 0, 0, 1};
    byte[] value = store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertEquals(2, store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0, 0 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42, value[0]);
  }

  /**
   * Configures storage for the addPoint() tests to validate that we're storing
   * data points correctly.
   */
  private void setupAddPointStorage() throws Exception {
    store.allocateUID("sys.cpu.user", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }
}