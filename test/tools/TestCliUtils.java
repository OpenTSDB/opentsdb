package net.opentsdb.tools;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, HBaseClient.class, Scanner.class, Const.class })
public class TestCliUtils {

  private TSDB tsdb = null;
  private HBaseClient client = null;
  private List<byte[]> start_keys;
  private List<byte[]> stop_keys;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    client = mock(HBaseClient.class);
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    
  }
  
  @Test
  public void getDataTableScanners1Thread() throws Exception {
    setupGetDataTableScanners(256);
    
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 1);
    assertEquals(1, scanners.size());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(0));
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(0));
  }
  
  @Test
  public void getDataTableScannersMultiThreaded() throws Exception {
    setupGetDataTableScanners(256);
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 15);
    assertEquals(15, scanners.size());
    byte[] key = new byte[3];
    int last_value = 0;
    for (int i = 0; i < 15; i++) {
      if (i == 0) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(i));
      } else {
        assertArrayEquals(key, start_keys.get(i));
      }
      last_value += 18;
      if (i == 14) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(i));
      } else {
        System.arraycopy(Bytes.fromInt(last_value), 1, key, 0, 3);
        assertArrayEquals(key, stop_keys.get(i));
      }
    }
  }
  
  @Test
  public void getDataTableScannersRandom1Thread() throws Exception {
    setupGetDataTableScanners(0);
    
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 1);
    assertEquals(1, scanners.size());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(0));
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(0));
  }
  
  @Test
  public void getDataTableScannersRandomMultiThreaded() throws Exception {
    setupGetDataTableScanners(0);
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 15);
    assertEquals(15, scanners.size());
    byte[] key = new byte[3];
    int last_value = 0;
    for (int i = 0; i < 15; i++) {
      if (i == 0) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(i));
      } else {
        assertArrayEquals(key, start_keys.get(i));
      }
      last_value += 1118481;
      if (i == 14) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(i));
      } else {
        System.arraycopy(Bytes.fromInt(last_value), 1, key, 0, 3);
        assertArrayEquals(key, stop_keys.get(i));
      }
    }
  }
  
  @Test
  public void getDataTableScannersSalted() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    setupGetDataTableScanners(256);
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 15);
    assertEquals(20, scanners.size());
    byte[] key = new byte[1];
    for (int i = 0; i < 20; i++) {
      if (i == 0) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(i));
      } else {
        assertArrayEquals(key, start_keys.get(i));
      }
      if (i == 19) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(i));
      } else {
        key = RowKey.getSaltBytes(i + 1);
        assertArrayEquals(key, stop_keys.get(i));
      }
    }
  }

  @Test
  public void getDataTableScannersSaltedRandom() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    setupGetDataTableScanners(0);
    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, 15);
    assertEquals(20, scanners.size());
    byte[] key = new byte[1];
    for (int i = 0; i < 20; i++) {
      if (i == 0) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, start_keys.get(i));
      } else {
        assertArrayEquals(key, start_keys.get(i));
      }
      if (i == 19) {
        assertArrayEquals(HBaseClient.EMPTY_ARRAY, stop_keys.get(i));
      } else {
        key = RowKey.getSaltBytes(i + 1);
        assertArrayEquals(key, stop_keys.get(i));
      }
    }
  }
  
  private void setupGetDataTableScanners(final long max) {
    final KeyValue kv = new KeyValue(new byte[] {}, 
        TSDB.FAMILY(), "metrics".getBytes(), Bytes.fromLong(max));
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(kv);
    when(client.get(any(GetRequest.class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(kvs));
    
    start_keys = new ArrayList<byte[]>();
    stop_keys = new ArrayList<byte[]>();
    
    final Scanner scanner = mock(Scanner.class);
    when(client.newScanner(any(byte[].class))).thenReturn(scanner);
    
    PowerMockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        start_keys.add((byte[])invocation.getArguments()[0]);
        return null;
      }
    }).when(scanner).setStartKey(any(byte[].class));
    
    PowerMockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        stop_keys.add((byte[])invocation.getArguments()[0]);
        return null;
      }
    }).when(scanner).setStopKey(any(byte[].class));
  }
}
