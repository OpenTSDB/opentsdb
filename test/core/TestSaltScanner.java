// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Scanner.class, SaltScanner.class, Span.class, 
  Const.class, UniqueId.class })
public class TestSaltScanner extends BaseTsdbTest {
  private final static byte[] KEY_A = { 0x00, 0x00, 0x00, 0x01, 
    0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01 };
  // different tagv
  private final static byte[] KEY_B = { 0x00, 0x00, 0x00, 0x01, 
    0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02 };
  // same as A bug different time
  private final static byte[] KEY_C = { 0x00, 0x00, 0x00, 0x01, 
    0x51, (byte) 0x0B, 0x13, (byte) 0x90, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01 };
  private final static byte[] FAMILY = "t".getBytes();
  private final static byte[] QUALIFIER_A = { 0x00, 0x00 };
  private final static byte[] QUALIFIER_B = { 0x00, 0x10 };
  private final static byte[] VALUE = { 0x42 };
  private final static long VALUE_LONG = 66;
  
  private final static int NUM_BUCKETS = 2;
  private List<Scanner> scanners;
  private TreeMap<byte[], Span> spans;
  private List<TagVFilter> filters;
  
  private List<ArrayList<ArrayList<KeyValue>>> kvs_a;
  private List<ArrayList<ArrayList<KeyValue>>> kvs_b;
  
  private Scanner scanner_a;
  private Scanner scanner_b;
  
  @Before
  public void beforeLocal() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(NUM_BUCKETS);
    
    filters = new ArrayList<TagVFilter>();
    
    spans = new TreeMap<byte[], Span>(new RowKey.SaltCmp());
    setupMockScanners(true);
  }
  
  @Test
  public void ctor() {
    assertNotNull(new SaltScanner(tsdb, METRIC_BYTES, scanners, spans, filters));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorSaltDisabled() {
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(0);
    new SaltScanner(tsdb, METRIC_BYTES, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTSDB() {
    new SaltScanner(null, METRIC_BYTES, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullMETRIC_BYTES() {
    new SaltScanner(tsdb, null, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorShortMETRIC_BYTES() {
    new SaltScanner(tsdb, new byte[] { 0, 1 }, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullScanners() {
    new SaltScanner(tsdb, METRIC_BYTES, null, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNotEnoughScanners() {
    scanners.remove(1);
    new SaltScanner(tsdb, METRIC_BYTES, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorTooManyScanners() {
    scanners.add(mock(Scanner.class));
    new SaltScanner(tsdb, METRIC_BYTES, scanners, spans, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullSpans() {
    new SaltScanner(tsdb, METRIC_BYTES, scanners, null, filters);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorSpansHaveData() {
    spans.put(new byte[] { 0, 0, 0, 1 }, new Span(tsdb));
    new SaltScanner(tsdb, METRIC_BYTES, scanners, spans, filters);
  }
  
  @Test
  public void scanNoData() throws Exception {
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertTrue(spans.isEmpty());
  }
  
  @Test
  public void scan() throws Exception {
    setupMockScanners(false);
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertEquals(3, spans.size());

    Span span = spans.get(KEY_A);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1356998401000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(KEY_B);
    assertEquals(1, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());
    
    span = spans.get(KEY_C);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1359680401000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
    
    verify(tag_values, never()).getNameAsync(TAGV_BYTES);
    verify(tag_values, never()).getNameAsync(TAGV_B_BYTES);
  }
  
  @Test
  public void scanWithFilter() throws Exception {
    setupMockScanners(false);
    filters.add(TagVFilter.Builder().setType("regexp").setFilter("web.*")
        .setTagk(TAGK_STRING).build());
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertEquals(3, spans.size());

    Span span = spans.get(KEY_A);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1356998401000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(KEY_B);
    assertEquals(1, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());
    
    span = spans.get(KEY_C);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1359680401000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
    
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
  }
  
  @Test
  public void scanWithTwoFilter() throws Exception {
    setupMockScanners(false);
    filters.add(TagVFilter.Builder().setType("regexp").setFilter("web.*")
        .setTagk(TAGK_STRING).build());
    filters.add(TagVFilter.Builder().setType("wildcard").setFilter("web*")
        .setTagk(TAGK_STRING).build());
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertEquals(3, spans.size());

    Span span = spans.get(KEY_A);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1356998401000L, span.timestamp(1));
    assertEquals(1, span.getAnnotations().size());

    span = spans.get(KEY_B);
    assertEquals(1, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(0, span.getAnnotations().size());
    
    span = spans.get(KEY_C);
    assertEquals(2, span.size());
    assertEquals(VALUE_LONG, span.longValue(0));
    assertEquals(1359680400000L, span.timestamp(0));
    assertEquals(VALUE_LONG, span.longValue(1));
    assertEquals(1359680401000L, span.timestamp(1));
    assertEquals(0, span.getAnnotations().size());
    
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
  }
  
  @Test
  public void scanWithFilterNoMatch() throws Exception {
    setupMockScanners(false);
    filters.add(TagVFilter.Builder().setType("regexp").setFilter("db.*")
        .setTagk(TAGK_STRING).build());
    
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertEquals(0, spans.size());

    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
  }
  
  @Test
  public void scanWithTwoFiltersNoMatch() throws Exception {
    setupMockScanners(false);
    filters.add(TagVFilter.Builder().setType("regexp").setFilter("web.*")
        .setTagk(TAGK_STRING).build());
    filters.add(TagVFilter.Builder().setType("wildcard").setFilter("db*")
        .setTagk(TAGK_STRING).build());
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    assertTrue(spans == scanner.scan().joinUninterruptibly());
    assertEquals(0, spans.size());

    verify(tag_values, atLeast(1)).getNameAsync(TAGV_BYTES);
    verify(tag_values, atLeast(1)).getNameAsync(TAGV_B_BYTES);
  }
  
  @Test
  public void scanHBaseScannerFromDeferredA() throws Exception {
    setupMockScanners(false);
    // we can't instantiate an HBaseException so just throw a RuntimeException
    final RuntimeException e = new RuntimeException("From HBase");
    when(scanner_a.nextRows())
      .thenReturn(Deferred.fromResult(kvs_a.get(0)))
      .thenReturn(Deferred.
          <ArrayList<ArrayList<KeyValue>>>fromError(e));

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    try {
      scanner.scan().joinUninterruptibly();
      fail("Expected a runtime exception here");
    } catch (RuntimeException re) {
      assertEquals(e, re);
    }
  }
  
  @Test
  public void scanHBaseScannerFromDeferredB() throws Exception {
    setupMockScanners(false);
    // we can't instantiate an HBaseException so just throw a RuntimeException
    final RuntimeException e = new RuntimeException("From HBase");
    when(scanner_b.nextRows())
      .thenReturn(Deferred.fromResult(kvs_b.get(0)))
      .thenReturn(Deferred.fromResult(kvs_b.get(1)))
      .thenReturn(Deferred.
          <ArrayList<ArrayList<KeyValue>>>fromError(e));
  
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    try {
      scanner.scan().joinUninterruptibly();
      fail("Expected a runtime exception here");
    } catch (RuntimeException re) {
      assertEquals(e, re);
    }
  }
  
  @Test
  public void scanHBaseScannerThrownA() throws Exception {
    setupMockScanners(false);
    // we can't instantiate an HBaseException so just throw a RuntimeException
    final RuntimeException e = new RuntimeException("From HBase");
    when(scanner_a.nextRows())
      .thenReturn(Deferred.fromResult(kvs_a.get(0)))
      .thenThrow(e);

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    try {
      scanner.scan().joinUninterruptibly();
      fail("Expected a runtime exception here");
    } catch (RuntimeException re) {
      assertEquals(e, re);
    }
  }
  
  @Test
  public void scanHBaseScannerThrownB() throws Exception {
    setupMockScanners(false);
    // we can't instantiate an HBaseException so just throw a RuntimeException
    final RuntimeException e = new RuntimeException("From HBase");
    when(scanner_b.nextRows())
      .thenReturn(Deferred.fromResult(kvs_b.get(0)))
      .thenReturn(Deferred.fromResult(kvs_b.get(1)))
      .thenThrow(e);
  
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    try {
      scanner.scan().joinUninterruptibly();
      fail("Expected a runtime exception here");
    } catch (RuntimeException re) {
      assertEquals(e, re);
    }
  }
  
  @Test (expected = IllegalDataException.class)
  public void scanBadRowKey() throws Exception {
    setupMockScanners(false);

    final ArrayList<ArrayList<KeyValue>> rows = 
        new ArrayList<ArrayList<KeyValue>>(1);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    rows.add(row);
    final byte[] key = { 0x00, 0x00, 0x00, 0x02, 
        0x51, (byte) 0x0B, 0x13, (byte) 0x90, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01 };
    row.add(new KeyValue(key, FAMILY, QUALIFIER_A, 0, VALUE));
    kvs_a.set(2, rows);
    
    when(scanner_a.nextRows())
      .thenReturn(Deferred.fromResult(kvs_a.get(0)))
      .thenReturn(Deferred.fromResult(kvs_a.get(1)))
      .thenReturn(Deferred.fromResult(kvs_a.get(2)))
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    scanner.scan().joinUninterruptibly();
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = IllegalDataException.class)
  public void scanCompactionDataException() throws Exception {
    setupMockScanners(false);
    
    doThrow(new IllegalDataException("Boo!")).when(
        tsdb).compact(any(ArrayList.class), any(List.class), any(List.class));
    
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    scanner.scan().joinUninterruptibly();
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = RuntimeException.class)
  public void scanCompactionRuntimeException() throws Exception {
    setupMockScanners(false);
    
    doThrow(new RuntimeException("Boo!")).when(
        tsdb).compact(any(ArrayList.class), any(List.class), any(List.class));
    
    final SaltScanner scanner = new SaltScanner(tsdb, METRIC_BYTES, scanners, 
        spans, filters);
    scanner.scan().joinUninterruptibly();
  }
  
  /**
   * Sets up a pair of scanners with either a list of values or no data
   * @param no_data Whether or not to return 0 data.
   */
  private void setupMockScanners(final boolean no_data) {
    scanners = new ArrayList<Scanner>(NUM_BUCKETS);
    scanner_a = mock(Scanner.class);
    scanner_b = mock(Scanner.class);
    if (no_data) {
      when(scanner_a.nextRows()).thenReturn(
          Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
      when(scanner_b.nextRows()).thenReturn(
          Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    } else {
      setupValues();
    }
    scanners.add(scanner_a);
    scanners.add(scanner_b);
  }
  
  /**
   * This method sets up some row keys and values to pass to the scanners.
   * The values aren't exactly what would normally be passed to a salt scanner
   * in that we have the same series salted across separate buckets. That would
   * only happen if you add the timestamp to the salt calculation, which we
   * may do in the future. We're testing now for future proofing.
   */
  private void setupValues() {
    kvs_a = new ArrayList<ArrayList<ArrayList<KeyValue>>>(3);
    kvs_b = new ArrayList<ArrayList<ArrayList<KeyValue>>>(2);
    
    final String note = "{\"tsuid\":\"000001000001000001\","
        + "\"startTime\":1356998490,\"endTime\":0,\"description\":"
        + "\"The Great A'Tuin!\",\"notes\":\"Millenium hand and shrimp\","
        + "\"custom\":null}";
    
    for (int i = 0; i < 5; i++) {
      final ArrayList<ArrayList<KeyValue>> rows = 
          new ArrayList<ArrayList<KeyValue>>(1);
      final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
      rows.add(row);
      byte[] key = null;
      
      switch (i) {
      case 0:
        row.add(new KeyValue(KEY_A, FAMILY, QUALIFIER_A, 0, VALUE));
        kvs_a.add(rows);
        break;
      case 1:
        row.add(new KeyValue(KEY_B, FAMILY, QUALIFIER_A, 0, VALUE));
        kvs_a.add(rows);
        break;
      case 2:
        row.add(new KeyValue(KEY_C, FAMILY, QUALIFIER_A, 0, VALUE));
        kvs_a.add(rows);
        break;
      case 3:
        key = Arrays.copyOf(KEY_A, KEY_A.length);
        key[0] = 1;
        row.add(new KeyValue(key, FAMILY, QUALIFIER_B, 0, VALUE));
        row.add(new KeyValue(key, FAMILY, new byte[] { 1, 0, 0 }, 0, 
            note.getBytes(Charset.forName("UTF8"))));
        kvs_b.add(rows);
        break;
      case 4:
        key = Arrays.copyOf(KEY_C, KEY_C.length);
        key[0] = 1;
        row.add(new KeyValue(key, FAMILY, QUALIFIER_B, 0, VALUE));
        kvs_b.add(rows);
        break;
      }
    }
    
    when(scanner_a.nextRows())
      .thenReturn(Deferred.fromResult(kvs_a.get(0)))
      .thenReturn(Deferred.fromResult(kvs_a.get(1)))
      .thenReturn(Deferred.fromResult(kvs_a.get(2)))
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
    
    when(scanner_b.nextRows())
      .thenReturn(Deferred.fromResult(kvs_b.get(0)))
      .thenReturn(Deferred.fromResult(kvs_b.get(1)))
      .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));
  }
}
