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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import net.opentsdb.uid.NoSuchUniqueId;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestRowKey extends BaseTsdbTest {
  
  @Test
  public void metricNameAsync() throws Exception {
    final byte[] key = { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    assertEquals(METRIC_STRING, RowKey.metricNameAsync(tsdb, key)
        .joinUninterruptibly());
  }
  
  @Test
  public void metricNameAsyncSalted() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    byte[] key = { 0, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    assertEquals(METRIC_STRING, RowKey.metricNameAsync(tsdb, key)
        .joinUninterruptibly());
    
    key = new byte[] { 1, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    assertEquals(METRIC_STRING, RowKey.metricNameAsync(tsdb, key)
        .joinUninterruptibly());
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(2);
    key = new byte[] { 0, 0, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    assertEquals(METRIC_STRING, RowKey.metricNameAsync(tsdb, key)
        .joinUninterruptibly());
    
    key = new byte[] { 0, 1, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    assertEquals(METRIC_STRING, RowKey.metricNameAsync(tsdb, key)
        .joinUninterruptibly());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void metricNameAsyncRowNull() throws Exception {
    RowKey.metricNameAsync(tsdb, null).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void metricNameAsyncRowEmpty() throws Exception {
    RowKey.metricNameAsync(tsdb, new byte[] { }).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void metricNameAsyncRowTooShort() throws Exception {
    RowKey.metricNameAsync(tsdb, new byte[] { 0, 1 }).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void metricNameAsyncMissingSalt() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    RowKey.metricNameAsync(tsdb, new byte[] { 0, 0, 1 }).joinUninterruptibly();
  }

  @Test (expected = NoSuchUniqueId.class)
  public void metricNameAsyncNoSuchUniqueId() throws Exception {
    final byte[] key = { 0, 0, 3, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.metricNameAsync(tsdb, key).joinUninterruptibly();
  }

  @Test (expected = NullPointerException.class)
  public void metricNameAsyncNullTsdb() throws Exception {
    final byte[] key = { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1};
    RowKey.metricNameAsync(null, key).joinUninterruptibly();
  }
  
  @Test
  public void rowKeyFromTSUID() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 2 };
    byte[] key = { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400));
    
    // zero timestamp
    key = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 0));
    
    // negative timestamp; honey badger don't care
    key = new byte[] { 0, 0, 1, -1, -21, 88, -128, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, -1356998400));
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyFromTSUIDNullTsuid() throws Exception {
    RowKey.rowKeyFromTSUID(tsdb, null, 1356998400);
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyFromTSUIDNullTsdb() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 2 };
    RowKey.rowKeyFromTSUID(null, tsuid, 1356998400);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void rowKeyFromTSUIDShortTsuid() throws Exception {
    final byte[] tsuid = { 0, 1 };
    RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void rowKeyFromTSUIDEmptyTsuid() throws Exception {
    final byte[] tsuid = { };
    RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400);
  }
  
  @Test
  public void rowKeyFromTSUIDNoTags() throws Exception {
    final byte[] tsuid = { 0, 0, 1 };
    byte[] key = { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400));
  }

  @Test
  public void rowKeyFromTSUIDMillis() throws Exception {
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 2 };
    byte[] key = { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400123L));
  }

  @Test
  public void rowKeyFromTSUIDSalted() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    
    final byte[] tsuid = { 0, 0, 1, 0, 0, 1, 0, 0, 2 };
    byte[] key = { 1, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400));
    
    // zero timestamp
    key = new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 0));
    
    // negative timestamp; honey badger don't care
    key = new byte[] { 1, 0, 0, 1, -1, -21, 88, -128, 0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, -1356998400));
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(4);
    key = new byte[] { 0, 0, 0, 1, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2 };
    assertArrayEquals(key, RowKey.rowKeyFromTSUID(tsdb, tsuid, 1356998400));
  }


  @Test
  public void getSalt() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(0);
    assertArrayEquals(new byte[] {}, RowKey.getSaltBytes(2));
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    assertArrayEquals(new byte[] { 2 }, RowKey.getSaltBytes(2));
    assertArrayEquals(new byte[] { 20 }, RowKey.getSaltBytes(20));
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(2);
    assertArrayEquals(new byte[] { 0, 20 }, RowKey.getSaltBytes(20));
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(4);
    assertArrayEquals(new byte[] { 0, 0, 0, 20 }, RowKey.getSaltBytes(20));
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    assertArrayEquals(new byte[] { -2 }, RowKey.getSaltBytes(-2));
  }
  
  @Test (expected = NegativeArraySizeException.class)
  public void getSaltNegativeWidth() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(-1);
    RowKey.getSaltBytes(2);
  }
  
  @Test
  public void prefixKeyWithSaltGlobalAndNoOps() {
    setupSalt();
    // short rows
    byte[] key = new byte[1];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[1], key);
    
    key = new byte[2];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[2], key);
    
    key = new byte[3];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[3], key);

    key = new byte[4];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[4], key);
    
    key = new byte[5];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[5], key);
    
    key = new byte[6];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[6], key);
    
    key = new byte[7];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[7], key);
    
    key = new byte[8];
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[8], key);

    key = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x50, (byte) 0xE2, 0x27, 0x00};
    byte[] compare = Arrays.copyOf(key, key.length);
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x01, 0x50, (byte) 0xE2, 0x27, 
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(0);
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltSameMetricDifferentTags() {
    setupSalt();
    byte[] key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 
        0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x10;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 
        0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x00;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x09;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 
        0x00, 0x00, 0x02, 0x00, 0x00, 0x03};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x03;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    // no tags. Shouldn't happen, but *shrug*
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 0x50, (byte) 0xE2, 0x27, 0x00 };
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x0C;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltDifferentMetricSameTags() {
    setupSalt();
    byte[] key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x02, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x09;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x03, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x06;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x0B, 0x14, 0x20, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x06;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltSameMetricSameTagsDifferentTimestamp() {
    setupSalt();
    byte[] key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x35, 0x10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x51, (byte) 0x0B, 0x13, (byte) 0x90, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { 0x00, 0x00, 0x00, 0x01, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltOverwrite() {
    setupSalt();
    // makes sure we ignore and overwrite anything in the salt position
    byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { (byte) 0xFF, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
    
    key = new byte[] { (byte) 0x0E, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test (expected = ArithmeticException.class)
  public void prefixKeyWithSaltZeroBucket() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(0);
    
    final byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    RowKey.prefixKeyWithSalt(key);
  }
  
  // This actually works, but PLEASE don't do it!
  @Test
  public void prefixKeyWithSaltNegativeBucket() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(-20);
    
    final byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    final byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x08;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltNegativeWidth() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(-1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    
    final byte[] key = new byte[] { 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01};
    final byte[] compare = Arrays.copyOf(key, key.length);
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltMissingTagV() {
    setupSalt();
    // Honey badger don't care
    final byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00, 0x00, 0x00, 0x01};
    final byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x0D;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test
  public void prefixKeyWithSaltPartialTagK() {
    setupSalt();
    // Honey badger don't care
    final byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27, 0x00 };
    final byte[] compare = Arrays.copyOf(key, key.length);
    compare[0] = 0x0C;
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(compare, key);
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void prefixKeyWithSaltMissingTags() {
    setupSalt();
    final byte[] key = new byte[] { 0x02, 0x00, 0x00, 0x01, 
        0x50, (byte) 0xE2, 0x27 };
    RowKey.prefixKeyWithSalt(key);
  }
  
  @Test (expected = NullPointerException.class)
  public void prefixKeyWithSaltNullKey() {
    setupSalt();
    RowKey.prefixKeyWithSalt(null);
  }
  
  @Test
  public void prefixKeyWithSaltEmptyKey() {
    setupSalt();
    final byte[] key = new byte[] {};
    RowKey.prefixKeyWithSalt(key);
    assertArrayEquals(new byte[] {}, key);
  }
  
  @Test
  public void rowKeyContainsMetric() {
    setupSalt();
    final byte[] metric = new byte[] { 0, 0, 1 };
    
    assertEquals(0, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // not there!
    assertEquals(-1, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // double salt bytes
    assertEquals(-1, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // missing salt but expecting it
    assertEquals(-1, RowKey.rowKeyContainsMetric(metric, 
        new byte[] {0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // both empty
    assertEquals(0, RowKey.rowKeyContainsMetric(new byte[] { }, 
        new byte[] { }));
    
    // empty metric returns 0 TODO(clarsen) see if we really want that
    assertEquals(0, RowKey.rowKeyContainsMetric(new byte[] { }, 
        new byte[] {0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyContainsMetricNullMetric() {
    setupSalt();
    RowKey.rowKeyContainsMetric(null, 
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 });
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyContainsMetricNullKey() {
    setupSalt();
    RowKey.rowKeyContainsMetric(new byte[] { 0, 0, 1 }, null);
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void rowKeyContainsMetricKeyTooShortSalted() {
    setupSalt();
    RowKey.rowKeyContainsMetric(new byte[] { 0, 0, 1 }, 
        new byte[] { 1, 0, 0 });
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void rowKeyContainsMetricKeyTooShortNoSalt() {
    RowKey.rowKeyContainsMetric(new byte[] { 0, 0, 1 }, 
        new byte[] { 0, 0 });
  }
  
  @Test
  public void rowKeyContainsMetricNoSalt() {
    final byte[] metric = new byte[] { 0, 0, 1 };
    
    assertEquals(0, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // not there!
    assertEquals(1, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // salted but shouldn't be
    assertEquals(-1, RowKey.rowKeyContainsMetric(metric, 
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
  }
  
  @Test
  public void saltCmp() {
    setupSalt();
    
    // diff salt, same keys
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // same addy
    final byte[] key = new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 };
    assertEquals(0, new RowKey.SaltCmp().compare(key, key));
    
    assertEquals(1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 4 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 4 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0 }));
    
    // nothing after the salt
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1 }, 
        new byte[] { 2 }));
    
    // empty
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { }, 
        new byte[] { }));
    
    // wider salt
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(3);
    
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1, 3, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 4, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1, 3, 5, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 4, 6, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 3, 5, 7, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 4, 6, 8, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
  }
  
  @Test (expected = NullPointerException.class)
  public void saltCmpNullA() throws Exception {
    setupSalt();
    new RowKey.SaltCmp().compare(null, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 });
  }
  
  @Test (expected = NullPointerException.class)
  public void saltCmpNullB() throws Exception {
    setupSalt();
    new RowKey.SaltCmp().compare( 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 },
        null);
  }
  
  @Test
  public void saltCmpNoSalt() {
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // diff salt, same keys
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    // same addy
    final byte[] key = new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 };
    assertEquals(0, new RowKey.SaltCmp().compare(key, key));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 4 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 4 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }));
    
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 3 }, 
        new byte[] { 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0 }));
    
    // nothing after the salt
    assertEquals(-1, new RowKey.SaltCmp().compare(
        new byte[] { 1 }, 
        new byte[] { 2 }));
    
    // empty
    assertEquals(0, new RowKey.SaltCmp().compare(
        new byte[] { }, 
        new byte[] { }));
  }
  
  /**
   * Mocks out the static Const class for a single salt byte with 20 buckets
   */
  private static void setupSalt() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
  }
 
}
