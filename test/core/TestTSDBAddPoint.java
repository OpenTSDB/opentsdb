// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.hbase.async.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.NoSuchUniqueName;

public class TestTSDBAddPoint extends BaseTsdbTest {

  @Before
  public void beforeLocal() throws Exception {
    setDataPointStorage();
  }
  
  @Test
  public void addPointLong1Byte() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointWithOTSDBTimeStamp() throws Exception {
	  long ts = 1356998400;
	  tsdb.getConfig().overrideConfig("tsd.storage.use_otsdb_timestamp", "true");
	  tsdb.addPoint(METRIC_STRING, ts, 42, tags).joinUninterruptibly();
		final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1 };
		TreeMap<Long, byte[]> result = storage.getFullColumn(tsdb.dataTable(), row, tsdb.FAMILY(), new byte[] { 0, 0 });
		assert(result != null);
		for (Map.Entry<Long, byte[]> e : result.entrySet()) {
			long retrievedTs = e.getKey();
			assert((ts * 1000) == retrievedTs);
		}
  }
  
  @Test
  public void addPointWithoutOTSDBTimeStamp() throws Exception {
	long ts = 1356998400;
	tsdb.getConfig().overrideConfig("tsd.storage.use_otsdb_timestamp", "false");
	tsdb.addPoint(METRIC_STRING, ts, 42, tags).joinUninterruptibly();
	final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1 };
	TreeMap<Long, byte[]> result = storage.getFullColumn(tsdb.dataTable(), row, tsdb.FAMILY(), new byte[] { 0, 0 });
	assert(result != null);
	for (Map.Entry<Long, byte[]> e : result.entrySet()) {
	  long retrievedTs = e.getKey();
	  assert((ts * 1000) != retrievedTs);
	}
  }
  
  @Test
  public void addPointLong1ByteNegative() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, -42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(-42, value[0]);
  }
  
  @Test
  public void addPointLong2Bytes() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 257, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(257, Bytes.getShort(value));
  }
  
  @Test
  public void addPointLong2BytesNegative() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, -257, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(-257, Bytes.getShort(value));
  }
  
  @Test
  public void addPointLong4Bytes() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 65537, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(65537, Bytes.getInt(value));
  }
  
  @Test
  public void addPointLong4BytesNegative() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, -65537, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(-65537, Bytes.getInt(value));
  }
  
  @Test
  public void addPointLong8Bytes() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 4294967296L, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(4294967296L, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLong8BytesNegative() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, -4294967296L, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(-4294967296L, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLongMs() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400500L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointLongMany() throws Exception {
    long timestamp = 1356998400;
    for (int i = 1; i <= 50; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp++, i, tags).joinUninterruptibly();
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, storage.numColumns(row));
  }
  
  @Test
  public void addPointLongManyMs() throws Exception {
    long timestamp = 1356998400500L;
    for (int i = 1; i <= 50; i++) {
      tsdb.addPoint(METRIC_STRING, timestamp++, i, tags).joinUninterruptibly();
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, storage.numColumns(row));
  }
  
  @Test
  public void addPointLongEndOfRow() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1357001999, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xE0, 
        (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointLongOverwrite() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400, 24, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void addPointNoAutoMetric() throws Exception {
    tsdb.addPoint(NSUN_METRIC, 1356998400, 42, tags).joinUninterruptibly();
  }

  @Test
  public void addPointSecondZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    tsdb.addPoint(METRIC_STRING, 0, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointSecondOne() throws Exception {
    // hey, it's valid *shrug* Thu, 01 Jan 1970 00:00:01 GMT
    tsdb.addPoint(METRIC_STRING, 1, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 16 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointSecond2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15 GMT
    tsdb.addPoint(METRIC_STRING, 4294967295L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0x69, (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointSecondNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    tsdb.addPoint(METRIC_STRING, -2147483648, 42, tags).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void emptyTagValue() throws Exception {
    tags.put(TAGK_STRING, "");
    tsdb.addPoint(METRIC_STRING, 1234567890, 42, tags).joinUninterruptibly();
  }

  @Test
  public void addPointMS1970() throws Exception {
    // Since it's just over Integer.MAX_VALUE, OpenTSDB will treat this as
    // a millisecond timestamp since it doesn't fit in 4 bytes.
    // Base time is 4294800 which is Thu, 19 Feb 1970 17:00:00 GMT
    // offset = F0A36000 or 167296 ms
    tsdb.addPoint(METRIC_STRING, 4294967296L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, (byte) 0x41, (byte) 0x88, 
        (byte) 0x90, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xF0, 
        (byte) 0xA3, 0x60, 0});
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointMS2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15.000 GMT
    tsdb.addPoint(METRIC_STRING, 4294967295000L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xF6, 
        (byte) 0x77, 0x46, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointMS2286() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    tsdb.addPoint(METRIC_STRING, 9999999999999L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0x54, (byte) 0x0B, (byte) 0xD9, 
        0x10, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xFA, 
        (byte) 0xAE, 0x5F, (byte) 0xC0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test  (expected = IllegalArgumentException.class)
  public void addPointMSTooLarge() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    tsdb.addPoint(METRIC_STRING, 10000000000000L, 42, tags).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointMSNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint(METRIC_STRING, -2147483648000L, 42, tags).joinUninterruptibly();
  }

  @Test
  public void addPointFloat() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatNegative() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint(METRIC_STRING, 1356998400, -42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(-42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatMs() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400500L, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatEndOfRow() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint(METRIC_STRING, 1357001999, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { (byte) 0xE0, 
        (byte) 0xFB });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatPrecision() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 42.5123459999F, tags)
      .joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.512345F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatOverwrite() throws Exception {
    tsdb.addPoint(METRIC_STRING, 1356998400, 42.5F, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400, 25.4F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(25.4F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeIntAndFloat() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs? 
    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertEquals(2, storage.numColumns(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = storage.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeIntAndFloatMs() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs? 
    tsdb.addPoint(METRIC_STRING, 1356998400500L, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400500L, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = storage.getColumn(row, new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertEquals(2, storage.numColumns(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = storage.getColumn(row, new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeSecondAndMs() throws Exception {
    // this can happen if a second and an ms data point are stored for the same
    // timestamp.
    tsdb.addPoint(METRIC_STRING, 1356998400L, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400000L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertEquals(2, storage.numColumns(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = storage.getColumn(row, new byte[] { (byte) 0xF0, 0, 0, 0 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointWithSalt() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 8, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }

  @Test
  public void addPointWithSaltDifferentTags() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);

    tags.put(TAGK_STRING, TAGV_B_STRING);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 9, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointWithSaltDifferentTime() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);

    tsdb.addPoint(METRIC_STRING, 1359680400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 8, 0, 0, 1, 0x51, (byte) 0x0B, 0x13, 
        (byte) 0x90, 0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointAppend() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42 }, value);
  }
  
  @Test
  public void addPointAppendWithOffset() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);
    
    tsdb.addPoint(METRIC_STRING, 1356998430, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 1, -32, 42 }, value);
  }
  
  @Test
  public void addPointAppendAppending() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 24, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998460, 1, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42, 1, -32, 24, 3, -64, 1 }, value);
  }
  
  @Test
  public void addPointAppendAppendingOutOfOrder() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998460, 1, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 24, tags).joinUninterruptibly();

    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42, 3, -64, 1, 1, -32, 24 }, value);
  }
  
  @Test
  public void addPointAppendAppendingDuplicates() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 24, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 1, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42, 1, -32, 24, 1, -32, 1 }, value);
  }
  
  @Test
  public void addPointAppendMS() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400050L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { (byte) 0xF0, 0, 12, -128, 42 }, value);
  }
  
  @Test
  public void addPointAppendAppendingMixMS() throws Exception {
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998400050L, 1, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 24, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 
        0, 0, 42, (byte) 0xF0, 0, 12, -128, 1, 1, -32, 24 }, value);
  }
  
  @Test
  public void addPointAppendWithSalt() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 8, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42 }, value);
  }
  
  @Test
  public void addPointAppendAppendingWithSalt() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(20);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    Whitebox.setInternalState(config, "enable_appends", true);

    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998430, 24, tags).joinUninterruptibly();
    tsdb.addPoint(METRIC_STRING, 1356998460, 1, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 8, 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER);
    assertArrayEquals(new byte[] { 0, 0, 42, 1, -32, 24, 3, -64, 1 }, value);
  }
  
  @Test
  public void dpFilterOK() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromResult(true));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
    
    verify(filter, times(1)).filterDataPoints();
    verify(filter, times(1)).allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort());
  }
  
  @Test
  public void dpFilterBlocked() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromResult(false));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNull(value);
    
    verify(filter, times(1)).filterDataPoints();
    verify(filter, times(1)).allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort());
  }
  
  @Test
  public void dpFilterReturnsException() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenReturn(Deferred.<Boolean>fromError(new UnitTestException("Boo!")));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    final Deferred<Object> deferred = 
        tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags);
    try {
      deferred.join();
      fail("Expected an UnitTestException");
    } catch (UnitTestException e) { };
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNull(value);
    
    verify(filter, times(1)).filterDataPoints();
    verify(filter, times(1)).allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort());
  }
  
  @Test
  public void uidFilterThrowsException() throws Exception {
    final WriteableDataPointFilterPlugin filter = 
        mock(WriteableDataPointFilterPlugin.class);
    when(filter.filterDataPoints()).thenReturn(true);
    when(filter.allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort()))
      .thenThrow(new UnitTestException("Boo!"));
    Whitebox.setInternalState(tsdb, "ts_filter", filter);
    
    try {
      tsdb.addPoint(METRIC_STRING, 1356998400, 42, tags);
      fail("Expected an UnitTestException");
    } catch (UnitTestException e) { };
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = storage.getColumn(row, new byte[] { 0, 0 });
    assertNull(value);
    
    verify(filter, times(1)).filterDataPoints();
    verify(filter, times(1)).allowDataPoint(eq(METRIC_STRING), anyLong(), 
        any(byte[].class), eq(tags), anyShort());
  }
}
