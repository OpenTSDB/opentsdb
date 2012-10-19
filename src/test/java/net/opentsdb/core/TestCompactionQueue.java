// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import java.util.ArrayList;

import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import net.opentsdb.uid.UniqueId;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ CompactionQueue.class, CompactionQueue.Thrd.class,
                  TSDB.class, UniqueId.class })
final class TestCompactionQueue {

  private TSDB tsdb = mock(TSDB.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 0, 0, 1, 78, 36, -84, 42, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  private CompactionQueue compactionq;

  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", mock(UniqueId.class));
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(TSDB.class, "enable_compactions", true);
    // Stub out the compaction thread, so it doesn't even start.
    PowerMockito.whenNew(CompactionQueue.Thrd.class).withNoArguments()
      .thenReturn(mock(CompactionQueue.Thrd.class));
    compactionq = new CompactionQueue(tsdb);

    when(tsdb.put(anyBytes(), anyBytes(), anyBytes()))
      .thenAnswer(newDeferred());
    when(tsdb.delete(anyBytes(), any(byte[][].class)))
      .thenAnswer(newDeferred());
  }

  @Test
  public void emptyRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(0);
    compactionq.compact(kvs);

    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb, never()).put(anyBytes(), anyBytes(), anyBytes());
    // ... verify there were no delete.
    verify(tsdb, never()).delete(anyBytes(), any(byte[][].class));
  }

  @Test
  public void oneCellRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    final byte[] qual = { 0x00, 0x03 };
    kvs.add(makekv(qual, Bytes.fromLong(42L)));
    compactionq.compact(kvs);

    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb, never()).put(anyBytes(), anyBytes(), anyBytes());
    // ... verify there were no delete.
    verify(tsdb, never()).delete(anyBytes(), any(byte[][].class));
  }

  @Test
  public void twoCellRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);

    // We had one row to compact, so one put to do.
    verify(tsdb, times(1)).put(KEY, concat(qual1, qual2),
                               concat(val1, val2, ZERO));
    // And we had to delete individual cells.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual2 });
  }

  @Test
  public void fixQualifierFlags() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    // Note: here the flags pretend the value is on 4 bytes, but it's actually
    // on 8 bytes, so we expect the code to fix the flags as it's compacting.
    final byte[] qual1 = { 0x00, 0x03 };   // Pretends 4 bytes...
    final byte[] val1 = Bytes.fromLong(4L);    // ... 8 bytes actually.
    final byte[] cqual1 = { 0x00, 0x07 };  // Should have been this.
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);

    // We had one row to compact, so one put to do.
    verify(tsdb, times(1)).put(KEY, concat(cqual1, qual2),
                               concat(val1, val2, ZERO));
    // And we had to delete individual cells.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual2 });
  }

  @Test
  public void fixFloatingPoint() throws Exception {
    // Check that the compaction process is fixing incorrectly encoded
    // floating point values.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    // Note: here the flags pretend the value is on 4 bytes, but it's actually
    // on 8 bytes, so we expect the code to fix the flags as it's compacting.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x1B };  // +1s, float, 4 bytes.
    final byte[] val2 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] cval2 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);

    // We had one row to compact, so one put to do.
    verify(tsdb, times(1)).put(KEY, concat(qual1, qual2),
                               concat(val1, cval2, ZERO));
    // And we had to delete individual cells.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual2, });
  }

  @Test(expected=IllegalDataException.class)
  public void overlappingDataPoints() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    // Same data point (same time delta, 0), but encoded on 4 bytes, not 8.
    final byte[] qual2 = { 0x00, 0x03 };
    final byte[] val2 = Bytes.fromInt(4);
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);
  }

  @Test
  public void failedCompactNoop() throws Exception {
    // In this case, the row contains both the compacted form as well as the
    // non-compacted form.  This could happen if the TSD dies in between the
    // `put' of a compaction, before getting a change to do the deletes.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));
    final byte[] qualcompact = concat(qual1, qual2);
    final byte[] valcompact = concat(val1, val2, ZERO);
    kvs.add(makekv(qualcompact, valcompact));

    compactionq.compact(kvs);

    // We didn't have anything to write.
    verify(tsdb, never()).put(anyBytes(), anyBytes(), anyBytes());
    // We had to delete stuff in 1 row.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual2 });
  }

  @Test
  public void secondCompact() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = concat(qual1, qual2);
    kvs.add(makekv(qual12, concat(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    compactionq.compact(kvs);

    // We had one row to compact, so one put to do.
    verify(tsdb, times(1)).put(KEY, concat(qual1, qual3, qual2),
                               concat(val1, val3, val2, ZERO));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual12, qual3 });
  }

  @Test
  public void doubleFailedCompactNoop() throws Exception {
    // In this test the row has already been compacted once, but we didn't
    // clean up the individual data points.  Then another data point was
    // written, another compaction ran, but once again didn't delete the
    // individual data points.  So the rows contains 2 compacted cells and
    // several individual cells.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    // Data points 1 + 2 compacted.
    final byte[] qual12 = concat(qual1, qual2);
    // This data point came late.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    // Data points 1 + 3 + 2 compacted.
    final byte[] qual132 = concat(qual1, qual3, qual2);
    kvs.add(makekv(qual1, val1));
    kvs.add(makekv(qual132, concat(val1, val3, val2, ZERO)));
    kvs.add(makekv(qual12, concat(val1, val2, ZERO)));
    kvs.add(makekv(qual3, val3));
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);

    // We didn't have anything to write, the last cell is already the correct
    // compacted version of the row.
    verify(tsdb, never()).put(anyBytes(), anyBytes(), anyBytes());
    // And we had to delete the 3 individual cells + the first pre-existing
    // compacted cell.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual12, qual3, qual2 });
  }

  @Test
  public void weirdOverlappingCompactedCells() throws Exception {
    // Here we have two partially compacted cell, but they contain different
    // data points.  Although a possible scenario, this is extremely unlikely,
    // but we need to test that logic works in this case too.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    // Data points 1 + 2 compacted.
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = concat(qual1, qual2);
    kvs.add(makekv(qual12, concat(val1, val2, ZERO)));
    // This data point came late.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    // Data points 1 + 3 compacted.
    final byte[] qual13 = concat(qual1, qual3);
    kvs.add(makekv(qual13, concat(val1, val3, ZERO)));
    kvs.add(makekv(qual3, val3));
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs);

    // We had one row to compact, so one put to do.
    verify(tsdb, times(1)).put(KEY, concat(qual1, qual3, qual2),
                               concat(val1, val3, val2, ZERO));
    // And we had to delete the 3 individual cells + 2 pre-existing
    // compacted cells.
    verify(tsdb, times(1)).delete(KEY, new byte[][] { qual1, qual12, qual13, qual3, qual2 });
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  /** Shorthand to create a {@link KeyValue}.  */
  private static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    return new KeyValue(KEY, FAMILY, qualifier, value);
  }

  /** Concatenates byte arrays together.  */
  private static byte[] concat(final byte[]... arrays) {
    int len = 0;
    for (final byte[] array : arrays) {
      len += array.length;
    }
    final byte[] result = new byte[len];
    len = 0;
    for (final byte[] array : arrays) {
      System.arraycopy(array, 0, result, len, array.length);
      len += array.length;
    }
    return result;
  }

  private static byte[] anyBytes() {
    return any(byte[].class);
  }

  /** Creates a new Deferred that's already called back.  */
  private static <T> Answer<Deferred<T>> newDeferred() {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(null);
      }
    };
  }

}
