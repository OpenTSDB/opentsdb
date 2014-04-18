// This file is part of OpenTSDB.
// Copyright (C) 2011-2014  The OpenTSDB Authors.
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

import java.util.Arrays;
import org.hbase.async.KeyValue;

/**
* Internal implementation detail for {@link net.opentsdb.core.CompactionQueue}.
*/
class Column implements Comparable<Column> {

  /**
   * @return true if this column needs one or more fixups applied.
   */
  public boolean needsFixup() {
    return needsFixup;
  }

  public final long rowTs;

  // immutable once the constructor returns, but may need to be adjusted for fixups
  public byte[] qualifier;
  public byte[] value;
  private boolean needsFixup;

  // pointers into the qualifier/value buffers
  private int qualifierOffset;
  private int valueOffset;

  // data from the current point
  private int currentTs;
  private int currentQualLength;
  private int currentValLength;
  private boolean isMs;

  /**
   * Create an entry for a column, which will be able to iterate over the individual values
   * contained in it.
   *
   * NOTE: This currently does not expect to be handed a column containing non-datapoint values.
   *
   * @param kv
   */
  public Column(final KeyValue kv) {
    this.rowTs = kv.timestamp();
    this.qualifier = kv.qualifier();
    this.value = kv.value();
    qualifierOffset = 0;
    valueOffset = 0;
    checkForFixup();
    update();
  }

  private void checkForFixup() {
    // fixups predate compaction and ms-resolution timestamps, so are all exactly 2 bytes
    if (qualifier.length == 2) {
      byte qual1 = qualifier[1];
      if (Internal.floatingPointValueToFix(qual1, value)) {
        value = Internal.fixFloatingPointValue(qual1, value);
        needsFixup = true;
      }
      byte lenByte = Internal.fixQualifierFlags(qual1, value.length);
      if (lenByte != qual1) {
        qualifier = new byte[] { qualifier[0], lenByte };
        needsFixup = true;
      }
    }
  }

  /**
   * @return true if there are datapoints in this column.
   */
  public boolean hasMoreData() {
    return qualifierOffset < qualifier.length;
  }

  public int getTimestampOffset() {
    return currentTs;
  }

  public boolean isMilliseconds() {
    return isMs;
  }

  /**
   * Copy this value to the output and advance to the next one.
   *
   * @param compQualifier
   * @param compValue
   * @return true if there is more data left in this column
   */
  public void writeToBuffers(ByteBufferList compQualifier, ByteBufferList compValue) {
    compQualifier.add(qualifier, qualifierOffset, currentQualLength);
    compValue.add(value, valueOffset, currentValLength);
  }

  public int getCurrentQualiferLength() {
    return currentQualLength;
  }

  public byte[] getCopyOfCurrentValue() {
    if (needsFixup) {
      assert valueOffset == 0; // fixups should only be in single-value columns
      return Internal.fixFloatingPointValue(qualifier[qualifierOffset + 1], value);
    } else {
      return Arrays.copyOfRange(value, valueOffset, valueOffset + currentValLength);
    }
  }

  public boolean advance() {
    qualifierOffset += currentQualLength;
    valueOffset += currentValLength;
    return update();
  }

  private boolean update() {
    if (qualifierOffset >= qualifier.length || valueOffset >= value.length) {
      return false;
    }
    if (Internal.inMilliseconds(qualifier[qualifierOffset])) {
      currentQualLength = 4;
      isMs = true;
    } else {
      currentQualLength = 2;
      isMs = false;
    }
    currentTs = Internal.getOffsetFromQualifier(qualifier, qualifierOffset);
    currentValLength = Internal.getValueLengthFromQualifier(qualifier, qualifierOffset);
    return true;
  }

  // order in ascending order by timestamp, descending order by row timestamp (so we find the
  // entry we are going to keep first, and don't have to copy over it)
  @Override public int compareTo(Column o) {
    int c = Integer.compare(currentTs, o.currentTs);
    if (c == 0) {
      // note inverse order of comparison!
      c = Long.compare(o.rowTs, rowTs);
    }
    return c;
  }

  @Override public String toString() {
    return "q=" + Arrays.toString(qualifier) + " [ofs=" + qualifierOffset + "], v="
        + Arrays.toString(value) + " [ofs=" + valueOffset + "], ts=" + currentTs;
  }
}
