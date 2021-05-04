// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.WriteStatus;
import net.opentsdb.utils.Bytes;

/**
 * A codec for handling TSDB 2.x rollup data points.
 * 
 * @since 3.0
 */
public class NumericSummaryCodec extends BaseCodec {

  NumericSummaryCodec() {
    values = new byte[4][];
    qualifiers = new byte[4][];
    valueLengths = new int[4];
    qualifierLengths = new int[4];
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericSummaryType.TYPE;
  }

  @Override
  public Span<? extends TimeSeriesDataType> newSequences(
      final boolean reversed) {
    return new NumericSummarySpan(reversed);
  }

  @Override
  public RowSeq newRowSeq(final long base_time) {
    throw new UnsupportedOperationException("This isn't used for rollup "
        + "sequences.");
  }

  @Override
  public WriteStatus encode(
          final TimeSeriesValue<? extends TimeSeriesDataType> value,
          final boolean append_format,
          final int base_time,
          final DefaultRollupInterval rollup_interval) {
    if (value.type() != NumericSummaryType.TYPE) {
      // TODO - metrics and log
      return WriteStatus.REJECTED;
    }

    final NumericSummaryType summary =
            ((TimeSeriesValue<NumericSummaryType>) value).value();
    if (summary == null) {
      // TODO - metrics and log
      return WriteStatus.REJECTED;
    }

    reset();

    // TODO - size and pass arrays into the encoding functions so we don't
    // continue to allocate all of these tiny byte arrays.
    for (int i : summary.summariesAvailable()) {
      if (encodedValues >= qualifiers.length) {
        grow();
      }

      // value first
      final NumericType v = summary.value(i);
      if (v == null) {
        // TODO - metrics and log
        continue;
      }

      final boolean is_float;
      if (v.isInteger()) {
        values[encodedValues] = NumericCodec.vleEncodeLong(v.longValue());
        is_float = false;
      } else {
        final double d = v.doubleValue();
        if (!Double.isFinite(d)) {
          // TODO - metrics and log
          continue;
        }
        if ((float) d == d) {
          values[encodedValues] = Bytes.fromInt(Float.floatToIntBits((float) d));
        } else {
          values[encodedValues] = Bytes.fromLong(Double.doubleToRawLongBits(d));
        }
        is_float = true;
      }

      final short flags = (short) ((is_float ? NumericCodec.FLAG_FLOAT :
              (short) 0) | (short) (values[encodedValues].length - 1));
      if (append_format) {
        if (qualifiers[encodedValues] != null &&
                qualifiers[encodedValues].length >= 1) {
          qualifiers[encodedValues][0] = (byte) i;
        } else {
          qualifiers[encodedValues] = new byte[] { (byte) i };
        }
        qualifierLengths[encodedValues] = 1;
        values[encodedValues] = RollupUtils.buildAppendRollupValue(
                value.timestamp().epoch(),
                base_time,
                flags,
                rollup_interval,
                values[encodedValues]);
      } else {
        // TODO - If the timestamp has a millisecond resolution or something we
        // could be stepping on data. No good there.
        qualifiers[encodedValues] = RollupUtils.buildRollupQualifier(
                value.timestamp().epoch(), flags, i, rollup_interval);
        qualifierLengths[encodedValues] = qualifiers[encodedValues].length;
      }
      valueLengths[encodedValues] = values[encodedValues].length;
      encodedValues++;
    }

    return WriteStatus.OK;
  }

  private void grow() {
    byte[][] tempBytes = new byte[qualifiers.length * 2][];
    System.arraycopy(qualifiers, 0, tempBytes, 0, encodedValues);
    qualifiers = tempBytes;

    tempBytes = new byte[values.length * 2][];
    System.arraycopy(values, 0, tempBytes, 0, encodedValues);
    values = tempBytes;

    int[] tempInts = new int[qualifierLengths.length * 2];
    System.arraycopy(qualifierLengths, 0, tempInts, 0, encodedValues);
    qualifierLengths = tempInts;

    tempInts = new int[valueLengths.length * 2];
    System.arraycopy(valueLengths, 0, tempInts, 0, encodedValues);
    valueLengths = tempInts;
  }
}