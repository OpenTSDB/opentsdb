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
import net.opentsdb.data.types.numeric.NumericSummaryType;

/**
 * A codec for handling TSDB 2.x rollup data points.
 * 
 * @since 3.0
 */
public class NumericSummaryCodec implements Codec {

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

}