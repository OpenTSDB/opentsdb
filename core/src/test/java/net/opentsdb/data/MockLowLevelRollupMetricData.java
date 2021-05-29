// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.data;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupDatum;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MockLowLevelRollupMetricData extends MockLowLevelMetricData
        implements LowLevelMetricData.LowLevelRollupMetricData {

  protected RollupConfig rollupConfig;
  protected Iterator<Integer> summaries;
  protected int summary;

  @Override
  public boolean advance() {
    if (summaries == null || !summaries.hasNext()) {
      if (++readIndex >= data.size()) {
        return false;
      }
      RollupDatum datum = (RollupDatum) data.get(readIndex);
      TimeSeriesValue<NumericSummaryType> value =
              (TimeSeriesValue<NumericSummaryType>) datum.value();
      summaries = value.value().summariesAvailable().iterator();
      summary = summaries.next();
    } else {
      summary = summaries.next();
    }
    currentId = (TimeSeriesDatumStringId) data.get(readIndex).id();
    flatTags = null;
    tagBitsIndex = 0;
    tagBitsReadIndex = 0;
    for (Map.Entry<String, String> entry : currentId.tags().entrySet()) {
      if (flatTags == null) {
        flatTags = entry.getKey().getBytes(StandardCharsets.UTF_8);
        tagBits[tagBitsIndex++] = 0;
        tagBits[tagBitsIndex++] = flatTags.length;
      } else {
        if (tagBitsIndex + 2 >= tagBits.length) {
          int[] temp = new int[tagBitsIndex * 2];
          System.arraycopy(tagBits, 0, temp, 0, tagBitsIndex);
          tagBits = temp;
        }
        byte[] tagk = entry.getKey().getBytes(StandardCharsets.UTF_8);
        tagBits[tagBitsIndex++] = flatTags.length;
        tagBits[tagBitsIndex++] = tagk.length;
        flatTags = Bytes.concat(flatTags, tagk);
      }

      byte[] tagv = entry.getValue().getBytes(StandardCharsets.UTF_8);
      tagBits[tagBitsIndex++] = flatTags.length;
      tagBits[tagBitsIndex++] = tagv.length;
      flatTags = Bytes.concat(flatTags, tagv);
    }
    return true;
  }

  @Override
  public ValueFormat valueFormat() {
    TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) data.get(readIndex).value();
    return value.value().value(summary).isInteger() ?
            ValueFormat.INTEGER : ValueFormat.DOUBLE;
  }

  @Override
  public long longValue() {
    TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) data.get(readIndex).value();
    return value.value().value(summary).longValue();
  }

  @Override
  public float floatValue() {
    TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) data.get(readIndex).value();
    return (float) value.value().value(summary).doubleValue();
  }

  @Override
  public double doubleValue() {
    TimeSeriesValue<NumericSummaryType> value =
            (TimeSeriesValue<NumericSummaryType>) data.get(readIndex).value();
    return value.value().value(summary).doubleValue();
  }

  public void setRollupConfig(final RollupConfig rollupConfig) {
    this.rollupConfig = rollupConfig;
  }

  @Override
  public String intervalString() {
    RollupDatum datum = (RollupDatum) data.get(readIndex);
    return datum.intervalString();
  }

  @Override
  public String intervalAggregatorString() {
    return rollupConfig.getAggregatorForId(summary);
  }

  @Override
  public int intervalAggregator() {
    return summary;
  }

  @Override
  public String groupByAggregatorString() {
    RollupDatum datum = (RollupDatum) data.get(readIndex);
    return datum.groupByAggregatorString();
  }

  @Override
  public int groupByAggregator() {
    return rollupConfig != null ?
            rollupConfig.getIdForAggregator(groupByAggregatorString()) : -1;
  }
}
