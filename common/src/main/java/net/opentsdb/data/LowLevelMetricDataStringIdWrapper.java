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

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import net.opentsdb.common.Const;

import java.util.Map;

public class LowLevelMetricDataStringIdWrapper implements TimeSeriesDatumStringId {

  private LowLevelMetricData data;
  private Map<String, String> tags;

  public LowLevelMetricDataStringIdWrapper(final LowLevelMetricData data) {
    this.data = data;
  }

  public void reset(final LowLevelMetricData data) {
    this.data = data;
    if (tags != null) {
      tags.clear();
    }
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_STRING_ID;
  }

  @Override
  public long buildHashCode() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String namespace() {
    return null;
  }

  @Override
  public String metric() {
    return new String(data.metricBuffer(),
            data.metricStart(),
            data.metricLength(),
            Const.UTF8_CHARSET);
  }

  @Override
  public Map<String, String> tags() {
    if (tags != null && !tags.isEmpty()) {
      // use cached
      return tags;
    }

    if (tags == null) {
      tags = Maps.newHashMap();
    }
    while (data.advanceTagPair()) {
      String tag_key = new String(data.tagsBuffer(),
              data.tagKeyStart(),
              data.tagKeyLength(),
              Const.UTF8_CHARSET);

      String tag_value = new String(data.tagsBuffer(),
              data.tagValueStart(),
              data.tagValueLength(),
              Const.UTF8_CHARSET);

      tags.put(tag_key, tag_value);
    }
    return tags;
  }

  @Override
  public int compareTo(TimeSeriesDatumId o) {
    throw new UnsupportedOperationException("TODO");
  }
}
