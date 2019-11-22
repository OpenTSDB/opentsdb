// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

package net.opentsdb.data.types.status;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

public class StatusGroupValue implements StatusGroupType, TimeSeriesValue<StatusGroupType> {

  private StatusValue[] statuses;
  private Summary summary;

  public StatusGroupValue(StatusValue[] statuses, Summary summary) {
    this.statuses = statuses;
    this.summary = summary;
  }

  @Override
  public TimeStamp timestamp() {
    return null;
  }

  @Override
  public StatusGroupType value() {
    return null;
  }

  @Override
  public TypeToken<StatusGroupType> type() {
    return StatusGroupType.TYPE;
  }

  @Override
  public StatusValue[] statuses() {
    return statuses;
  }

  @Override
  public Summary summary() {
    return summary;
  }
}
