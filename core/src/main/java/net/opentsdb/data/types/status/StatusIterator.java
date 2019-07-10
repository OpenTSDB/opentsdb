// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
import net.opentsdb.data.TypedTimeSeriesIterator;

public class StatusIterator extends StatusValue implements TypedTimeSeriesIterator<StatusType> {

  private String namespace;
  boolean has_next = true;

  public StatusIterator(
      String namespace,
      String application,
      byte statusCode,
      byte[] statusCodeArray,
      byte statusType,
      String message,
      TimeStamp lastUpdateTime,
      TimeStamp[] timestampArray) {
    super(
        application,
        statusCode,
        statusCodeArray,
        statusType,
        message,
        lastUpdateTime,
        timestampArray);
    this.namespace = namespace;
  }

  @Override
  public TypeToken<StatusType> getType() {
    return StatusType.TYPE;
  }

  @Override
  public boolean hasNext() {
    boolean had = has_next;
    has_next = false;
    return had;
  }

  @Override
  public TimeSeriesValue<StatusType> next() {
    return this;
  }

  public String namespace() {
    return namespace;
  }
}
