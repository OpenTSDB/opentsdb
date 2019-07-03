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

public class StatusValue implements StatusType, TimeSeriesValue<StatusType> {

  private byte statusCode;
  private byte[] statusCodeArray;
  private byte statusType;
  private String message;
  private TimeStamp lastUpdateTime;
  private TimeStamp[] timestampArray;

  public StatusValue(
      final byte statusCode,
      final byte[] statusCodeArray,
      final byte statusType,
      final String message,
      final TimeStamp lastUpdateTime,
      final TimeStamp[] timestampArray) {
    this.statusCode = statusCode;
    this.statusCodeArray = statusCodeArray;
    this.statusType = statusType;
    this.message = message;
    this.lastUpdateTime = lastUpdateTime;
    this.timestampArray = timestampArray;
  }

  @Override
  public TimeStamp timestamp() {
    return lastUpdateTime;
  }

  @Override
  public TimeStamp[] timestampArray() {
    return timestampArray;
  }

  @Override
  public StatusType value() {
    return this;
  }

  @Override
  public TypeToken<StatusType> type() {
    return StatusType.TYPE;
  }

  @Override
  public String message() {
    return message;
  }

  @Override
  public byte statusCode() {
    return statusCode;
  }

  @Override
  public byte[] statusCodeArray() {
    return statusCodeArray;
  }

  @Override
  public byte statusType() {
    return statusType;
  }

  @Override
  public TimeStamp lastUpdateTime() {
    return lastUpdateTime;
  }
}
