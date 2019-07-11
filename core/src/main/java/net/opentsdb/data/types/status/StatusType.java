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
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

/**
 * Represents a status
 *
 * @since 3.0
 */
public interface StatusType extends TimeSeriesDataType<StatusType> {

  TypeToken<StatusType> TYPE = TypeToken.of(StatusType.class);

  String application();

  String message();

  byte statusCode();

  byte[] statusCodeArray();

  TimeStamp timestamp();

  TimeStamp[] timestampArray();

  byte statusType();

  TimeStamp lastUpdateTime();
}
