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
package net.opentsdb.meta;

import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesId;

/**
 * The result from a meta data store query.
 * 
 * @since 3.0
 */
public interface MetaDataStorageResult {
  public static enum MetaResult {
    DATA,
    NO_DATA_FALLBACK,
    NO_DATA,
    EXCEPTION_FALLBACK,
    EXCEPTION
  }
  
  public MetaResult result();
  
  public Throwable exception();
  
  public List<TimeSeriesId> timeSeries();
  
  public TypeToken<? extends TimeSeriesId> idType();

  public List<String> metrics();

  public Map<String, List<String>> tags();
}