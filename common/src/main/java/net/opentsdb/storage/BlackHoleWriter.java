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
package net.opentsdb.storage;

import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.stats.Span;

/**
 * Simple writer that just dumps the data into the bit-bucket in the sky.
 * 
 * @since 3.0
 */
public class BlackHoleWriter implements WritableTimeSeriesDataStore, TSDBPlugin {

  /**
   * Default ctor.
   */
  public BlackHoleWriter() {
    
  }
  
  @Override
  public String id() {
    return "BlackHoleWriter";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<WriteStatus> write(final AuthState state, 
                                     final TimeSeriesDatum datum,
                                     final Span span) {
    return Deferred.fromResult(WriteStatus.OK);
  }

  @Override
  public Deferred<List<WriteStatus>> write(final AuthState state,
                                           final TimeSeriesSharedTagsAndTimeData data, 
                                           final Span span) {
    final List<WriteStatus> list = Lists.newArrayListWithExpectedSize(data.size());
    for (int i = 0; i < data.size(); i++) {
      list.add(WriteStatus.OK);
    }
    return Deferred.fromResult(list);
  }

}
