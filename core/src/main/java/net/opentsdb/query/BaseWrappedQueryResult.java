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
package net.opentsdb.query;

import java.time.temporal.ChronoUnit;
import java.util.Collection;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.rollup.RollupConfig;

/**
 * The base class for wrapped results wherein the implementation doesn't
 * modify most of the underlying result but only needs to override a 
 * method or two.
 *
 * @since 3.0
 */
public abstract class BaseWrappedQueryResult implements QueryResult {

  /** The result. */
  protected final QueryResult result;
  
  /**
   * Default ctor.
   * @param result A non-null result to work from.
   */
  public BaseWrappedQueryResult(final QueryResult result) {
    this.result = result;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return result.timeSpecification();
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return result.timeSeries();
  }

  @Override
  public long sequenceId() {
    return result.sequenceId();
  }

  @Override
  public QueryNode source() {
    return result.source();
  }

  @Override
  public String dataSource() {
    return result.dataSource();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return result.idType();
  }

  @Override
  public ChronoUnit resolution() {
    return result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return result.rollupConfig();
  }

  @Override
  public void close() {
    result.close();
  }

}
