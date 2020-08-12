// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
import java.util.List;

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

  /** The query node this came from, if set. */
  protected final QueryNode source;
  
  /** The result. */
  protected final QueryResult result;
  
  /** The ID of this result. If null we pass-through the result's ID. */
  protected QueryResultId id;
  
  /**
   * Default ctor.
   * @param result A non-null result to work from.
   */
  public BaseWrappedQueryResult(final QueryResult result) {
    source = null;
    this.result = result;
  }

  /**
   * Ctor that allows for a different query node source that is used to override
   * the node ID of the result ID. The result data source is carried through.
   * @param source The non-null query source.
   * @param result The non-null result.
   */
  public BaseWrappedQueryResult(final QueryNode source, final QueryResult result) {
    this.source = source;
    this.result = result;
    if (result.dataSource() == null) {
      throw new IllegalArgumentException("The result cannot have a null "
          + "data source.");
    }
    id = new DefaultQueryResultId(source.config().getId(), 
        result.dataSource().dataSource());
  }
  
  @Override
  public QueryNode source() {
    return source == null ? result.source() : source;
  }
  
  @Override
  public QueryResultId dataSource() {
    return id == null ? result.dataSource() : id;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return result == null ? null : result.timeSeries();
  }

  @Override
  public String error() {
    return result.error();
  }
  
  @Override
  public Throwable exception() {
    return result.exception();
  }
  
  @Override
  public long sequenceId() {
    return result.sequenceId();
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

  @Override
  public boolean processInParallel() {
    return result.processInParallel();
  }
}
