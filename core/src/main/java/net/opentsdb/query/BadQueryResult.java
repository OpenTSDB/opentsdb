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
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.rollup.RollupConfig;

/**
 * A simple buildable class for a failed query.
 */
public class BadQueryResult implements QueryResult {

  private final Throwable exception;
  private final String error;
  private final QueryNode node;
  private final QueryResultId data_source;
  
  private BadQueryResult(final Builder builder) {
    exception = builder.exception;
    error = Strings.isNullOrEmpty(builder.error) ? 
        exception.getMessage() : builder.error;
    node = builder.node;
    data_source = builder.data_source;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return null;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return Collections.emptyList();
  }

  @Override
  public String error() {
    return error;
  }

  @Override
  public Throwable exception() {
    return exception;
  }

  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return data_source;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return ChronoUnit.SECONDS;
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }

  @Override
  public void close() {
    
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Throwable exception;
    private String error;
    private QueryNode node;
    private QueryResultId data_source;
    
    public Builder setException(final Throwable exception) {
      this.exception = exception;
      return this;
    }
    
    public Builder setError(final String error) {
      this.error = error;
      return this;
    }
    
    public Builder setNode(final QueryNode node) {
      this.node = node;
      return this;
    }
    
    public Builder setDataSource(final QueryResultId data_source) {
      this.data_source = data_source;
      return this;
    }
    
    public BadQueryResult build() {
      if (Strings.isNullOrEmpty(error) && exception == null) {
        throw new IllegalArgumentException("Must have an error string or "
            + "an exception.");
      }
      if (data_source == null || 
          Strings.isNullOrEmpty(data_source.nodeID()) || 
          Strings.isNullOrEmpty(data_source.dataSource())) {
        throw new IllegalArgumentException("Data source cannot be null or empty.");
      }
      if (node == null) {
        throw new IllegalArgumentException("Node cannot be null.");
      }
      return new BadQueryResult(this);
    }
  }
  
}
