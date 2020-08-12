// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;

/**
 * Class that stores the predictions as a Result from a model.
 * 
 * @since 3.0
 */
public class EgadsPredictionResult implements QueryResult, TimeSpecification {
  private final QueryNode node;
  private final QueryResultId data_source;
  private final TimeStamp start;
  private final TimeStamp end;
  private final List<TimeSeries> series;
  private final TypeToken<? extends TimeSeriesId> id_type;

  public EgadsPredictionResult(final QueryNode node, 
                               final QueryResultId data_source,
                               final TimeStamp start, 
                               final TimeStamp end, 
                               final List<TimeSeries> series,
                               final TypeToken<? extends TimeSeriesId> id_type) {
    this.node = node;
    this.data_source = data_source;
    this.start = start;
    this.end = end;
    this.series = series;
    this.id_type = id_type;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return this;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
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
    return id_type;
  }

  @Override
  public ChronoUnit resolution() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RollupConfig rollupConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  @Override
  public TimeStamp start() {
    return start;
  }

  @Override
  public TimeStamp end() {
    return end;
  }

  @Override
  public TemporalAmount interval() {
    // TODO Auto-generated method stub
    return Duration.ofSeconds(60);
  }

  @Override
  public String stringInterval() {
    // TODO Auto-generated method stub
    return "1m";
  }

  @Override
  public ChronoUnit units() {
    // TODO Auto-generated method stub
    return ChronoUnit.MINUTES;
  }

  @Override
  public ZoneId timezone() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }
  
}
