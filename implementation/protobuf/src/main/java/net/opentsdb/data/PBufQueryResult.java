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
package net.opentsdb.data;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.QueryResultPB;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.PBufIteratorSerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.rollup.RollupConfig;

/**
 * Implementation of the {@link QueryResult} interface using Protobuf.
 * 
 * @since 3.0
 */
public class PBufQueryResult implements QueryResult {
  
  /** The factory used to serdes data types. */
  private final PBufIteratorSerdesFactory factory;
  
  /** The protobuf result. */
  private QueryResultPB.QueryResult result;
  
  /** The query node that deserialized this data. */
  private final QueryNode node;
  
  /**
   * Default ctor.
   * @param factory A non-null factory.
   * @param node A non-null node that owns this data.
   * @param options A non-null options object.
   * @param stream The input stream to parse.
   */
  public PBufQueryResult(final PBufIteratorSerdesFactory factory, 
                         final QueryNode node, 
                         final SerdesOptions options, 
                         final InputStream stream) {
    this.factory = factory;
    this.node = node;
    try {
      result = QueryResultPB.QueryResult.parseFrom(stream);
    } catch (IOException e) {
      throw new SerdesException("Failed to parse the query results.", e);
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    final List<TimeSeries> series = Lists.newArrayListWithCapacity(
        result.getTimeseriesCount());
    for (final TimeSeriesPB.TimeSeries time_series : 
        result.getTimeseriesList()) {
      series.add(new PBufTimeSeries(factory, time_series));
    }
    return series;
  }

  @Override
  public long sequenceId() {
    return result.getSequenceId();
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return ChronoUnit.values()[result.getResolution()];
  }
  
  @Override
  public RollupConfig rollupConfig() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void close() {
    // no-op
  }

}
