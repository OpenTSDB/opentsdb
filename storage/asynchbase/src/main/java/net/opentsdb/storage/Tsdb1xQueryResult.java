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

import java.util.Collection;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * A query result generated by the Tsdb1xQueryNode
 * 
 * @since 3.0
 */
public class Tsdb1xQueryResult implements QueryResult {

  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  public void addData(final TimeStamp base, 
                      final byte[] tsuid, 
                      final byte prefix, 
                      final byte[] qualifier, 
                      final byte[] value) {
  
  }
  
  public boolean isFull() {
    // TODO - implement
    return false;
  }
}
