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
package net.opentsdb.data;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;

/**
 * A simple implementation of a PTS that contains no data. Can be used when a 
 * set or segment doesn't have any data.
 */
public class NoDataPartialTimeSeries implements 
    PartialTimeSeries<NumericLongArrayType>, 
    CloseablePooledObject {

  /** The pooled object ref. */
  protected PooledObject pooled_object;
  
  /** The set. */
  protected PartialTimeSeriesSet set;
  
  /**
   * Call after acquiring from the pool to set the reference.
   * @param set The non-null set to store a reference to.
   */
  public void reset(final PartialTimeSeriesSet set) {
    this.set = set;
  }
  
  @Override
  public void close() throws Exception {
    set = null;
    release();
  }

  @Override
  public long idHash() {
    return 0;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }
  
  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }
  
  @Override
  public NumericLongArrayType value() {
    return null;
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }

  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }

}