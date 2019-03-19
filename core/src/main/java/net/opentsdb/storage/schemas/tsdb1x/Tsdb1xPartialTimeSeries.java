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
package net.opentsdb.storage.schemas.tsdb1x;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.ObjectPool;

public interface Tsdb1xPartialTimeSeries extends PartialTimeSeries, 
    CloseablePooledObject {

  /**
   * Sorts, de-duplicates and optionally reverses the data in this series. Call
   * it only after adding all of the data.
   * @param keep_earliest Whether or not to keep the earliest duplicates in the
   * array or the latest. 
   * @param reverse Whether or not to reverse the data.
   */
  public void dedupe(final boolean keep_earliest, final boolean reverse);
 
  /**
   * Sets the ID hash and the set but leaves the array null.
   * @param id_hash
   * @param set
   */
  public void setEmpty(final long id_hash, final PartialTimeSeriesSet set);
  
  /**
   * Adds a column to the series.
   * @param prefix The schema prefix so we know what kind of data we're dealing
   * with.
   * @param base_timestamp The base timestamp.
   * @param qualifier The non-null qualifier.
   * @param value The non-null value.
   * @param long_array_pool The array pool to claim an array from.
   * @param id_hash The hash for the time series Id.
   * @param set The set this partial belongs to.
   */
  public void addColumn(final byte prefix, 
                        final TimeStamp base_timestamp,
                        final byte[] qualifier, 
                        final byte[] value,
                        final ObjectPool long_array_pool, 
                        final long id_hash, 
                        final PartialTimeSeriesSet set);
}
