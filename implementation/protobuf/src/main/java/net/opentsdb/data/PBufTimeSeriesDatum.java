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

import java.time.ZoneId;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.pbuf.TimeSeriesDatumPB;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.serdes.PBufDataSerdesFactory;

/**
 * A decoder for the TimeSeriesDatum from PBuf. Note that since consumers
 * may call these fields multiple times, we just initialize them once.
 * 
 * TODO - see if we can lazily parse for better performance.
 * 
 * @since 3.0
 */
public class PBufTimeSeriesDatum implements TimeSeriesDatum {

  /** The source. */
  private final TimeSeriesDatumPB.TimeSeriesDatum datum;
  
  /** A parsed timestamp. */
  private final TimeStamp timestamp;
  
  /** The type of data. */
  private final TypeToken<?> type;
  
  /** The actual value parsed out. */
  private final TimeSeriesDataType data;
  
  /** A wrapper to satisfy the interface colission. */
  private final Wrapper wrapper;
  
  /**
   * Default ctor.
   * @param factory The non-null factory.
   * @param datum The non-null datum to parse.
   */
  public PBufTimeSeriesDatum(final PBufDataSerdesFactory factory, 
                             final TimeSeriesDatumPB.TimeSeriesDatum datum) {
    this.datum = datum;
    // TODO - see if we could use an MS timestamp
    if (datum.getTimestamp().getNanos() > 0 || 
        datum.getTimestamp().getZoneId() != null) {
      timestamp = new ZonedNanoTimeStamp(
          datum.getTimestamp().getEpoch(), 
          datum.getTimestamp().getNanos(), 
          ZoneId.of(datum.getTimestamp().getZoneId()));
    } else {
      timestamp = new SecondTimeStamp(datum.getTimestamp().getEpoch());
    }
    try {
      final Class<?> clazz = Class.forName(datum.getType());
      type = TypeToken.of(clazz);
    } catch (ClassNotFoundException e) {
      throw new SerdesException("Failed to find a class for type: " 
          + datum.getType(), e);
    }
    
    data = factory.serdesForType(type).deserialize(datum.getData());
    wrapper = new Wrapper();
  }

  @Override
  public TimeSeriesDatumId id() {
    return new PBufTimeSeriesDatumId(datum.getId());
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> value() {
    return wrapper;
  }
  
  /** Wrapper to satisfy the interface colission. */
  private class Wrapper implements TimeSeriesValue {
    
    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public TimeSeriesDataType value() {
      return data;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }
    
  }
}
