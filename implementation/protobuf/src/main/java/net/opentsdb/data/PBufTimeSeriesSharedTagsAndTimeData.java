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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.DataList;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.DataType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.serdes.PBufDataSerdesFactory;

/**
 * A wrapper around a PBuf shared time datum.
 * 
 * Note that we pull out the data immediatly as it may be fetched multiple
 * times. 
 * 
 * TODO - see if we can lazily parse.
 * 
 * @since 3.0
 */
public class PBufTimeSeriesSharedTagsAndTimeData implements 
    TimeSeriesSharedTagsAndTimeData {

  /** The data source. */
  private final TimeSeriesSharedTagsAndTimeDataPB
    .TimeSeriesSharedTagsAndTimeData source;
  
  /** The parsed timestamp. */
  private final TimeStamp timestamp;
  
  /** The parsed data. */
  private Multimap<String, TimeSeriesDataType> data;
  
  /**
   * Default ctor.
   * @param factory The non-null factory to use for serdes of values.
   * @param source The non-null source to parse.
   */
  public PBufTimeSeriesSharedTagsAndTimeData(
      final PBufDataSerdesFactory factory, 
      final TimeSeriesSharedTagsAndTimeDataPB
        .TimeSeriesSharedTagsAndTimeData source) {
    this.source = source;
    
    // TODO - see if we could use an MS timestamp
    if (source.getTimestamp().getNanos() > 0 || 
        source.getTimestamp().getZoneId() != null) {
      timestamp = new ZonedNanoTimeStamp(
          source.getTimestamp().getEpoch(), 
          source.getTimestamp().getNanos(), 
          ZoneId.of(source.getTimestamp().getZoneId()));
    } else {
      timestamp = new SecondTimeStamp(source.getTimestamp().getEpoch());
    }
    
    data = ArrayListMultimap.create(source.getDataCount(), 1);
    for (final Entry<String, DataList> entry : source.getDataMap().entrySet()) {
      for (final DataType value : entry.getValue().getListList()) {
        try {
          final Class<?> clazz = Class.forName(value.getType());
          final TypeToken<?> type = TypeToken.of(clazz);
          final TimeSeriesDataType deserialized = 
              factory.serdesForType(type).deserialize(value.getData());
          data.put(entry.getKey(), deserialized);
        } catch (ClassNotFoundException e) {
          throw new SerdesException("Failed to find a class for type: " 
              + value.getType(), e);
        }
      }
    }
  }
  
  @Override
  public Iterator<TimeSeriesDatum> iterator() {
    return new DataIterator();
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public Map<String, String> tags() {
    return source.getTags();
  }

  @Override
  public Multimap<String, TimeSeriesDataType> data() {
    return data;
  }

  class DataIterator implements Iterator<TimeSeriesDatum>, 
    TimeSeriesDatum, TimeSeriesDatumStringId {
    
    private boolean has_next;
    private Iterator<Entry<String, TimeSeriesDataType>> iterator;
    private Entry<String, TimeSeriesDataType> next_entry;
    private String current_metric;
    private TimeSeriesDataType current_value;
    
    DataIterator() {
      iterator = data.entries().iterator();
      if (iterator.hasNext()) {
        next_entry = iterator.next();
        has_next = true;
      }
    }
    
    @Override
    public boolean hasNext() {
      return has_next;
    }
  
    @Override
    public TimeSeriesDatum next() {
      current_metric = next_entry.getKey();
      current_value = next_entry.getValue();
      has_next = false;
      if (iterator.hasNext()) {
        next_entry = iterator.next();
        has_next = true;
      }
      return this;
    }
  
    @Override
    public TimeSeriesDatumId id() {
      return this;
    }
  
    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> value() {
      return new WrappedValue();
    }
    
    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      return Const.TS_STRING_ID;
    }
  
    @Override
    public long buildHashCode() {
      // TODO Auto-generated method stub
      return 0;
    }
  
    @Override
    public int compareTo(TimeSeriesDatumId o) {
      // TODO Auto-generated method stub
      return 0;
    }
  
    @Override
    public String namespace() {
      return source.getNamespace();
    }
  
    @Override
    public String metric() {
      return current_metric;
    }
  
    @Override
    public Map<String, String> tags() {
      return source.getTags();
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("metric=")
          .append(current_metric)
          .append(", tags=")
          .append(source.getTagsCount())
          .append(", namespace=")
          .append(source.getNamespace())
          .append(", value=")
          .append("")
          .append(", timestamp=")
          .append(timestamp)
          .toString();
    }

    class WrappedValue implements TimeSeriesValue {

      @Override
      public TimeStamp timestamp() {
        return timestamp;
      }

      @Override
      public TimeSeriesDataType value() {
        return current_value;
      }

      @Override
      public TypeToken type() {
        return current_value.type();
      }
      
    }
  }
  
}
