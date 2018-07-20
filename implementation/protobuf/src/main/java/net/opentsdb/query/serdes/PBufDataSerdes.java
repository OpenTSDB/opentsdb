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
package net.opentsdb.query.serdes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map.Entry;

import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.PBufTimeSeriesDatum;
import net.opentsdb.data.PBufTimeSeriesDatumId;
import net.opentsdb.data.PBufTimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.pbuf.TimeSeriesDatumPB;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB;
import net.opentsdb.data.pbuf.TimeStampPB;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.DataList;
import net.opentsdb.data.pbuf.TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.DataType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.stats.Span;

/**
 * A serializer for writable data points as opposed to query values.
 * 
 * @since 3.0
 */
public class PBufDataSerdes implements TimeSeriesDataSerdes, TSDBPlugin  {

  /** The non-null factory for datum. */
  protected final PBufDataSerdesFactory factory;
  
  public PBufDataSerdes() {
    factory = new PBufDataSerdesFactory();
  }
  
  @Override
  public void serialize(final SerdesOptions options, 
                        final TimeSeriesDatum datum,
                        final OutputStream stream, 
                        final Span span) {
    if (datum.value() == null) {
      throw new IllegalArgumentException("Can't serialize a null time series value.");
    }
    if (datum.value().value() == null) {
      throw new IllegalArgumentException("Can't serialize a null data value.");
    }
    
    final PBufDatumSerdes serdes = factory.serdesForType(datum.value().type());
    if (serdes == null) {
      throw new SerdesException("Unable to find a serialiation module "
          + "for type: " + datum.value().type());
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".serialize")
                  .start();
    } else {
      child = null;
    }
    
    // TODO Auto-generated method stub
    TimeSeriesDatumPB.TimeSeriesDatum.Builder builder = 
        TimeSeriesDatumPB.TimeSeriesDatum.newBuilder()
        .setId(PBufTimeSeriesDatumId.newBuilder(datum.id())
            .build()
            .pbufID());
    
    TimeStampPB.TimeStamp.Builder ts_builder = TimeStampPB.TimeStamp.newBuilder()
        .setEpoch(datum.value().timestamp().epoch())
        .setNanos(datum.value().timestamp().nanos());
    if (datum.value().timestamp().timezone() != null &&
        datum.value().timestamp().timezone() != Const.UTC) {
      ts_builder.setZoneId(datum.value().timestamp().timezone().toString());
    }
    builder.setTimestamp(ts_builder)
           .setData(serdes.serialize(datum.value().value()))
           .setType(datum.value().type().toString());
    
    try {
      builder.build().writeTo(stream);
      if (child != null) {
        child.setSuccessTags().finish();
      }
    } catch (IOException e) {
      final SerdesException ex = new SerdesException(
          "Unexpected exception serializing data", e);
      if (child != null) {
        child.setErrorTags(ex).finish();
      }
      throw ex;
    }
  }

  @Override
  public void serialize(final SerdesOptions options,
                        final TimeSeriesSharedTagsAndTimeData data, 
                        final OutputStream stream, 
                        final Span span) {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data cannot be null.");
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".deserialize")
                  .start();
    } else {
      child = null;
    }
    
    TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.Builder builder =
      TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.newBuilder()
      .putAllTags(data.tags());
    
    TimeStampPB.TimeStamp.Builder ts_builder = TimeStampPB.TimeStamp.newBuilder()
        .setEpoch(data.timestamp().epoch())
        .setNanos(data.timestamp().nanos());
    if (data.timestamp().timezone() != null &&
        data.timestamp().timezone() != Const.UTC) {
      ts_builder.setZoneId(data.timestamp().timezone().toString());
    }
    builder.setTimestamp(ts_builder);
    
    for (final Entry<String, Collection<TimeSeriesDataType>> entry : 
        data.data().asMap().entrySet()) {
      DataList.Builder list_builder = DataList.newBuilder();
      
      for (final TimeSeriesDataType value : entry.getValue()) {
        list_builder.addList(DataType.newBuilder()
            .setType(value.type().toString())
            .setData(factory.serdesForType(value.type()).serialize(value)));
      }
      
      builder.putData(entry.getKey(), list_builder.build());
    }
    
    try {
      builder.build().writeTo(stream);
      if (child != null) {
        child.setSuccessTags().finish();
      }
    } catch (IOException e) {
      final SerdesException ex = new SerdesException(
          "Unexpected exception serializing data", e);
      if (child != null) {
        child.setErrorTags(ex).finish();
      }
      throw ex;
    }
  }

  @Override
  public TimeSeriesDatum deserializeDatum(final SerdesOptions options,
                                          final InputStream stream, 
                                          final Span span) {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".deserialize")
                  .start();
    } else {
      child = null;
    }
    
    try {
      final TimeSeriesDatumPB.TimeSeriesDatum parsed = 
          TimeSeriesDatumPB.TimeSeriesDatum.parseFrom(stream);
      if (child != null) {
        child.setSuccessTags().finish();
      }
      return new PBufTimeSeriesDatum(factory, parsed);
    } catch (IOException e) {
      final SerdesException ex =
        new SerdesException("Unexpected execution deserializing data", e);
      if (child != null) {
        child.setErrorTags(ex).finish();
      }
      throw ex;
    }
  }

  @Override
  public TimeSeriesSharedTagsAndTimeData deserializeShared(
      final SerdesOptions options, 
      final InputStream stream, 
      final Span span) {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".deserialize")
                  .start();
    } else {
      child = null;
    }
    
    try {
      final TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData parsed = 
          TimeSeriesSharedTagsAndTimeDataPB.TimeSeriesSharedTagsAndTimeData.parseFrom(stream);
      if (child != null) {
        child.setSuccessTags().finish();
      }
      return new PBufTimeSeriesSharedTagsAndTimeData(factory, parsed);
    } catch (IOException e) {
      final SerdesException ex =
        new SerdesException("Unexpected execution deserializing data", e);
      if (child != null) {
        child.setErrorTags(ex).finish();
      }
      throw ex;
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
}
