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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.pbuf.TimeStampPB;
import net.opentsdb.data.pbuf.NumericSummarySegmentPB.NumericSummarySegment;
import net.opentsdb.data.pbuf.NumericSummarySegmentPB.NumericSummarySegment.NumericSummary;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;
import net.opentsdb.data.pbuf.TimeSeriesDataSequencePB.TimeSeriesDataSegment;
import net.opentsdb.data.pbuf.TimeSeriesPB.TimeSeries.Builder;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.PBufIteratorSerdes;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.utils.Bytes;

/**
 * A serdes implementation for {@link NumericSummaryType} data. This 
 * class will encode numeric values preserving longs and doubles as well 
 * as {@link Double#NaN} and null values.
 * <p>
 * Note that the base time of the segment is the start time of the 
 * {@link SerdesOptions} without any normalization.
 * <p>
 * If the resolution in the query results is lower than the resolution of
 * the timestamps and one or more timestamps would be encoded on the same
 * offset a {@link SerdesException} is thrown during serialization.
 * 
 * @since 3.0
 */
public class PBufNumericSummaryTimeSeriesSerdes implements PBufIteratorSerdes {

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public void serialize(final Builder ts_builder, 
                        final QueryContext context,
                        final SerdesOptions options,
                        final QueryResult result,
                        final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
    ts_builder.addData(serialize(context, options, result, iterator));
  }

  @Override
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> deserialize(
      final TimeSeriesData series) {
    return new PBufNumericSummaryIterator(series);
  }

  /**
   * Encodes the given iterator.
   * @param context A non-null query context.
   * @param options Options, ignored.
   * @param result A non-null result.
   * @param iterator A non-null iterator.
   * @return A data protobuf object.
   */
  TimeSeriesData serialize(final QueryContext context,
                           final SerdesOptions options,
                           final QueryResult result,
                           final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
    final long span;
    switch(result.resolution()) {
    case NANOS:
    case MICROS:
      long seconds = options.end().epoch() - options.start().epoch();
      long ns = options.end().nanos() - options.start().nanos();
      span = (seconds * 1000L * 1000L * 1000L) + ns;
      break;
    case MILLIS:
      span = options.end().epoch() - options.start().epoch();
      break;
    default:
      span = options.end().epoch() - options.start().epoch();
    }
    byte encode_on = NumericCodec.encodeOn(span, NumericCodec.LENGTH_MASK);
    
    final Map<Integer, ByteArrayOutputStream> summary_streams = 
        Maps.newHashMap();
    try {
      long previous_offset = -1;
      while (iterator.hasNext()) {
        @SuppressWarnings("unchecked")
        final TimeSeriesValue<NumericSummaryType> value = 
            (TimeSeriesValue<NumericSummaryType>) iterator.next();
        if (value.timestamp().compare(Op.LT, options.start())) {
          continue;
        }
        if (value.timestamp().compare(Op.GT, options.end())) {
          break;
        }
        
        long current_offset = offset(options.start(), 
            value.timestamp(), result.resolution());
        if (current_offset == previous_offset) {
          throw new SerdesException("With results set to a resolution of " 
              + result.resolution() + " one or more data points with "
              + "duplicate timestamps would be written at offset: " 
              + current_offset);
        }
        previous_offset = current_offset;
        
        if (value.value() == null) {
          // so, if we have already populated our summaries with nulls we
          // can fill with nulls. But at the start of the iteration we
          // don't know what to fill with.
          for (final Entry<Integer, ByteArrayOutputStream> entry : 
            summary_streams.entrySet()) {
            ByteArrayOutputStream baos = entry.getValue();
            final byte flags = NumericCodec.FLAG_FLOAT;
            baos.write(Bytes.fromLong(
                (current_offset << NumericCodec.FLAG_BITS) | flags),
                  8 - encode_on, encode_on);
          }
          continue;
        }
        
        for (final int summary : value.value().summariesAvailable()) {
          ByteArrayOutputStream baos = summary_streams.get(summary);
          if (baos == null) {
            baos = new ByteArrayOutputStream();
            summary_streams.put(summary, baos);
          }
          
          NumericType val = value.value().value(summary);
          if (val == null) {
            // length of 0 + float flag == null value, so nothing following
            final byte flags = NumericCodec.FLAG_FLOAT;
            baos.write(Bytes.fromLong(
                (current_offset << NumericCodec.FLAG_BITS) | flags),
                  8 - encode_on, encode_on);
          } else if (val.isInteger()) {
            final byte[] vle = NumericCodec.vleEncodeLong(val.longValue());
            final byte flags = (byte) (vle.length - 1);
            baos.write(Bytes.fromLong(
                (current_offset << NumericCodec.FLAG_BITS) | flags),
                8 - encode_on, encode_on);
            baos.write(vle);
          } else {
            final double v = val.doubleValue();
            final byte[] vle = NumericType.fitsInFloat(v) ? 
                Bytes.fromInt(Float.floatToIntBits((float) v)) :
                  Bytes.fromLong(Double.doubleToLongBits(v));
            final byte flags = (byte) ((vle.length - 1) | NumericCodec.FLAG_FLOAT);
            baos.write(Bytes.fromLong(
                (current_offset << NumericCodec.FLAG_BITS) | flags),
                8 - encode_on, encode_on);
            baos.write(vle);
          }
        }
      }
    } catch (IOException e) {
      throw new SerdesException("Unexppected exception serializing "
          + "iterator: " + iterator, e);
    }
  
    final NumericSummarySegment.Builder segment_builder = 
        NumericSummarySegment.newBuilder()
          .setEncodedOn(encode_on)
          .setResolution(result.resolution().ordinal());
    for (final Entry<Integer, ByteArrayOutputStream> entry : 
        summary_streams.entrySet()) {
      segment_builder.addData(NumericSummary.newBuilder()
          .setSummaryId(entry.getKey())
          // TODO - can I wrap???
          .setData(ByteString.copyFrom(entry.getValue().toByteArray())));
    }
    
    final TimeStampPB.TimeStamp.Builder start = TimeStampPB.TimeStamp.newBuilder()
        .setEpoch(options.start().epoch())
        .setNanos(options.start().nanos());
    if (options.start().timezone() != null) {
      start.setZoneId(options.start().timezone().toString());
    }
    
    final TimeStampPB.TimeStamp.Builder end = TimeStampPB.TimeStamp.newBuilder()
        .setEpoch(options.end().epoch())
        .setNanos(options.end().nanos());
    if (options.end().timezone() != null) {
      end.setZoneId(options.end().timezone().toString());
    }
    
    return TimeSeriesData.newBuilder()
      .setType(NumericSummaryType.TYPE.getRawType().getName())
      .addSegments(TimeSeriesDataSegment.newBuilder()
          .setStart(start)
          .setEnd(end)
          .setData(Any.pack(segment_builder.build())))
      .build();
  }
  
  /**
   * Calculates the offset from the base timestamp at the right resolution.
   * @param base A non-null base time.
   * @param value A non-null value.
   * @param resolution A non-null resolution.
   * @return An offset in the appropriate units.
   */
  long offset(final TimeStamp base, 
              final TimeStamp value, 
              final ChronoUnit resolution) {
    switch(resolution) {
    case NANOS:
    case MICROS:
      long seconds = value.epoch() - base.epoch();
      return (seconds * 1000L * 1000L * 1000L) + (value.nanos() - base.nanos());
    case MILLIS:
      return value.msEpoch() - base.msEpoch();
    default:
      return value.epoch() - base.epoch();
    }
  }
}
