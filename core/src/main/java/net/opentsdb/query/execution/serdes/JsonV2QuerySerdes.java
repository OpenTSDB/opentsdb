// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.execution.serdes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * Simple serializer that outputs the time series in the same format as
 * OpenTSDB 2.x's /api/query endpoint.
 * <b>NOTE:</b> The serializer will write the individual query results to 
 * a JSON array but will not start or close the array (so additional data
 * can be added like the summary, query, etc).
 * 
 * @since 3.0
 */
public class JsonV2QuerySerdes implements TimeSeriesSerdes<IteratorGroups> {

  /** The JSON generator used for writing. */
  private final JsonGenerator json;
  
  /**
   * Default ctor.
   * @param generator A non-null JSON generator.
   */
  public JsonV2QuerySerdes(final JsonGenerator generator) {
    if (generator == null) {
      throw new IllegalArgumentException("Generator can not be null.");
    }
    this.json = generator;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void serialize(final TimeSeriesQuery query, 
                        final SerdesOptions options,
                        final OutputStream stream, 
                        final IteratorGroups data) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV2QuerySerdesOptions opts;
    if (options != null) {
      if (!(options instanceof JsonV2QuerySerdesOptions)) {
        throw new IllegalArgumentException("Options were of the wrong type: " 
            + options.getClass());
      }
      opts = (JsonV2QuerySerdesOptions) options;
    } else {
      opts = null;
    }
    try {
      for (final TimeSeriesIterator<?> it : data.flattenedIterators()) {
        json.writeStartObject();
        
        json.writeStringField("metric", it.id().metric());
        json.writeObjectFieldStart("tags");
        for (final Entry<String, String> entry : it.id().tags().entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeArrayFieldStart("aggregateTags");
        for (final String tag : it.id().aggregatedTags()) {
          json.writeString(tag);
        }
        json.writeEndArray();
        json.writeEndObject();
        json.writeObjectFieldStart("dps");
        
        long ts = 0;
        while (it.status() == IteratorStatus.HAS_DATA) {
          final TimeSeriesValue<NumericType> v = 
              (TimeSeriesValue<NumericType>) it.next();
          if (v.timestamp().compare(TimeStampComparator.LT, query.getTime().startTime()) || 
              v.timestamp().compare(TimeStampComparator.GT, query.getTime().endTime())) {
            continue;
          }
          
          ts = (opts != null && opts.msResolution()) 
              ? v.timestamp().msEpoch() 
              : v.timestamp().msEpoch() / 1000;
          
          if (v.value().isInteger()) {
            json.writeNumberField(Long.toString(ts), 
                v.value().longValue());
          } else {
            json.writeNumberField(Long.toString(ts), 
                v.value().doubleValue());
          }
        }
        
        json.writeEndObject();
        json.writeEndObject();
        json.flush();
      }
      
      json.flush();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception serializing: " + data);
    }
  }

  @Override
  public IteratorGroups deserialize(final SerdesOptions options,
                                    final InputStream stream) {
    throw new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName());
  }

}
