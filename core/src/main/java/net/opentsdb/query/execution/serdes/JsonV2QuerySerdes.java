// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;

/**
 * Simple serializer that outputs the time series in the same format as
 * OpenTSDB 2.x's /api/query endpoint.
 * <b>NOTE:</b> The serializer will write the individual query results to 
 * a JSON array but will not start or close the array (so additional data
 * can be added like the summary, query, etc).
 * 
 * @since 3.0
 */
public class JsonV2QuerySerdes implements TimeSeriesSerdes {

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
  public void serialize(final QueryContext context, 
                        final SerdesOptions options,
                        final OutputStream stream, 
                        final QueryResult result) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (result == null) {
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
    final net.opentsdb.query.pojo.TimeSeriesQuery query = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) context.query();
    
    try {
      for (final TimeSeries series : result.timeSeries()) {
        final Optional<Iterator<TimeSeriesValue<?>>> optional = 
            series.iterator(NumericType.TYPE);
        if (!optional.isPresent()) {
          continue;
        }
        final Iterator<TimeSeriesValue<?>> iterator = optional.get();
        if (!iterator.hasNext()) {
          continue;
        }
        
        TimeSeriesValue<NumericType> value = 
            (TimeSeriesValue<NumericType>) iterator.next();
        while (value != null && value.timestamp().compare(RelationalOperator.LT, 
            query.getTime().startTime())) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            value = null;
          }
        }
        if (value == null) {
          continue;
        }
        if (value.timestamp().compare(RelationalOperator.LT, query.getTime().startTime()) ||
            value.timestamp().compare(RelationalOperator.GT, query.getTime().endTime())) {
          continue;
        }
        
        json.writeStartObject();
        
        json.writeStringField("metric", series.id().metric());
        json.writeObjectFieldStart("tags");
        for (final Entry<String, String> entry : series.id().tags().entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeArrayFieldStart("aggregateTags");
        for (final String tag : series.id().aggregatedTags()) {
          json.writeString(tag);
        }
        json.writeEndArray();
        json.writeEndObject();
        json.writeObjectFieldStart("dps");
        
        long ts = 0;
        while(value != null) {
          if (value.timestamp().compare(RelationalOperator.GT, query.getTime().endTime())) {
            break;
          }
          ts = (opts != null && opts.msResolution()) 
              ? value.timestamp().msEpoch() 
              : value.timestamp().msEpoch() / 1000;
          
          if (value.value().isInteger()) {
            json.writeNumberField(Long.toString(ts), 
                value.value().longValue());
          } else {
            json.writeNumberField(Long.toString(ts), 
                value.value().doubleValue());
          }
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            value = null;
          }
        }
        
        json.writeEndObject();
        json.writeEndObject();
        json.flush();
      }
      
      json.flush();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception serializing: " + result);
    }
  }

  @Override
  public QueryResult deserialize(final SerdesOptions options,
                                    final InputStream stream) {
    throw new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName());
  }

}
