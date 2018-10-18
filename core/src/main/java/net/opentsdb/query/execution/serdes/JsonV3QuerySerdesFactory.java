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
package net.opentsdb.query.execution.serdes;

import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;

/**
 * A factory for returning JSON serializers for the OpenTSDB 3x format.
 * 
 * @since 3.0
 */
public class JsonV3QuerySerdesFactory extends BaseTSDBPlugin implements SerdesFactory {

  public static final String TYPE = "JsonV3QuerySerdes";
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options,
                                      final OutputStream stream) {
    return new JsonV3QuerySerdes(context, options, stream);
  }
  
  @Override
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options, 
                                      final InputStream stream) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public SerdesOptions parseConfig(final ObjectMapper mapper, 
                                   final TSDB tsdb,
                                   final JsonNode node) {
    try {
      return (SerdesOptions) mapper.treeToValue(node, 
          JsonV2QuerySerdesOptions.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse config.", e);
    }
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }
  
}
