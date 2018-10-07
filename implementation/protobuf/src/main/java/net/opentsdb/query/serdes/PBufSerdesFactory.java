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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.PBufNumericTimeSeriesSerdes;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PBufNumericSummaryTimeSeriesSerdes;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;

/**
 * A factory used to return de/serializers for various data types.
 * 
 * @since 3.0
 */
public class PBufSerdesFactory extends BaseTSDBPlugin implements SerdesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      PBufSerdesFactory.class);
  
  public static final String ID = "PBufSerdes";
  
  /** The map of types. */
  private final Map<TypeToken<?>, PBufIteratorSerdes> types;
  
  /**
   * Default ctor.
   */
  public PBufSerdesFactory() {
    types = Maps.newConcurrentMap();
    types.put(NumericType.TYPE, new PBufNumericTimeSeriesSerdes());
    types.put(NumericSummaryType.TYPE, new PBufNumericSummaryTimeSeriesSerdes());
  }
  
  @Override
  public String id() {
    return ID;
  }
  
  /**
   * Registers the given serdes module with the factory, replacing any 
   * existing modules for the given type.
   * @param serdes A non-null serdes module.
   * @throws IllegalArgumentException if the serdes was null or it's type
   * was null.
   */
  public void register(final PBufIteratorSerdes serdes) {
    if (serdes == null) {
      throw new IllegalArgumentException("Serdes cannot be null.");
    }
    if (serdes.type() == null) {
      throw new IllegalArgumentException("Serdes type cannot be null.");
    }
    
    final PBufIteratorSerdes existing = types.put(serdes.type(), serdes);
    if (existing != null) {
      LOG.warn("Replacing existing serdes module [" + existing 
          + "] for type [" + serdes.type() + "] with module [" 
          + serdes + "]");
    } else {
      LOG.info("Successfully registered serdes module [" + serdes 
          + "] for type [" + serdes.type() + "]");
    }
  }
  
  /**
   * Returns the serdes module for the given type, if registered.
   * @param type A non-null type of time series data.
   * @return Null if the type is not handled or a serdes module if found.
   */
  public PBufIteratorSerdes serdesForType(final TypeToken<?> type) {
    return types.get(type);
  }

  @Override
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options, 
                                      final OutputStream stream) {
    return new PBufSerdes(this, context, options, stream);
  }

  @Override
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options, 
                                      final InputStream stream) {
    return new PBufSerdes(this, context, options, stream);
  }

  @Override
  public SerdesOptions parseConfig(ObjectMapper mapper, TSDB tsdb,
      JsonNode node) {
    // TODO Auto-generated method stub
    try {
      return mapper.treeToValue(node, JsonV2QuerySerdesOptions.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse JSON", e);
    }
  }

  @Override
  public String version() {
    return "3.0.0";
  }
}
