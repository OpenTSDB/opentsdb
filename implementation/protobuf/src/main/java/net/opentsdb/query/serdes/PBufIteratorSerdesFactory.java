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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.PBufNumericSerdesFactory;
import net.opentsdb.data.PBufNumericSummarySerdesFactory;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * A factory used to return de/serializers for various data types.
 * 
 * @since 3.0
 */
public class PBufIteratorSerdesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      PBufIteratorSerdesFactory.class);
  
  /** The map of types. */
  private final Map<TypeToken<?>, PBufIteratorSerdes> types;
  
  /**
   * Default ctor.
   */
  public PBufIteratorSerdesFactory() {
    types = Maps.newConcurrentMap();
    types.put(NumericType.TYPE, new PBufNumericSerdesFactory());
    types.put(NumericSummaryType.TYPE, new PBufNumericSummarySerdesFactory());
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
}
