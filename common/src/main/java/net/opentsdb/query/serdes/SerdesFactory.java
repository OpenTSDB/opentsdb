// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;

/**
 * An interface for serialization/deserialization implementation 
 * factories. It's used to return a new instance of the implementation
 * depending on the context (in or out).
 * 
 * @since 3.0
 */
public interface SerdesFactory {

  /** @return The non-null, non-empty and unique ID of this factory. */
  public String id();
  
  /**
   * Returns a serialization instance used to write to the given 
   * output stream.
   * @param context The non-null query context.
   * @param options The non-null serdes options.
   * @param stream The non-null and open stream to write to.
   * @return The non-null serdes instance.
   */
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options,
                                      final OutputStream stream);
  
  /**
   * Returns a deserialization instance used to read to the given 
   * input stream.
   * @param context The non-null query context.
   * @param options The non-null serdes options.
   * @param stream The non-null and open stream to read from.
   * @return The non-null serdes instance.
   */
  public TimeSeriesSerdes newInstance(final QueryContext context,
                                      final SerdesOptions options,
                                      final InputStream stream);
  
  /**
   * Parse the given JSON or YAML into the proper serdes config.
   * @param mapper A non-null mapper to use for parsing.
   * @param tsdb The non-null TSD to pull factories from.
   * @param node The non-null node to parse.
   * @return An instantiated node config if successful.
   */
  public SerdesOptions parseConfig(final ObjectMapper mapper, 
                                   final TSDB tsdb,
                                   final JsonNode node);
}
