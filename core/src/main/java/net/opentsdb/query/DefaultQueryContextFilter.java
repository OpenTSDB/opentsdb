// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;

/**
 * Stub class for a super simple context filter that filters on the user and
 * headers for now.
 * 
 * TODO - Can cache the fetches from the config class for effeciency.
 *  
 * @since 3.0
 */
public class DefaultQueryContextFilter extends BaseTSDBPlugin 
    implements QueryContextFilter {
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultQueryContextFilter.class);
  
  private static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAPS =
      new TypeReference<Map<String, Map<String, String>>>() { };
  private static final TypeReference<
    Map<String, Map<String, Map<String, String>>>> MAP_OF_MAP_OF_MAPS =
      new TypeReference<Map<String, Map<String, Map<String, String>>>>() { };
  private static final String HEADER_KEY = "tsd.queryfilter.filter.headers";
  private static final String USER_KEY = "tsd.queryfilter.filter.users";
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    if (!tsdb.getConfig().hasProperty(HEADER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(HEADER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MAP_OF_MAP_OF_MAPS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(USER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(USER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MAP_OF_MAPS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSeriesQuery filter(final TimeSeriesQuery query, 
      final AuthState auth_state, 
      final Map<String, String> headers) {
    SemanticQuery.Builder builder = null;
    Map<String, Map<String, Map<String, String>>> hdr_filter = 
        tsdb.getConfig().getTyped(HEADER_KEY, MAP_OF_MAP_OF_MAPS);
    if (!hdr_filter.isEmpty() && headers != null) {
      for (final Entry<String, String> entry : headers.entrySet()) {
        Map<String, Map<String, String>> header_filter = 
            hdr_filter.get(entry.getKey());
        if (header_filter != null) {
          Map<String, String> values = header_filter.get(entry.getValue());
          if (values != null) {
            // OVERRIDE the query!
            if (builder == null) {
              builder = ((SemanticQuery) query).toBuilder();
            }
            
            String ov = values.get("cacheMode");
            if (ov != null && query.getCacheMode() == null) {
              builder.setCacheMode(CacheMode.valueOf(ov));
              LOG.trace("Overriding cache mode for header: " + 
                  entry.getKey() + ":" + entry.getValue() + " to " + 
                  CacheMode.valueOf(ov));
            }
          }
        }
      }
    }
    
    Map<String, Map<String, String>> user_filters = 
        tsdb.getConfig().getTyped(USER_KEY, MAP_OF_MAPS);
    if (user_filters != null) {
      final String user = auth_state != null && 
          auth_state.getPrincipal() != null ? 
              auth_state.getPrincipal().getName() : "Unknown";
      Map<String, String> filter = user_filters.get(user);
      if (filter != null) {
        // OVERRIDE the query!
        if (builder == null) {
          builder = ((SemanticQuery) query).toBuilder();
        }
        String ov = filter.get("cacheMode");
        if (ov != null && query.getCacheMode() == null) {
          builder.setCacheMode(CacheMode.valueOf(ov));
          if (LOG.isTraceEnabled()) {
            LOG.trace("Overriding cache mode for user: " + user + " to " 
                + CacheMode.valueOf(ov));
          }
        }
      }
    }
    
    return builder != null ? builder.build() : query;
  }

  @Override
  public String type() {
    return "DefaultQueryContextFilter";
  }
  
}