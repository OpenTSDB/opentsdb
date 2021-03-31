// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.MetaQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * A chain of query context filters processed in the order of definition.
 *
 * @since 3.0
 */
public class ChainedQueryContextFilter extends BaseTSDBPlugin
        implements QueryContextFilter {
  private static final Logger LOG = LoggerFactory.getLogger(
          ChainedQueryContextFilter.class);

  public static final TypeReference<List<String>> TYPE_REF =
          new TypeReference<List<String>>() { };
  public static final String TYPE = "ChainedQueryContextFilter";
  public static final String KEY_PREFIX = "tsd.query.context.filter.";
  public static final String CHAIN_KEY = "chain";

  private List<QueryContextFilter> chain;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    if (!tsdb.getConfig().hasProperty(getConfigKey(CHAIN_KEY))) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
      .setKey(getConfigKey(CHAIN_KEY))
      .setType(TYPE_REF)
      .setDescription("A list of strings referring to the ID of " +
              "QueryContextFilter plugins. The plugins will be processed " +
              "in order of first to last.")
      .setSource(getClass().getName()));
    }

    List<String> filter_ids = tsdb.getConfig().getTyped(getConfigKey(CHAIN_KEY), TYPE_REF);
    if (filter_ids == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "Chain filter list cannot be empty."));
    }

    // prevent a self referential loop and check for nulls.
    chain = Lists.newArrayList();
    for (int i = 0; i < filter_ids.size(); i++) {
      final String filter_id = filter_ids.get(i);
      if (Strings.isNullOrEmpty(filter_id)) {
        return Deferred.fromError(new IllegalArgumentException(
                "Filter ID at index " + i + " cannot be null or empty."));
      }
      // yes, someone could nest the chains.
      if (!Strings.isNullOrEmpty(id) && id.equals(filter_id)) {
        return Deferred.fromError(new IllegalArgumentException(
                "A chain cannot reference itself => " + id + " at " + i));
      }

      final QueryContextFilter filter = tsdb.getRegistry().getPlugin(
              QueryContextFilter.class, filter_id);
      if (filter == null) {
        return Deferred.fromError(new IllegalArgumentException(
                "No filter found for " + id + " at " + i));
      }
      LOG.info("Loaded query context filter " + filter_id + " into chain "
              + id + " at " + i);
      chain.add(filter);
    }
    LOG.info("Successfully loaded " + chain.size() + " query context filters.");
    return Deferred.fromResult(null);
  }

  @Override
  public TimeSeriesQuery filter(TimeSeriesQuery query,
                                final AuthState auth_state,
                                final Map<String, String> headers) {
    for (int i = 0; i < chain.size(); i++) {
      query = chain.get(i).filter(query, auth_state, headers);
    }
    return query;
  }

  @Override
  public MetaQuery filter(MetaQuery query,
                          final AuthState auth_state,
                          final Map<String, String> headers) {
    for (int i = 0; i < chain.size(); i++) {
      query = chain.get(i).filter(query, auth_state, headers);
    }
    return query;
  }

  @Override
  public String type() {
    return TYPE;
  }

  /**
   * Helper to get the proper key.
   * @param suffix The non-null suffix to append.
   * @return The full config key.
   */
  private String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ?
            "" : id + ".")
            + suffix;
  }
}
