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
package net.opentsdb.query.execution.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecutorConfig;

/**
 * A simple implementation of a cluster config that loads a static config file
 * and returns a list of host URIs for executors to call. The hosts could be
 * actual hosts, load balancers, etc.
 *
 * @since 3.0
 */
public class StaticClusterConfig extends ClusterConfigPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      StaticClusterConfig.class);
  
  /** A map of cluster IDs to cluster descriptors, the defaults. */
  private Map<String, ClusterDescriptor> clusters;
  
  /** A map of overrides used when {@link #setupQuery(QueryContext, String)} 
   * is called. */
  private Map<String, ClusterOverride> overrides;
  
  @Override
  public void setConfig(final ClusterConfigPlugin.Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (this.config != null) {
      throw new IllegalStateException("Config was already set. "
          + "Cannot change it.");
    }
    this.config = (Config) config;
    clusters = Maps.newHashMapWithExpectedSize(this.config.clusters.size());
    for (final ClusterDescriptor cluster : this.config.clusters) {
      clusters.put(cluster.getCluster(), cluster);
    }
    
    if (this.config.overrides != null) {
      overrides = Maps.newHashMapWithExpectedSize(this.config.overrides.size());
      for (final ClusterOverride override : this.config.overrides) {
        overrides.put(override.getId(), override);
      }
    }
  }
  
  @Override
  public Map<String, ClusterDescriptor> clusters() {
    return Collections.unmodifiableMap(clusters);
  }

  @Override
  public List<String> setupQuery(final QueryContext context, 
                                 final String override) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    final Collection<ClusterDescriptor> descriptors;
    if (!Strings.isNullOrEmpty(override)) {
      final ClusterOverride found = overrides.get(override);
      if (found == null) {
        throw new IllegalArgumentException("No such override [" + override 
            + "] configured: " + this);
      }
      
      descriptors = Lists.newArrayListWithExpectedSize(found.getClusters().size());
      for (final ClusterDescriptor descriptor : found.getClusters()) {
        // in case the shorthand override has an empty executor config set, 
        // use those from the parent.
        if (descriptor.getExecutorConfigs().isEmpty()) {
          descriptors.add(clusters.get(descriptor.getCluster()));
        } else {
          descriptors.add(descriptor);
        }
      }
    } else {
      descriptors = clusters.values();
    }
    
    final List<String> ids = 
        Lists.newArrayListWithExpectedSize(descriptors.size());
    for (final ClusterDescriptor descriptor : descriptors) {
      ids.add(descriptor.getCluster());
      for (final QueryExecutorConfig config : descriptor.getExecutorConfigs()) {
        final QueryExecutorConfig user_override = 
            context.getConfigOverride(config.getExecutorId());
        if (user_override != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Override already present in context: " + user_override);
          }
        } else {
          context.addConfigOverride(config);
        }
      }
    }
    
    return ids;
  }

  @Override
  public String id() {
    return "StaticHostClusterConfig";
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  @VisibleForTesting
  Map<String, ClusterOverride> overrides() {
    return overrides;
  }
  
  /** The config for this plugin. */
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends ClusterConfigPlugin.Config {

    /**
     * Default ctor.
     * @param builder A non-null builder.
     */
    protected Config(final Builder builder) {
      super(builder);
    }

    /** @return A builder to construct the config from. */
    public static Builder newBuilder() {
      return new Builder();
    }
    
    /**
     * @param config A non-null config.
     * @return A builder populated with the config data.
     */
    public static Builder newBuilder(final Config config) {
      final Builder builder = (Builder) new Builder()
          .setId(config.id)
          .setImplementation(config.implementation);
      if (config.clusters != null) {
        for (final ClusterDescriptor cluster : config.clusters) {
          builder.addCluster(ClusterDescriptor.newBuilder(cluster));
        }
      }
      if (config.overrides != null) {
        for (final ClusterOverride override : config.overrides) {
          builder.addOverride(ClusterOverride.newBuilder(override));
        }
      }
      return builder;
    }
    
    /** The builder extension. */
    public static class Builder extends ClusterConfigPlugin.Config.Builder {
      @Override
      public Config build() {
        return new Config(this);
      }
    }
  }
}
